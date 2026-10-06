#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""glm_proxy 的离线自测（纯标准库 unittest，无需联网、不消耗真实账号）。

跑法（项目根目录）::

    python -m unittest discover -s tests -v

覆盖点：
    * 签名算法 / 多轮消息拍平 / 联网与 tools 解析
    * 账号串行闸：同账号并发请求被排队（上游从未看到并发生成）
    * 多账号并行 + 失效轮换 + 游客兜底
    * 撞并发闸的退避重试、429 归类、非并发类错误不重试
    * refresh_token 轮换落盘与重载
    * 真 HTTP 端到端：/health、/v1/models 鉴权、流式与非流式补全、503 映射
"""

from __future__ import annotations

import codecs
import hashlib
import http.client
import io
import json
import os
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from contextlib import contextmanager
from http.server import ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import glm_proxy as gp  # noqa: E402

SSE_TEXT = (
    'data: {"conversation_id":"conv-1","parts":[{"logic_id":"p1","model":"moe_53f",'
    '"content":[{"type":"text","text":"你好"}]}],"status":"generating"}\n\n'
    'data: {"conversation_id":"conv-1","parts":[{"logic_id":"p1","model":"moe_53f",'
    '"content":[{"type":"text","text":"你好世界"}]}],"status":"finish"}\n\n'
)
BUSY_PAYLOAD = {"code": 400, "message": "请等待其他对话生成完毕"}
BUSY_REQUEST = {"role": "user", "content": "你好"}
WEATHER_TOOL = {
    "type": "function",
    "function": {
        "name": "get_weather",
        "description": "查询指定城市的天气",
        "parameters": {
            "type": "object",
            "properties": {"city": {"type": "string"}},
            "required": ["city"],
        },
    },
}


def sse_with_text(text: str) -> str:
    """造一段「上游模型输出了这段文字」的 SSE。"""
    return (
        'data: {"conversation_id":"conv-1","parts":[{"logic_id":"p1","content":'
        '[{"type":"text","text":%s}]}],"status":"generating"}\n\n'
        'data: {"conversation_id":"conv-1","parts":[],"status":"finish"}\n\n'
    ) % json.dumps(text, ensure_ascii=False)


def sse_text_event(text: str, status: str = "generating", logic_id: str = "p1") -> str:
    """单个 SSE 帧（正文），可按字节偏移拼接/截断。"""
    return (
        'data: {"conversation_id":"conv-1","parts":[{"logic_id":%s,"content":'
        '[{"type":"text","text":%s}]}],"status":%s}\n\n'
    ) % (json.dumps(logic_id), json.dumps(text, ensure_ascii=False), json.dumps(status))


def sse_without_finish(text: str) -> str:
    """只有 generating 帧、**等不到 finish** 的 SSE（上游把连接掐了）。"""
    return sse_text_event(text)


def sse_with_think_only(thinking: str) -> str:
    """造一段「上游只吐了思维链、没有正文」的 SSE（实测踩坑的形态 B）。"""
    return (
        'data: {"conversation_id":"conv-1","parts":[{"logic_id":"p1","content":'
        '[{"type":"think","think":%s}]}],"status":"generating"}\n\n'
        'data: {"conversation_id":"conv-1","parts":[],"status":"finish"}\n\n'
    ) % json.dumps(thinking, ensure_ascii=False)


def sse_with_native_tool_call(tool_name: str, answer: str) -> str:
    """造一段「上游调用了**平台自带工具**（search 等），随后自己把结果用于作答」的 SSE。

    item 形状与 arguments 都是 JSON 字符串 —— 与抓帧实测的 chatglm.cn 网页版一致。
    """
    call = json.dumps({
        "type": "tool_calls",
        "tool_calls": {
            "id": "call_0123456789abcdef",
            "name": tool_name,
            "arguments": json.dumps({"query": "北京 天气"}, ensure_ascii=False),
        },
    }, ensure_ascii=False)
    result = json.dumps({
        "type": "tool_result",
        "tool_result": {"id": "call_0123456789abcdef", "content": "北京 晴 21 度"},
    }, ensure_ascii=False)
    text = json.dumps({"type": "text", "text": answer}, ensure_ascii=False)
    return (
        'data: {"conversation_id":"conv-1","parts":[{"logic_id":"p1","content":[%s]}],'
        '"status":"generating"}\n\n'
        'data: {"conversation_id":"conv-1","parts":[{"logic_id":"p1","content":[%s,%s]}],'
        '"status":"generating"}\n\n'
        'data: {"conversation_id":"conv-1","parts":[{"logic_id":"p1","content":[%s]}],'
        '"status":"finish"}\n\n'
    ) % (call, call, result, text)


@contextmanager
def env_patch(env: dict):
    """临时替换 GLM_*/HOST/PORT/SERVER_API_KEYS 环境变量，退出时完全还原。"""
    keys = [k for k in os.environ if k.startswith(("GLM_", "SERVER_API_", "HOST", "PORT"))]
    backup = {k: os.environ[k] for k in keys}
    for k in keys:
        del os.environ[k]
    os.environ.update({k: str(v) for k, v in env.items()})
    try:
        yield
    finally:
        for k in [k for k in os.environ if k.startswith(("GLM_", "SERVER_API_", "HOST", "PORT"))]:
            del os.environ[k]
        os.environ.update(backup)


def make_config(**overrides) -> gp.Config:
    """构造不读外部环境的 Config，并默认调小等待时间让测试跑得快。"""
    with env_patch({}):
        config = gp.Config()
    config.delete_conversation = False
    config.busy_backoff = 0.01
    config.queue_timeout = 5.0
    config.account_cooldown = 5.0
    for key, value in overrides.items():
        setattr(config, key, value)
    return config


class Counter:
    """统计「同时在跑的上游生成」数量，用来验证串行闸是否真的生效。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.active = 0
        self.max_active = 0

    def enter(self) -> None:
        with self._lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)

    def exit(self) -> None:
        with self._lock:
            self.active -= 1


class FakeResponse:
    """最小可用的 urlopen 响应替身。``cut_after`` 非 0 时模拟上游中途掐线。"""

    def __init__(self, body: bytes, ctype: str = "text/event-stream", counter: Counter = None,
                 cut_after: int = 0):
        self._buf = io.BytesIO(body)
        self.headers = {"Content-Type": ctype}
        self.closed = False
        self._counter = counter
        self._cut_after = cut_after
        self._read_bytes = 0
        if counter is not None:
            counter.enter()

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            size = len(self._buf.getvalue()) - self._buf.tell()
        if self._cut_after:
            size = min(size, self._cut_after - self._read_bytes)
            if size <= 0:
                raise http.client.IncompleteRead(b"", 0)
        data = self._buf.read(size)
        self._read_bytes += len(data)
        return data

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        if self._counter is not None:
            self._counter.exit()

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


class FakeUpstream:
    """替身上游：按 URL 分发鉴权/对话请求，对话响应按脚本依次吐出。"""

    def __init__(self, access_tokens: list[str] | None = None) -> None:
        self.real_urlopen = urllib.request.urlopen  # 本机代理请求要放行到真 socket
        self.access_tokens = access_tokens or ["at-1", "at-2", "at-3", "at-4", "at-5"]
        self.script: list[tuple] = []
        self.calls: list[str] = []
        self.bodies: list[str] = []
        self.stream_calls = 0
        self.refresh_calls = 0
        self.guest_calls = 0
        self.auth_fail_seeds: set[str] = set()
        self.rotate_to: str | None = None
        self.counter = Counter()
        self._token_index = 0
        self._lock = threading.Lock()

    # ── urlopen 替身 ──
    def __call__(self, req, timeout=None):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        if url.startswith(("http://127.0.0.1", "http://localhost")):
            return self.real_urlopen(req, timeout=timeout)
        with self._lock:
            self.calls.append(url)
            if getattr(req, "data", None):
                self.bodies.append(req.data.decode("utf-8"))

        if "/user-api/user/refresh" in url:
            return self._refresh(req)
        if "/user-api/guest/access" in url:
            with self._lock:
                self.guest_calls += 1
            return FakeResponse(json.dumps({"result": {"access_token": "guest-token"}}).encode())
        if "/conversation/delete" in url:
            return FakeResponse(json.dumps({"result": {"status": "ok"}}).encode())

        with self._lock:
            self.stream_calls += 1
        item = self.script.pop(0) if self.script else ("sse", SSE_TEXT)
        kind = item[0]
        if kind == "sse":
            return FakeResponse(item[1].encode("utf-8"), "text/event-stream", self.counter)
        if kind == "sse_cut":     # 读到第 cut_after 字节就抛 IncompleteRead（上游掐线）
            return FakeResponse(item[1].encode("utf-8"), "text/event-stream",
                                self.counter, cut_after=item[2])
        if kind in ("json", "busy"):  # 业务错误 JSON（busy = 并发闸类）
            payload = json.dumps(item[1], ensure_ascii=False).encode()
            return FakeResponse(payload, "application/json")
        if kind == "http":
            raise urllib.error.HTTPError(url, item[1], "err", {}, io.BytesIO(item[2].encode()))
        raise AssertionError(f"未知脚本项: {item!r}")

    def _refresh(self, req):
        seed = (req.get_header("Authorization") or "").replace("Bearer ", "")
        with self._lock:
            self.refresh_calls += 1
        if seed in self.auth_fail_seeds:
            raise urllib.error.HTTPError(
                req.full_url, 401, "Unauthorized", {}, io.BytesIO(b'{"message":"token expired"}')
            )
        with self._lock:
            token = self.access_tokens[self._token_index % len(self.access_tokens)]
            self._token_index += 1
        result = {"access_token": token}
        if self.rotate_to:
            result["refresh_token"] = self.rotate_to
        return FakeResponse(json.dumps({"result": result}).encode())


class FakeUpstreamCase(unittest.TestCase):
    """装上/卸下假上游的公共基类。"""

    def setUp(self) -> None:
        self.fake = FakeUpstream()
        self._real_urlopen = urllib.request.urlopen
        urllib.request.urlopen = self.fake

    def tearDown(self) -> None:
        urllib.request.urlopen = self._real_urlopen

    def build_client(self, seeds: list[str], store: gp.TokenStore = None, **overrides):
        """按给定 token 种子构造 (config, accounts, pool, client)。"""
        config = make_config(**overrides)
        store = store if store is not None else gp.TokenStore("", False)
        accounts = [gp.Account(config, f"账号{i}", seed, store) for i, seed in enumerate(seeds, 1)]
        pool = gp.AccountPool(config, accounts)
        return config, accounts, pool, gp.GLMClient(config, pool)

    def consume(self, resp, lease, pause: float = 0.0) -> str:
        """把上游流读完（可选模拟生成耗时），再释放租约。返回原始文本。"""
        chunks = []
        while True:
            raw = resp.read(4096)
            if not raw:
                break
            chunks.append(raw)
            if pause:
                time.sleep(pause)
        resp.close()
        lease.release()
        return b"".join(chunks).decode("utf-8")


# ─────────────────────────── 纯函数 / 配置 ───────────────────────────
class PureLogicTest(unittest.TestCase):
    def test_build_sign_matches_client_algorithm(self):
        timestamp, nonce, sign = gp.build_sign()
        self.assertEqual(len(timestamp), 13)
        self.assertTrue(timestamp.isdigit())
        digits = [int(ch) for ch in timestamp]
        self.assertEqual(digits[-2], (sum(digits) - digits[-2]) % 10)
        self.assertEqual(len(nonce), 32)
        expect = hashlib.md5(f"{timestamp}-{nonce}-{gp.SIGN_SECRET}".encode()).hexdigest()
        self.assertEqual(sign, expect)

    def test_convert_messages_flattens_multiturn(self):
        messages = [
            {"role": "system", "content": "你是助手"},
            {"role": "user", "content": "第一问"},
            {"role": "assistant", "content": "第一答"},
            {"role": "user", "content": [{"type": "text", "text": "第二问"}]},
        ]
        out = gp.convert_messages(messages)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["role"], "user")
        text = out[0]["content"][0]["text"]
        self.assertIn("# INSTRUCTIONS", text)
        self.assertIn("你是助手", text)
        self.assertIn("User: 第一问", text)
        self.assertIn("Assistant: 第一答", text)
        self.assertTrue(text.endswith("Assistant: "))

    def test_extract_networking_forms(self):
        self.assertTrue(gp.extract_networking({"glm": {"networking": True}}))
        self.assertTrue(gp.extract_networking({"glm_networking": True}))
        self.assertTrue(gp.extract_networking({"tools": [{"type": "web_search"}]}))
        self.assertFalse(gp.extract_networking({}))
        self.assertTrue(gp.extract_networking({}, default=True))
        self.assertFalse(gp.extract_networking({"tools": [{"type": "function"}]}))

    def test_extract_deep_thinking_forms(self):
        """深度思考的三种请求体写法；显式 false 要能压过全局默认的 true。"""
        for payload in ({"glm": {"deep_thinking": True}},
                        {"glm_deep_thinking": True},
                        {"deep_thinking": True}):
            self.assertTrue(gp.extract_deep_thinking(payload), payload)
        # 没写 → 用全局默认值（GLM_DEEP_THINKING）
        self.assertTrue(gp.extract_deep_thinking({}, default=True))
        self.assertFalse(gp.extract_deep_thinking({}, default=False))
        # 全局开着，单次请求显式关掉
        self.assertFalse(gp.extract_deep_thinking({"glm": {"deep_thinking": False}}, default=True))
        self.assertFalse(gp.extract_deep_thinking({"glm_deep_thinking": False}, default=True))
        self.assertFalse(gp.extract_deep_thinking({"deep_thinking": False}, default=True))
        # 只开联网不该被误判成深度思考
        self.assertFalse(gp.extract_deep_thinking({"glm": {"networking": True}}))

    def test_config_deep_thinking_env(self):
        with env_patch({"GLM_DEEP_THINKING": "true"}):
            self.assertTrue(gp.Config().deep_thinking)
        with env_patch({}):
            self.assertFalse(gp.Config().deep_thinking)

    def test_has_function_tools(self):
        self.assertTrue(gp.has_function_tools({"tools": [{"type": "function", "function": {}}]}))
        self.assertFalse(gp.has_function_tools({"tools": [{"type": "web_search"}]}))
        self.assertFalse(gp.has_function_tools({}))

    def test_busy_payload_classification(self):
        self.assertTrue(gp.is_busy_payload(BUSY_PAYLOAD))
        self.assertTrue(gp.is_busy_payload({"code": 429, "message": "Too Many Requests"}))
        self.assertFalse(gp.is_busy_payload({"code": 400, "message": "参数不合法"}))

    def test_config_reads_multi_tokens(self):
        with env_patch({
            "GLM_REFRESH_TOKEN": "seed-single",
            "GLM_REFRESH_TOKENS": "seed-a, seed-b\nseed-a",
            "GLM_QUEUE_TIMEOUT": "12",
            "GLM_BUSY_RETRIES": "5",
        }):
            config = gp.Config()
        self.assertEqual(config.refresh_tokens, ["seed-single", "seed-a", "seed-b"])
        self.assertEqual(config.queue_timeout, 12.0)
        self.assertEqual(config.busy_retries, 5)

    def test_parse_textual_tool_calls(self):
        """Cherry 等客户端自带的 list/inspect/invoke/exec 说明会让模型用 JS 风格写调用。"""
        allowed = {"mcp__CherryHub__list", "mcp__CherryHub__inspect",
                   "mcp__CherryHub__invoke", "mcp__CherryHub__exec"}
        # 最简形态
        calls = gp.parse_textual_tool_calls("list({ limit: 50 })", allowed)
        self.assertEqual(calls[0]["function"]["name"], "mcp__CherryHub__list")
        self.assertEqual(json.loads(calls[0]["function"]["arguments"]), {"limit": 50})
        # 嵌套对象 + 单引号 + 尾逗号 + 前后有叙述/代码块（真实的失败场景）
        text = (
            "我来 list 工具：\n\n```javascript\ninspect({ name: 'writeFile', }\n```\n"
            "然后写入：\n\n"
            "```javascript\ninvoke({ name: \"writeFile\", params: { path: './a.txt', "
            "content: '你好world' } })\n```\n完成"
        )
        calls = gp.parse_textual_tool_calls(text, allowed)
        self.assertEqual([c["function"]["name"] for c in calls],
                         ["mcp__CherryHub__inspect", "mcp__CherryHub__invoke"])
        args = json.loads(calls[1]["function"]["arguments"])
        self.assertEqual(args, {"name": "writeFile",
                                "params": {"path": "./a.txt", "content": "你好world"}})
        # 客户端没注册对应工具 / 普通文字 / 空 → None（退化成聊天）
        self.assertIsNone(gp.parse_textual_tool_calls("list({ limit: 50 })", {"other_tool"}))
        self.assertIsNone(gp.parse_textual_tool_calls("北京今天晴，25 度。", allowed))
        self.assertIsNone(gp.parse_textual_tool_calls("", allowed))
        # 括号不闭合也不能崩
        self.assertIsNone(gp.parse_textual_tool_calls("invoke({ name: \"x\" ", allowed))

    def test_parse_textual_tool_calls_prefers_long_tool_names(self):
        allowed = {"mcp__fs__write", "mcp__CherryHub__write"}
        calls = gp.parse_textual_tool_calls("write({ path: 'a' })", allowed)
        self.assertIn(calls[0]["function"]["name"], allowed)  # 能映射上就行

    def test_parse_textual_tool_calls_accepts_full_tool_names(self):
        """真实失败场景：模型写的是完整工具名 mcp__CherryHub__invoke(...)。

        旧实现左边界用 \\b，而完整名里 invoke 前面是下划线（属 \\w），没有 word boundary，
        整条调用会被静默丢弃 → 客户端收不到 tool_calls。
        """
        allowed = {"mcp__CherryHub__list", "mcp__CherryHub__inspect",
                   "mcp__CherryHub__invoke", "mcp__CherryHub__exec"}
        text = ('<tool_call>mcp__CherryHub__invoke({"name": "CherryBraveSearchBraveWebSearch", '
                '"params": {"query": "北京 景点 今日 客流 人数"}})')
        calls = gp.parse_textual_tool_calls(text, allowed)
        self.assertIsNotNone(calls)
        self.assertEqual(calls[0]["function"]["name"], "mcp__CherryHub__invoke")
        self.assertEqual(json.loads(calls[0]["function"]["arguments"]),
                         {"name": "CherryBraveSearchBraveWebSearch",
                          "params": {"query": "北京 景点 今日 客流 人数"}})
        # 完整名与短名混用：两种写法都要认
        mixed = "先 list({ limit: 5 })，再 mcp__CherryHub__invoke({ name: 'x' })"
        names = [c["function"]["name"]
                 for c in gp.parse_textual_tool_calls(mixed, allowed)]
        self.assertEqual(names, ["mcp__CherryHub__list", "mcp__CherryHub__invoke"])
        # 前缀是普通标识符字符（foo_invoke）→ 不是完整工具名，拒绝，避免误伤
        self.assertIsNone(gp.parse_textual_tool_calls("foo_invoke({ a: 1 })", allowed))

    def test_config_models_list(self):
        with env_patch({}):
            self.assertIn("glm-5.3", gp.Config().models)  # 默认清单
        with env_patch({"GLM_MODELS": "glm-5.3, my-model;"}):
            self.assertEqual(gp.Config().models, ["glm-5.3", "my-model"])

    def test_parse_model_map(self):
        self.assertEqual(
            gp.parse_model_map("glm-5.3=65940acff94777010aa6b796, GLM-4.6:aaaabbbbccccddddeeeeffff"),
            {"glm-5.3": "65940acff94777010aa6b796",
             "glm-4.6": "aaaabbbbccccddddeeeeffff"},
        )
        self.assertEqual(gp.parse_model_map(""), {})
        self.assertEqual(gp.parse_model_map("bad,also-bad"), {})       # 没有分隔符 → 忽略
        self.assertEqual(gp.parse_model_map("glm-5.3="), {})           # 缺值 → 忽略

    def test_model_map_from_env(self):
        with env_patch({"GLM_MODEL_ASSISTANT_MAP": "glm-5.3=deadbeefdeadbeefdeadbeef"}):
            config = gp.Config()
        self.assertEqual(config.model_assistant_map, {"glm-5.3": "deadbeefdeadbeefdeadbeef"})

    def test_normalize_route_aliases(self):
        """GUI 客户端对 /v1 的拼接五花八门，路由要都能认。"""
        cases = {
            "/v1/models": "/v1/models",
            "/models": "/v1/models",
            "/v1/models/": "/v1/models",
            "/v1/models?x=1": "/v1/models",
            "/v1/v1/models": "/v1/models",
            "/v1/chat/completions": "/v1/chat/completions",
            "/chat/completions": "/v1/chat/completions",
            "/v1/v1/chat/completions": "/v1/chat/completions",
            "/health": "/health",
            "/": "/",
        }
        for raw, expect in cases.items():
            self.assertEqual(gp.normalize_route(raw), expect, raw)

    def test_split_tokens_variants(self):
        self.assertEqual(gp.split_tokens("a;b,c\n d ", "a"), ["a", "b", "c", "d"])
        self.assertEqual(gp.split_tokens(""), [])

    def test_convert_messages_keeps_tool_context(self):
        """拍平时不能把 tool_calls / role:"tool" 丢掉，否则多轮工具上下文全失真。"""
        messages = [
            {"role": "user", "content": "北京天气"},
            {"role": "assistant", "content": None, "tool_calls": [
                {"id": "c1", "type": "function",
                 "function": {"name": "get_weather", "arguments": '{"city":"北京"}'}},
            ]},
            {"role": "tool", "tool_call_id": "c1", "content": "晴 25 度"},
            {"role": "user", "content": "要穿外套吗"},
        ]
        text = gp.convert_messages(messages)[0]["content"][0]["text"]
        self.assertIn('get_weather({"city":"北京"})', text)  # 行首不带前缀（见 ToolProtocolHintTest）
        self.assertIn("Tool(get_weather): 晴 25 度", text)  # 工具名按 call_id 回填
        self.assertIn("User: 要穿外套吗", text)

    def test_convert_messages_keeps_empty_tool_result(self):
        """客户端工具执行失败/超时 → 回传空结果，不能整条丢弃（否则上下文断层）。

        真实场景：模型反复调 CherryHub 搜索工具、工具一直超时，客户端回传空 content，
        旧实现把这条 tool 消息丢掉，上游只看到「Assistant 要用工具，然后没了」。
        """
        messages = [
            {"role": "user", "content": "北京今天景点客流"},
            {"role": "assistant", "content": "", "tool_calls": [
                {"id": "c1", "type": "function",
                 "function": {"name": "mcp__CherryHub__invoke", "arguments": "{}"}},
            ]},
            {"role": "tool", "tool_call_id": "c1", "content": ""},
        ]
        text = gp.convert_messages(messages)[0]["content"][0]["text"]
        # 工具名按 call_id 回填，且必须留下「调用过但没结果」的显式痕迹
        self.assertIn("Tool(mcp__CherryHub__invoke):", text)
        self.assertIn(gp.EMPTY_TOOL_RESULT_HINT, text)

    def test_convert_messages_injects_extra_instructions(self):
        tools = gp.extract_tool_definitions({"tools": [WEATHER_TOOL]})
        text = gp.convert_messages(
            [{"role": "user", "content": "北京天气"}], gp.render_tools_prompt(tools)
        )[0]["content"][0]["text"]
        self.assertIn("# TOOLS", text)
        self.assertIn("get_weather：查询指定城市的天气", text)
        self.assertIn(gp.TOOL_FRAME_TAIL, text)
        self.assertTrue(text.endswith("Assistant: "))

    def test_tools_protocol_follows_conversation(self):
        """协议块必须在对话**之后**：放头部时被长 system prompt 埋掉，模型整块无视（真机）。"""
        tools = gp.extract_tool_definitions({"tools": [WEATHER_TOOL]})
        text = gp.convert_messages(
            [{"role": "system", "content": "你是助手"}, {"role": "user", "content": "北京天气"}],
            gp.render_tools_prompt(tools),
        )[0]["content"][0]["text"]
        self.assertLess(text.rindex("# INSTRUCTIONS"), text.rindex("# CONVERSATION"))
        # index 而非 rindex：TOOL_FRAME_TAIL 里也写着「按上面 # TOOLS 的协议」，那处不是协议块起点
        head = text.index("# TOOLS")
        self.assertLess(text.rindex("# CONVERSATION"), head)
        self.assertLess(head, text.rindex(gp.TOOL_FRAME_TAIL))
        self.assertTrue(text.endswith("Assistant: "))

    def test_no_tool_frame_tail_without_tools(self):
        """没有协议块时不许出现「按上面 # TOOLS 的协议」—— 那会指向不存在的内容。"""
        text = gp.convert_messages([{"role": "user", "content": "你好"}])[0]["content"][0]["text"]
        self.assertNotIn(gp.TOOL_FRAME_TAIL, text)
        self.assertNotIn("# TOOLS", text)

    def test_extract_tool_definitions(self):
        payload = {"tools": [WEATHER_TOOL, {"type": "web_search"}, {"type": "function"}]}
        tools = gp.extract_tool_definitions(payload)
        self.assertEqual([t["name"] for t in tools], ["get_weather"])
        self.assertEqual(gp.extract_tool_definitions({}), [])
        self.assertEqual(gp.extract_tool_definitions({"tools": "not-a-list"}), [])

    def test_parse_tool_calls_variants(self):
        allowed = {"get_weather"}
        # 标准形状
        calls = gp.parse_tool_calls(
            '{"tool_calls":[{"name":"get_weather","arguments":{"city":"北京"}}]}', allowed
        )
        self.assertEqual(calls[0]["function"]["name"], "get_weather")
        self.assertEqual(json.loads(calls[0]["function"]["arguments"]), {"city": "北京"})
        self.assertTrue(calls[0]["id"].startswith("call_"))
        self.assertEqual(calls[0]["type"], "function")
        # 单对象形状
        self.assertEqual(
            len(gp.parse_tool_calls('{"name":"get_weather","arguments":"{}"}', allowed)), 1
        )
        # 被 ``` 包起来 / 前后有解释文字，也能抠出来
        self.assertIsNotNone(gp.parse_tool_calls(
            '好的：\n```json\n{"tool_calls":[{"name":"get_weather","arguments":{"city":"沪"}}]}\n```',
            allowed,
        ))
        # 多个工具（不同参数）—— 测的是「多重调用」都要保留，同名同参的重复项另有去重
        self.assertEqual(len(gp.parse_tool_calls(
            '{"tool_calls":[{"name":"get_weather","arguments":{"city":"京"}},'
            '{"name":"get_weather","arguments":{"city":"沪"}}]}',
            allowed,
        )), 2)
        # 尾随杂质：上游实测会在 {"tool_calls":[...]} 后再补一个 }（...}]}]}），
        # 只解析第一个完整对象即可，别把本该是工具调用的输出误判成普通回答。
        tail = gp.parse_tool_calls(
            '{"tool_calls":[{"name":"get_weather","arguments":{"city":"京"}}]}]}', allowed
        )
        self.assertEqual(len(tail), 1)
        self.assertEqual(json.loads(tail[0]["function"]["arguments"]), {"city": "京"})
        # 另一种畸形：多余的 } 夹在数组闭合 ] 之前（...}}}}]}），raw_decode 也会失败，
        # 需要靠括号配平兜底，否则同样会被误判成普通回答。
        brace = gp.parse_tool_calls(
            '{"tool_calls":[{"name":"get_weather","arguments":{"city":"沪"}}}}]}', allowed
        )
        self.assertEqual(len(brace), 1)
        self.assertEqual(json.loads(brace[0]["function"]["arguments"]), {"city": "沪"})
        # 不是工具调用（普通回答 / 未知工具名 / 坏 JSON）→ 一律返回 None，退化成聊天
        self.assertIsNone(gp.parse_tool_calls("北京今天晴，25 度。", allowed))
        self.assertIsNone(gp.parse_tool_calls('{"tool_calls":[{"name":"rm_rf","arguments":{}}]}', allowed))
        self.assertIsNone(gp.parse_tool_calls('{"tool_calls":[{"name":"get_weather"', allowed))
        self.assertIsNone(gp.parse_tool_calls("", allowed))


# ─────────────────────────── 重复调用去重 ───────────────────────────
class DuplicateCallDedupeTest(unittest.TestCase):
    """上游实测会把同一个工具块输出两遍（先是半截、最后是完整版）。

    两处都能解析成合法调用，不去重客户端就执行两次 —— 写文件/发消息这类工具
    重复执行有真实副作用，不是单纯的显示问题。
    """

    def test_identical_duplicate_calls_collapse_to_one(self):
        calls = gp.parse_tool_calls(
            '{"tool_calls":[{"name":"get_weather","arguments":{"city":"北京"}},'
            '{"name":"get_weather","arguments":{"city":"北京"}}]}', {"get_weather"}
        )
        self.assertEqual(len(calls), 1)
        self.assertEqual(json.loads(calls[0]["function"]["arguments"]), {"city": "北京"})

    def test_dedupe_keeps_the_first_call_id(self):
        first = {"id": "call_1", "function": {"name": "t", "arguments": '{"a":1}'}}
        second = {"id": "call_2", "function": {"name": "t", "arguments": '{"a":1}'}}
        self.assertEqual([c["id"] for c in gp._dedupe_calls([first, second])], ["call_1"])

    def test_argument_key_order_does_not_count_as_different(self):
        left = {"function": {"name": "t", "arguments": '{"a":1,"b":2}'}}
        right = {"function": {"name": "t", "arguments": '{"b":2,"a":1}'}}
        self.assertEqual(len(gp._dedupe_calls([left, right])), 1)

    def test_same_name_different_args_are_kept(self):
        calls = gp.parse_tool_calls(
            '{"tool_calls":[{"name":"get_weather","arguments":{"city":"北京"}},'
            '{"name":"get_weather","arguments":{"city":"上海"}}]}', {"get_weather"}
        )
        self.assertEqual(len(calls), 2)
        self.assertEqual(
            [json.loads(c["function"]["arguments"])["city"] for c in calls], ["北京", "上海"]
        )

    def test_textual_duplicates_collapse(self):
        calls = gp.parse_textual_tool_calls(
            'get_weather({"city": "北京"})\nget_weather({"city": "北京"})', {"get_weather"}
        )
        self.assertEqual(len(calls), 1)

    def test_duplicate_does_not_waste_a_slot(self):
        """去重要在限数之前做：否则重复项白占一个额度，最后一个真调用被挤掉。"""
        text = "\n".join([
            'a({"x": 1})', 'a({"x": 1})',
            'b({"x": 2})', 'c({"x": 3})', 'd({"x": 4})', 'e({"x": 5})',
        ])
        calls = gp.parse_textual_tool_calls(text, {"a", "b", "c", "d", "e"})
        self.assertEqual(len(calls), gp.TEXTUAL_CALL_MAX)
        self.assertEqual([c["function"]["name"] for c in calls], ["a", "b", "c", "d"])


# ─────────────────────────── 流式增量（乱序/改写防护） ───────────────────────────
def sse_event(text: str, status: str = "generating", logic_id: str = "p1",
              think: str = "", conversation_id: str = "c1", model: str = "") -> dict:
    content = []
    if text:
        content.append({"type": "text", "text": text})
    if think:
        content.append({"type": "think", "think": think})
    part = {"logic_id": logic_id, "content": content}
    if model:
        part["model"] = model
    return {"conversation_id": conversation_id, "status": status, "parts": [part]}


class StreamAccumulatorTest(unittest.TestCase):
    def test_served_model_is_captured(self):
        """上游会在 part 里回报实际模型（moe_53f = MoE GLM-5.3 flash），要抓下来。"""
        acc = gp.StreamAccumulator()
        acc.consume(sse_event("你好", model="moe_53f"))
        self.assertEqual(acc.served_model, "moe_53f")
        self.assertEqual(acc.full_text(), "你好")

    def test_snapshot_replacing_emitted_fragment_stays_complete(self):
        """罕见兜底：末态快照与已发出的分片不连续时，补齐而不是整段丢弃。

        SSE 只能追加，已发出去的字节收不回来，所以接缝处会重复几个字。
        旧行为是「冲突就放弃」—— 输出看起来更干净，但实测会把整段答案丢掉。
        抓帧实测这个形态几乎不会出现（分片是有序的），留作保险。
        """
        acc = gp.StreamAccumulator()
        streamed = "".join(
            acc.consume(event)[0]
            for event in (sse_event("2\n3"), sse_event("1\n2\n3"),
                          sse_event("1\n2\n3", "finish"))
        )
        self.assertEqual(streamed, "2\n31\n2\n3")      # 内容全在，接缝重复
        self.assertEqual(acc.full_text(), "1\n2\n3")   # 末态以快照为准
        self.assertGreater(acc.rewrites, 0)
        self.assertGreater(acc.rescued, 0)

    def test_ordered_deltas_stream_out_immediately(self):
        """上游真实形态（抓帧实测）：有序增量分片 + 末尾一次完整快照。

        修前：每帧覆盖 parts，分片全被丢掉，思维链只能在末态快照那一下吐出来 ——
        客户端于是「思考几分钟、一个字都不出」。
        """
        chunks = ["1.  **分析", "请求：**\n", "    *   用户", "要三句话", "\n2.  **起草**"]
        truth = "".join(chunks)
        acc = gp.StreamAccumulator()
        out = [acc.consume(sse_event(chunk))[0] for chunk in chunks]
        self.assertEqual("".join(out), truth)                      # 逐字流出去
        self.assertEqual(len([x for x in out if x]), len(chunks))
        out.append(acc.consume(sse_event(truth, "finish"))[0])     # 末尾快照
        self.assertEqual("".join(out), truth)                      # 快照不重复吐
        self.assertEqual(acc.full_text(), truth)
        self.assertEqual(acc.rewrites, 0)

    def test_duplicate_frames_are_not_emitted_twice(self):
        """上游会逐字重复推同一帧，必须识别成「没有新内容」。"""
        acc = gp.StreamAccumulator()
        streamed = "".join(
            acc.consume(event)[0]
            for event in (sse_event("你好"), sse_event("你好"), sse_event("你好", "finish"))
        )
        self.assertEqual(streamed, "你好")
        self.assertEqual(acc.full_text(), "你好")

    def test_cut_stream_keeps_every_received_fragment(self):
        """没等到末态快照就断流：已收到的分片仍然拼得出完整内容，不再只剩碎片。"""
        acc = gp.StreamAccumulator()
        streamed = ""
        for chunk in ("第一段", "第二段", "第三段"):
            streamed += acc.consume(sse_event(chunk))[0]
        self.assertEqual(streamed, "第一段第二段第三段")
        self.assertEqual(acc.full_text(), "第一段第二段第三段")
        self.assertEqual(acc.finalize()[0], "")

    def test_platform_tool_call_is_recorded_not_leaked(self):
        """上游平台自带工具（search 等）：记账，但绝不进正文、也不变成客户端的 tool_calls。

        抓帧实测：``{"type":"tool_calls","tool_calls":{"id","name","arguments"}}``，
        紧跟一个 ``tool_result`` —— 上游已经自己跑完了，客户端无从执行。
        """
        part = {
            "logic_id": "p1",
            "content": [
                {"type": "tool_calls", "tool_calls": {
                    "id": "call-1", "name": "search",
                    "arguments": '{"query": "北京 天气"}'}},
                {"type": "tool_result", "tool_result": {"id": "call-1", "content": "晴 21 度"}},
                {"type": "text", "text": "北京今天晴，21 度。"},
            ],
        }
        acc = gp.StreamAccumulator()
        event = {"conversation_id": "c1", "status": "generating", "parts": [part]}
        text, reasoning = acc.consume(event)
        self.assertEqual(acc.platform_tools, ["search"])
        self.assertEqual(text, "北京今天晴，21 度。")
        self.assertEqual(reasoning, "")
        self.assertNotIn("tool_calls", acc.full_text())
        self.assertNotIn("search", acc.full_text())

    def test_platform_tool_call_logged_once_per_part(self):
        """同一帧被重复推（上游常态）：同一个 part 的同一个工具只记一次。"""
        part = {
            "logic_id": "p1",
            "content": [{"type": "tool_calls", "tool_calls": {"name": "sandbox",
                                                              "arguments": "{}"}}],
        }
        event = {"conversation_id": "c1", "status": "generating", "parts": [part]}
        acc = gp.StreamAccumulator()
        for _ in range(3):
            acc.consume(event)
        self.assertEqual(acc.platform_tools, ["sandbox"])

    def test_platform_tool_call_without_name_is_ignored(self):
        """形状对不上（没有 name）就什么都不记，也别把 JSON 漏进正文。"""
        part = {"logic_id": "p1", "content": [{"type": "tool_calls", "tool_calls": {}}]}
        acc = gp.StreamAccumulator()
        acc.consume({"conversation_id": "c1", "status": "finish", "parts": [part]})
        self.assertEqual(acc.platform_tools, [])
        self.assertEqual(acc.full_text(), "")

    def test_normal_incremental_streaming(self):
        """每帧都推完整累积值（快照式）：新增的尾巴立刻发出，边生边出。"""
        acc = gp.StreamAccumulator()
        out = []
        for event in (sse_event("你好"), sse_event("你好世界"),
                      sse_event("你好世界，今天"), sse_event("你好世界，今天晴天", "finish")):
            out.append(acc.consume(event)[0])
        self.assertEqual("".join(out), "你好世界，今天晴天")
        self.assertGreater(len("".join(out[:2])), 0)   # 不是全部堆到最后

    def test_tiny_confirmed_fragment_does_not_freeze_the_stream(self):
        """线上复现：上游确认过一个两字碎片后整段改写，旧行为永久冻结在 'to'。

        客户端表现为「已深度思考（用时 501 秒）」却没有任何内容 —— 思维链槽位
        一旦和真值不再互为前缀就再也发不出东西了。
        """
        truth = "Count 1 to 3. 这就是最终答案。"
        acc = gp.StreamAccumulator()
        streamed = "".join(
            acc.consume(event)[0]
            for event in (sse_event("to"), sse_event("to"),
                          sse_event("Count 1 to 3."), sse_event(truth, "finish"))
        )
        self.assertEqual(streamed, "to" + truth)
        self.assertIn("这就是最终答案", streamed)
        self.assertGreater(acc.rewrites, 0)

    def test_finalize_flushes_without_a_finish_event(self):
        """上游没推 finish 就断流：finalize() 兜住最后没吐出去的部分，且幂等。"""
        acc = gp.StreamAccumulator()
        streamed = acc.consume(sse_event("前半段"))[0]
        streamed += acc.consume(sse_event("前半段后半段"))[0]
        self.assertEqual(streamed, "前半段后半段")                 # 增量已经直接外发
        self.assertEqual(acc.finalize()[0], "")                    # 没有欠账
        self.assertEqual(acc.finalize()[0], "")                    # 重复调用安全

    def test_finalize_after_finish_is_noop(self):
        acc = gp.StreamAccumulator()
        streamed = "".join(
            acc.consume(event)[0]
            for event in (sse_event("完整回答"), sse_event("完整回答", "finish"))
        )
        self.assertEqual(streamed, "完整回答")
        self.assertEqual(acc.finalize()[0], "")

    def test_reasoning_and_text_are_separated(self):
        acc = gp.StreamAccumulator()
        events = [
            sse_event("", think="先想一下"),
            sse_event("", think="先想一下再答"),
            sse_event("答案", think="先想一下再答", status="finish"),
        ]
        texts, reasons = [], []
        for event in events:
            text_delta, reason_delta = acc.consume(event)
            texts.append(text_delta)
            reasons.append(reason_delta)
        self.assertEqual("".join(texts), "答案")
        self.assertEqual("".join(reasons), "先想一下再答")
        self.assertEqual(acc.full_reasoning(), "先想一下再答")

    def test_multiple_parts_are_joined(self):
        acc = gp.StreamAccumulator()
        out = []
        for event in (
            sse_event("第一段", logic_id="p1"),
            sse_event("第一段", logic_id="p1"),
            sse_event("第二段", logic_id="p2"),
            sse_event("第二段", logic_id="p2", status="finish"),
        ):
            out.append(acc.consume(event)[0])
        self.assertEqual("".join(out), "第一段\n\n第二段")


# ─────────────────────────── 账号 / token ───────────────────────────
class AccountStoreTest(FakeUpstreamCase):
    def test_access_token_is_cached(self):
        _, accounts, _, _ = self.build_client(["seed-1"])
        account = accounts[0]
        self.assertEqual(account.get_access_token(), account.get_access_token())
        self.assertEqual(self.fake.refresh_calls, 1)

    def test_guest_account_uses_guest_endpoint(self):
        _, accounts, _, _ = self.build_client([""])
        self.assertTrue(accounts[0].is_guest)
        self.assertEqual(accounts[0].get_access_token(), "guest-token")
        self.assertEqual(self.fake.guest_calls, 1)
        self.assertEqual(self.fake.refresh_calls, 0)

    def test_rotated_token_is_persisted_and_reloadable(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "tokens.json")
            self.fake.rotate_to = "rotated-token"
            _, accounts, _, _ = self.build_client(["seed-1"], store=gp.TokenStore(path, True))
            accounts[0].get_access_token()
            self.assertEqual(accounts[0].refresh_token, "rotated-token")
            with open(path, encoding="utf-8") as fh:
                saved = json.load(fh)
            self.assertEqual(
                saved["accounts"], [{"seed": "seed-1", "refresh_token": "rotated-token"}]
            )
            # 新的 store 应把 .env 里的旧 seed 解析成最新 token
            self.assertEqual(gp.TokenStore(path, True).resolve("seed-1"), "rotated-token")

    def test_persist_disabled_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "tokens.json")
            self.fake.rotate_to = "rotated-token"
            _, accounts, _, _ = self.build_client(["seed-1"], store=gp.TokenStore(path, False))
            accounts[0].get_access_token()
            self.assertEqual(accounts[0].refresh_token, "rotated-token")
            self.assertFalse(os.path.exists(path))

    def test_build_accounts_dedupes_and_falls_back_to_guest(self):
        with env_patch({"GLM_REFRESH_TOKEN": "seed-1", "GLM_REFRESH_TOKENS": "seed-1,seed-2"}):
            accounts = gp.build_accounts(gp.Config(), gp.TokenStore("", False))
        self.assertEqual([a.name for a in accounts], ["账号1", "账号2"])
        self.assertEqual([a.refresh_token for a in accounts], ["seed-1", "seed-2"])

        with env_patch({}):
            accounts = gp.build_accounts(gp.Config(), gp.TokenStore("", False))
        self.assertEqual(len(accounts), 1)
        self.assertTrue(accounts[0].is_guest)

        with env_patch({"GLM_REFRESH_TOKEN": "seed-1", "GLM_USE_GUEST": "true"}):
            accounts = gp.build_accounts(gp.Config(), gp.TokenStore("", False))
        self.assertTrue(accounts[0].is_guest)

    def test_account_cooldown_blocks_selection(self):
        _, accounts, pool, _ = self.build_client(["seed-1", "seed-2"])
        accounts[0].cooldown(5)
        lease = pool.acquire()  # 冷却中的账号不会被选中
        self.assertEqual(lease.account.name, "账号2")
        lease.release()
        self.assertFalse(accounts[0].available)
        self.assertTrue(accounts[1].available)
        self.assertTrue(accounts[0].try_acquire())  # 槽位本身没被泄漏
        accounts[0].release()


# ─────────────────────────── 并发闸 / 重试 ───────────────────────────
class ConcurrencyTest(FakeUpstreamCase):
    def test_single_account_serializes_requests(self):
        """3 个并发请求打同一账号，上游同一时刻只应看到 1 个生成。"""
        config, accounts, _, client = self.build_client(["seed-1"])
        self.fake.script = [("sse", SSE_TEXT)] * 3
        done: list[str] = []

        def worker():
            lease, resp = client.open_stream([BUSY_REQUEST], "glm-4")
            self.consume(resp, lease, pause=0.03)
            done.append("ok")

        threads = [threading.Thread(target=worker) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        self.assertEqual(done, ["ok"] * 3)
        self.assertEqual(self.fake.stream_calls, 3)
        self.assertEqual(self.fake.counter.max_active, 1)  # 关键断言：从未并发
        self.assertTrue(accounts[0].try_acquire())
        accounts[0].release()

    def test_two_accounts_run_in_parallel(self):
        config, _, _, client = self.build_client(["seed-1", "seed-2"])
        self.fake.script = [("sse", SSE_TEXT)] * 2
        barrier = threading.Barrier(2)
        done: list[str] = []

        def worker():
            lease, resp = client.open_stream([BUSY_REQUEST], "glm-4")
            barrier.wait(timeout=10)  # 两个账号都拿到槽位后才继续读流
            self.consume(resp, lease, pause=0.03)
            done.append("ok")

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        self.assertEqual(sorted(done), ["ok", "ok"])
        self.assertEqual(self.fake.counter.max_active, 2)  # 账号之间并行

    def test_queue_timeout_when_account_is_stuck(self):
        config, accounts, pool, client = self.build_client(["seed-1"], queue_timeout=0.3)
        self.assertTrue(accounts[0].try_acquire())  # 手工占住槽位，模拟卡死的生成
        try:
            with self.assertRaises(gp.QueueTimeout):
                pool.acquire()
            with self.assertRaises(gp.QueueTimeout):
                client.open_stream([BUSY_REQUEST], "glm-4")
        finally:
            accounts[0].release()

    def test_busy_gate_is_retried_then_succeeds(self):
        config, _, _, client = self.build_client(["seed-1"], busy_retries=3)
        self.fake.script = [("busy", BUSY_PAYLOAD), ("busy", BUSY_PAYLOAD), ("sse", SSE_TEXT)]
        lease, resp = client.open_stream([BUSY_REQUEST], "glm-4")
        text = self.consume(resp, lease)
        self.assertIn("你好世界", text)
        self.assertEqual(self.fake.stream_calls, 3)  # 前两次撞闸被本地重试掉了

    def test_http_429_is_treated_as_busy(self):
        config, _, _, client = self.build_client(["seed-1"], busy_retries=2)
        self.fake.script = [("http", 429, "too many requests"), ("sse", SSE_TEXT)]
        lease, resp = client.open_stream([BUSY_REQUEST], "glm-4")
        self.consume(resp, lease)
        self.assertEqual(self.fake.stream_calls, 2)

    def test_busy_retries_exhausted_releases_slot(self):
        config, accounts, _, client = self.build_client(["seed-1"], busy_retries=1)
        self.fake.script = [("busy", BUSY_PAYLOAD), ("busy", BUSY_PAYLOAD)]
        with self.assertRaises(gp.UpstreamBusy):
            client.open_stream([BUSY_REQUEST], "glm-4")
        self.assertEqual(self.fake.stream_calls, 2)
        self.assertTrue(accounts[0].try_acquire())  # 槽位必须归还
        accounts[0].release()

    def test_other_business_error_is_not_retried(self):
        config, accounts, _, client = self.build_client(["seed-1"], busy_retries=3)
        self.fake.script = [("json", {"code": 400, "message": "参数不合法"})]
        with self.assertRaises(RuntimeError) as ctx:
            client.open_stream([BUSY_REQUEST], "glm-4")
        self.assertIn("参数不合法", str(ctx.exception))
        self.assertEqual(self.fake.stream_calls, 1)  # 非并发类错误不重试
        self.assertTrue(accounts[0].try_acquire())
        accounts[0].release()


# ─────────────────────────── 多账号鉴权轮换 ───────────────────────────
class MultiAccountAuthTest(FakeUpstreamCase):
    def test_dead_account_rotates_to_next(self):
        _, accounts, pool, client = self.build_client(["dead-seed", "good-seed"])
        self.fake.auth_fail_seeds = {"dead-seed"}
        warmup = pool.acquire()  # 校准轮询起点，保证第一个被选中的就是失效账号
        warmup.release()
        lease, resp = client.open_stream([BUSY_REQUEST], "glm-4")
        self.assertEqual(lease.account.name, "账号2")
        self.consume(resp, lease)
        self.assertFalse(accounts[0].available)  # 失效账号被冷却
        self.assertTrue(accounts[1].available)

    def test_all_dead_accounts_fall_back_to_guest(self):
        _, accounts, _, client = self.build_client(["dead-1", "dead-2"])
        self.fake.auth_fail_seeds = {"dead-1", "dead-2"}
        lease, resp = client.open_stream([BUSY_REQUEST], "glm-4")
        self.assertEqual(lease.account.name, "游客")
        self.consume(resp, lease)
        self.assertEqual(self.fake.guest_calls, 1)
        self.assertFalse(accounts[0].available)
        self.assertFalse(accounts[1].available)

    def test_all_dead_accounts_without_guest_fallback_raises(self):
        _, accounts, _, client = self.build_client(
            ["dead-1", "dead-2"], guest_fallback=False, busy_retries=3
        )
        self.fake.auth_fail_seeds = {"dead-1", "dead-2"}
        with self.assertRaises(gp.UpstreamAuthError):
            client.open_stream([BUSY_REQUEST], "glm-4")
        self.assertEqual(self.fake.guest_calls, 0)
        # 两个账号的槽位都要归还，不能因为异常把账号永久锁死
        for account in accounts:
            self.assertTrue(account.try_acquire())
            account.release()

    def test_401_on_chat_refreshes_token_and_retries(self):
        config, accounts, _, client = self.build_client(["seed-1"])
        self.fake.script = [
            ("http", 401, '{"message":"unauthorized"}'),
            ("sse", SSE_TEXT),
        ]
        lease, resp = client.open_stream([BUSY_REQUEST], "glm-4")
        self.consume(resp, lease)
        self.assertEqual(self.fake.stream_calls, 2)
        self.assertGreaterEqual(self.fake.refresh_calls, 2)  # 首次取 token + 强制刷新


# ─────────────────────────── 真 HTTP 端到端 ───────────────────────────
class HttpEndToEndTest(FakeUpstreamCase):
    """起一个真的 ThreadingHTTPServer，验证路由、鉴权、流式/非流式与错误码映射。"""

    def setUp(self) -> None:
        super().setUp()
        self.config = make_config(server_api_keys=["secret"], networking=False)
        self.accounts = [gp.Account(self.config, "账号1", "seed-1", gp.TokenStore("", False))]
        self.pool = gp.AccountPool(self.config, self.accounts)
        self.client = gp.GLMClient(self.config, self.pool)
        self._saved = (gp.Handler.config, gp.Handler.client)
        gp.Handler.config = self.config
        gp.Handler.client = self.client
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), gp.Handler)
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        self.base = f"http://127.0.0.1:{self.port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        gp.Handler.config, gp.Handler.client = self._saved
        super().tearDown()

    def post_json(self, payload: dict, key: str | None = "secret"):
        headers = {"Content-Type": "application/json"}
        if key:
            headers["Authorization"] = f"Bearer {key}"
        req = urllib.request.Request(
            self.base + "/v1/chat/completions", data=json.dumps(payload).encode(), headers=headers
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def post_raw(self, payload: dict, key: str | None = "secret") -> str:
        headers = {"Content-Type": "application/json"}
        if key:
            headers["Authorization"] = f"Bearer {key}"
        req = urllib.request.Request(
            self.base + "/v1/chat/completions", data=json.dumps(payload).encode(), headers=headers
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            self.assertEqual(resp.status, 200)
            return resp.read().decode("utf-8")

    def test_health_has_no_auth(self):
        with urllib.request.urlopen(self.base + "/health", timeout=10) as resp:
            self.assertEqual(resp.status, 200)
            self.assertEqual(json.loads(resp.read())["status"], "ok")

    def test_models_requires_api_key(self):
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(self.base + "/v1/models", timeout=10)
        self.assertEqual(ctx.exception.code, 401)

        req = urllib.request.Request(
            self.base + "/v1/models", headers={"Authorization": "Bearer secret"}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        self.assertIn("glm-4", [item["id"] for item in data["data"]])

    def test_non_stream_completion(self):
        status, data = self.post_json(
            {"model": "glm-4", "messages": [BUSY_REQUEST]}
        )
        self.assertEqual(status, 200)
        self.assertEqual(data["object"], "chat.completion")
        self.assertEqual(data["choices"][0]["message"]["content"], "你好世界")
        self.assertEqual(data["choices"][0]["finish_reason"], "stop")
        # 上游实际模型通过 system_fingerprint 暴露出来（moe_53f = GLM-5.3 flash）
        self.assertEqual(data["system_fingerprint"], "moe_53f")

    def test_stream_completion_deltas(self):
        raw = self.post_raw({"model": "glm-4", "stream": True, "messages": [BUSY_REQUEST]})
        payloads = [
            json.loads(line[6:]) for line in raw.splitlines()
            if line.startswith("data: ") and line[6:].strip() != "[DONE]"
        ]
        # 内容按增量分帧发出（你好 / 世界）+ 收尾帧 —— 不再攒成一帧一次性给
        self.assertEqual(len(payloads), 3)
        self.assertTrue(all(item["object"] == "chat.completion.chunk" for item in payloads))

        first, second, last = payloads
        self.assertEqual(first["choices"][0]["delta"]["role"], "assistant")
        self.assertEqual([p["choices"][0]["delta"]["content"] for p in payloads[:-1]],
                         ["你好", "世界"])
        self.assertEqual(last["choices"][0]["delta"], {})
        self.assertEqual(last["choices"][0]["finish_reason"], "stop")
        self.assertTrue(raw.rstrip().endswith("data: [DONE]"))

    def test_stream_does_not_leak_platform_tool_call(self):
        """上游自己跑了平台工具（search）时，客户端只会看到普通正文。

        平台工具不在客户端声明的 tools 里、客户端也没有实现；回传 tool_calls 会让
        客户端去执行一个不存在的工具并卡在等结果。
        """
        self.fake.script = [("sse", sse_with_native_tool_call("search", "北京今天晴，21 度。"))]
        raw = self.post_raw({"model": "glm-4", "stream": True, "messages": [BUSY_REQUEST]})
        payloads = [
            json.loads(line[6:]) for line in raw.splitlines()
            if line.startswith("data: ") and line[6:].strip() != "[DONE]"
        ]
        self.assertFalse([p for p in payloads if p["choices"][0]["delta"].get("tool_calls")])
        self.assertEqual("".join(p["choices"][0]["delta"].get("content") or ""
                                 for p in payloads), "北京今天晴，21 度。")
        self.assertEqual(payloads[-1]["choices"][0]["finish_reason"], "stop")

    def test_platform_tool_call_in_tools_mode_stays_a_plain_answer(self):
        """带 tools 的请求也一样：平台工具不构成 tool_calls，直接当回答返回。"""
        self.fake.script = [("sse", sse_with_native_tool_call("search", "北京今天晴，21 度。"))]
        status, data = self.post_json({
            "model": "glm-4", "messages": [BUSY_REQUEST], "tools": [WEATHER_TOOL]
        })
        self.assertEqual(status, 200)
        self.assertEqual(data["choices"][0]["finish_reason"], "stop")
        self.assertIsNone(data["choices"][0]["message"].get("tool_calls"))
        self.assertEqual(data["choices"][0]["message"]["content"], "北京今天晴，21 度。")

    def _stream_content(self, raw: str) -> str:
        frames = [json.loads(line[6:]) for line in raw.splitlines()
                  if line.startswith("data: ") and line[6:].strip() != "[DONE]"]
        return "".join(f["choices"][0]["delta"].get("content") or "" for f in frames)

    def test_stream_survives_upstream_disconnect(self):
        """上游没推 finish 就把连接断了：已收到的内容仍要吐完并正常收尾。

        修前：累积器一直等「下一次确认」，客户端拿到一个空回答。
        """
        self.fake.script = [("sse", sse_without_finish("被掐断的回答"))]
        raw = self.post_raw({"model": "glm-4", "stream": True, "messages": [BUSY_REQUEST]})
        self.assertEqual(self._stream_content(raw), "被掐断的回答")
        self.assertTrue(raw.rstrip().endswith("data: [DONE]"))

    def test_incomplete_read_is_treated_as_end_of_stream(self):
        """上游读到一半抛 IncompleteRead：已到帧照常解析，不整条流报废。"""
        first = sse_text_event("前半段")
        body = first + sse_text_event("前半段后半段")
        self.fake.script = [("sse_cut", body, len(first.encode("utf-8")))]
        raw = self.post_raw({"model": "glm-4", "stream": True, "messages": [BUSY_REQUEST]})
        self.assertEqual(self._stream_content(raw), "前半段")
        self.assertTrue(raw.rstrip().endswith("data: [DONE]"))

    def test_stream_is_closed_when_handler_raises(self):
        """SSE 头已发出之后才抛异常：必须补收尾帧，否则客户端挂在无结束的流上。"""
        real_consume = gp.StreamAccumulator.consume

        def boom(self, event):
            raise RuntimeError("炸在流中间")

        gp.StreamAccumulator.consume = boom
        try:
            raw = self.post_raw({"model": "glm-4", "stream": True, "messages": [BUSY_REQUEST]})
        finally:
            gp.StreamAccumulator.consume = real_consume
        frames = [json.loads(line[6:]) for line in raw.splitlines()
                  if line.startswith("data: ") and line[6:].strip() != "[DONE]"]
        self.assertEqual(frames[-1]["choices"][0]["finish_reason"], "stop")
        self.assertTrue(raw.rstrip().endswith("data: [DONE]"))

    def test_bad_request_and_missing_messages(self):
        status, data = self.post_json({"model": "glm-4"})
        self.assertEqual(status, 400)
        self.assertIn("messages", data["error"]["message"])

        status, data = self.post_json({"messages": [BUSY_REQUEST]}, key="wrong")
        self.assertEqual(status, 401)
        self.assertEqual(data["error"]["type"], "authentication_error")

    def test_busy_maps_to_503_and_releases_slot(self):
        self.fake.script = [("busy", BUSY_PAYLOAD)] * (self.config.busy_retries + 1)
        status, data = self.post_json({"model": "glm-4", "messages": [BUSY_REQUEST]})
        self.assertEqual(status, 503)
        self.assertEqual(data["error"]["type"], "upstream_busy")
        self.assertEqual(self.fake.stream_calls, self.config.busy_retries + 1)
        self.assertTrue(self.accounts[0].try_acquire())  # 槽位已归还
        self.accounts[0].release()

    def test_upstream_business_error_maps_to_502(self):
        self.fake.script = [("json", {"code": 400, "message": "参数不合法"})]
        status, data = self.post_json({"model": "glm-4", "messages": [BUSY_REQUEST]})
        self.assertEqual(status, 502)
        self.assertEqual(data["error"]["type"], "upstream_error")

    def test_bom_prefixed_body_is_accepted(self):
        """Windows 下用记事本/PowerShell 存请求体常带 UTF-8 BOM，不能因此 400。"""
        body = codecs.BOM_UTF8 + json.dumps(
            {"model": "glm-4", "messages": [BUSY_REQUEST]}, ensure_ascii=False
        ).encode("utf-8")
        req = urllib.request.Request(
            self.base + "/v1/chat/completions", data=body,
            headers={"Content-Type": "application/json", "Authorization": "Bearer secret"},
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            status = resp.status
            data = json.loads(resp.read().decode("utf-8"))
        self.assertEqual(status, 200)
        self.assertEqual(data["choices"][0]["message"]["content"], "你好世界")

    def test_models_endpoint_path_aliases(self):
        """Cherry/Open WebUI 等对 /v1 的拼接不一致，三条路径都要能用。"""
        for path in ("/v1/models", "/models", "/v1/v1/models", "/v1/models/"):
            req = urllib.request.Request(
                self.base + path, headers={"Authorization": "Bearer secret"}
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                self.assertEqual(resp.status, 200, path)
                ids = [item["id"] for item in json.loads(resp.read())["data"]]
            self.assertIn("glm-4", ids, path)  # 默认清单里必须有 glm-4
            self.assertEqual(ids, self.config.models, path)

    def test_models_endpoint_follows_glm_models_env(self):
        self.config.models = ["glm-5.3", "my-private-model"]
        req = urllib.request.Request(
            self.base + "/v1/models", headers={"Authorization": "Bearer secret"}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            ids = [item["id"] for item in json.loads(resp.read())["data"]]
        self.assertEqual(ids, ["glm-5.3", "my-private-model"])

    def test_unknown_model_name_is_echoed_not_rejected(self):
        """model 只是标签：上游不吃这个参数，所以任何名字都该能跑（原样回显）。"""
        status, data = self.post_json(
            {"model": "glm-5.3-vip-whatever", "messages": [BUSY_REQUEST]}
        )
        self.assertEqual(status, 200)
        self.assertEqual(data["model"], "glm-5.3-vip-whatever")
        self.assertEqual(data["choices"][0]["message"]["content"], "你好世界")

    def test_chat_completion_path_aliases(self):
        for path in ("/v1/chat/completions", "/chat/completions"):
            req = urllib.request.Request(
                self.base + path,
                data=json.dumps({"model": "glm-4", "messages": [BUSY_REQUEST]}).encode(),
                headers={"Content-Type": "application/json", "Authorization": "Bearer secret"},
            )
            with urllib.request.urlopen(req, timeout=20) as resp:
                self.assertEqual(resp.status, 200, path)
                data = json.loads(resp.read())
            self.assertEqual(data["choices"][0]["message"]["content"], "你好世界", path)

    def test_tool_mode_off_ignores_tools(self):
        """显式关闭提示词工具调用时：请求体里不应出现 # TOOLS，响应也没有 tool_calls。"""
        self.config.prompt_tool_calling = False
        status, data = self.post_json(
            {"model": "glm-4", "messages": [BUSY_REQUEST], "tools": [WEATHER_TOOL]}
        )
        self.assertEqual(status, 200)
        self.assertNotIn("tool_calls", json.dumps(data, ensure_ascii=False))
        self.assertEqual(data["choices"][0]["finish_reason"], "stop")
        self.assertNotIn("# TOOLS", self.fake.bodies[-1])

    def test_tool_mode_is_on_by_default(self):
        """默认开启：客户端发了 tools 就该用（与 /v1/messages 端点行为一致）。"""
        with env_patch({}):
            self.assertTrue(gp.Config().prompt_tool_calling)
        self.fake.script = [("sse", sse_with_text(
            '{"tool_calls":[{"name":"get_weather","arguments":{"city":"北京"}}]}'
        ))]
        status, data = self.post_json(
            {"model": "glm-4", "messages": [BUSY_REQUEST], "tools": [WEATHER_TOOL]}
        )
        self.assertEqual(status, 200)
        self.assertEqual(data["choices"][0]["finish_reason"], "tool_calls")

    def test_tool_mode_returns_tool_calls(self):
        self.config.prompt_tool_calling = True
        self.fake.script = [("sse", sse_with_text(
            '{"tool_calls":[{"name":"get_weather","arguments":{"city":"北京"}}]}'
        ))]
        status, data = self.post_json(
            {"model": "glm-4", "messages": [BUSY_REQUEST], "tools": [WEATHER_TOOL]}
        )
        self.assertEqual(status, 200)
        choice = data["choices"][0]
        self.assertEqual(choice["finish_reason"], "tool_calls")
        self.assertIsNone(choice["message"]["content"])
        call = choice["message"]["tool_calls"][0]
        self.assertEqual(call["type"], "function")
        self.assertEqual(call["function"]["name"], "get_weather")
        self.assertEqual(json.loads(call["function"]["arguments"]), {"city": "北京"})
        # 工具协议确实注入了提示词，模型才知道能调什么
        self.assertIn("# TOOLS", json.loads(self.fake.bodies[-1])["messages"][0]["content"][0]["text"])

    def test_prompt_puts_protocol_after_history_with_real_example(self):
        """端到端：协议块在对话之后，且示例用的是客户端真实注册的工具名与必填参数。"""
        self.config.prompt_tool_calling = True
        tools = [{"type": "function", "function": {
            "name": "read_file", "description": "读本地文件",
            "parameters": {"type": "object", "properties": {"path": {"type": "string"}},
                           "required": ["path"]}}}]
        self.fake.script = [("sse", sse_with_text("这里不需要工具，直接回答。"))]
        status, _ = self.post_json(
            {"model": "glm-4", "messages": [BUSY_REQUEST], "tools": tools}
        )
        self.assertEqual(status, 200)
        prompt = json.loads(self.fake.bodies[-1])["messages"][0]["content"][0]["text"]
        # 用 index 而非 rindex：生成提示里也提到「# TOOLS」，那处不是协议块的起点
        head = prompt.index("# TOOLS")
        self.assertLess(prompt.rindex("# CONVERSATION"), head)
        self.assertLess(head, prompt.rindex(gp.TOOL_FRAME_TAIL))
        self.assertIn('read_file({"path":', prompt)
        self.assertTrue(prompt.endswith("Assistant: "))

    def test_tool_mode_with_40_tools_and_budget(self):
        """40 个胖 Schema + 小预算：定义被裁剪，但排在最后、参数没展开的工具仍要能被调用。"""
        self.config.prompt_tool_calling = True
        self.config.tools_prompt_max_chars = 4000
        tools = ToolsPromptBudgetTest.fat_tools(40)
        self.fake.script = [("sse", sse_with_text(
            '{"tool_calls":[{"name":"tool_39","arguments":{"query":"北京"}}]}'
        ))]
        status, data = self.post_json(
            {"model": "glm-4", "messages": [BUSY_REQUEST], "tools": tools}
        )
        self.assertEqual(status, 200)
        choice = data["choices"][0]
        self.assertEqual(choice["finish_reason"], "tool_calls")
        self.assertEqual(choice["message"]["tool_calls"][0]["function"]["name"], "tool_39")
        prompt = json.loads(self.fake.bodies[-1])["messages"][0]["content"][0]["text"]
        self.assertIn("未展开的工具（参数请勿猜测，猜了必失败）：", prompt)
        self.assertLess(prompt.count("参数 JSON Schema"), 40)

    def test_tool_mode_stream_returns_tool_calls(self):
        self.config.prompt_tool_calling = True
        self.fake.script = [("sse", sse_with_text(
            '{"tool_calls":[{"name":"get_weather","arguments":{"city":"上海"}}]}'
        ))]
        raw = self.post_raw(
            {"model": "glm-4", "stream": True, "messages": [BUSY_REQUEST], "tools": [WEATHER_TOOL]}
        )
        payloads = [
            json.loads(line[6:]) for line in raw.splitlines()
            if line.startswith("data: ") and line[6:].strip() != "[DONE]"
        ]
        self.assertEqual(payloads[0]["choices"][0]["delta"]["tool_calls"][0]["function"]["name"],
                         "get_weather")
        self.assertEqual(payloads[-1]["choices"][0]["finish_reason"], "tool_calls")
        self.assertTrue(raw.rstrip().endswith("data: [DONE]"))

    def _sse_growing_think(self, *steps: str, text: str = "") -> str:
        """造一段思维链逐帧变长、最后给正文的 SSE。

        每帧都是「累积快照」式的更长内容，增量分片式的写法见
        ``StreamAccumulatorTest.test_ordered_deltas_stream_out_immediately``。
        """
        events = [
            'data: {"conversation_id":"conv-1","parts":[{"logic_id":"p1","content":'
            '[{"type":"think","think":%s}]}],"status":"generating"}\n\n'
            % json.dumps(step, ensure_ascii=False)
            for step in steps
        ]
        if text:
            events.append(
                'data: {"conversation_id":"conv-1","parts":[{"logic_id":"p2","content":'
                '[{"type":"text","text":%s}]}],"status":"generating"}\n\n'
                % json.dumps(text, ensure_ascii=False)
            )
        events.append('data: {"conversation_id":"conv-1","parts":[],"status":"finish"}\n\n')
        return "".join(events)

    def _stream_frames(self, raw: str) -> list:
        """把 SSE 响应解析成 data 帧列表（去掉 [DONE]）。"""
        return [json.loads(line[6:]) for line in raw.splitlines()
                if line.startswith("data: ") and line[6:].strip() != "[DONE]"]

    def test_tool_mode_streams_reasoning_before_answer(self):
        """工具模式下思维链要先流出去，客户端才看得到「先思考后回答」的真实节奏。

        （修前：思维链和正文被同一刻吐出，客户端显示「已深度思考 用时 0.1 秒」。）
        """
        self.config.prompt_tool_calling = True
        self.fake.script = [("sse", self._sse_growing_think(
            "先想一下", "先想一下，分三步", text="答案如右"))]
        raw = self.post_raw({"model": "glm-4", "stream": True,
                             "messages": [BUSY_REQUEST], "tools": [WEATHER_TOOL]})
        deltas = [f["choices"][0]["delta"] for f in self._stream_frames(raw)]
        reasoning = [d["reasoning_content"] for d in deltas if "reasoning_content" in d]
        content = [d["content"] for d in deltas if d.get("content")]
        # 思维链分多段先到，正文最后一帧到齐
        self.assertGreater(len(reasoning), 1)
        self.assertEqual("".join(reasoning), "先想一下，分三步")
        self.assertEqual("".join(content), "答案如右")
        # 顺序：最后一个思维链帧必须早于第一个正文帧
        last_reason = max(i for i, d in enumerate(deltas) if "reasoning_content" in d)
        first_content = next(i for i, d in enumerate(deltas) if d.get("content"))
        self.assertLess(last_reason, first_content)
        # 第一帧正文带 role；结尾补全量的思维链（否则客户端会看到重复）
        self.assertEqual(deltas[first_content].get("role"), "assistant")
        self.assertEqual(len(reasoning), sum("reasoning_content" in d for d in deltas))
        self.assertTrue(raw.rstrip().endswith("data: [DONE]"))

    def test_tool_mode_stream_reasoning_then_tool_calls(self):
        """流式 + 工具调用：思维链先流，tool_calls 后到，finish_reason 仍是 tool_calls。"""
        self.config.prompt_tool_calling = True
        self.fake.script = [("sse", sse_with_think_and_text(
            "先查天气",
            '{"tool_calls":[{"name":"get_weather","arguments":{"city":"上海"}}]}'
        ))]
        raw = self.post_raw({"model": "glm-4", "stream": True,
                             "messages": [BUSY_REQUEST], "tools": [WEATHER_TOOL]})
        frames = self._stream_frames(raw)
        deltas = [f["choices"][0]["delta"] for f in frames]
        self.assertEqual(deltas[0].get("reasoning_content"), "先查天气")
        call_frame = next(d for d in deltas if d.get("tool_calls"))
        self.assertEqual(call_frame["tool_calls"][0]["function"]["name"], "get_weather")
        self.assertEqual(json.loads(call_frame["tool_calls"][0]["function"]["arguments"]),
                         {"city": "上海"})
        self.assertEqual(frames[-1]["choices"][0]["finish_reason"], "tool_calls")

    def test_tool_mode_non_stream_unchanged(self):
        """非流式 + tools：仍然是一个完整 JSON，reasoning_content 原样带回。"""
        self.config.prompt_tool_calling = True
        self.fake.script = [("sse", sse_with_think_and_text("想一下", "北京今天多云"))]
        status, data = self.post_json(
            {"model": "glm-4", "messages": [BUSY_REQUEST], "tools": [WEATHER_TOOL]}
        )
        self.assertEqual(status, 200)
        message = data["choices"][0]["message"]
        self.assertEqual(message["content"], "北京今天多云")
        self.assertEqual(message["reasoning_content"], "想一下")

    def test_tool_mode_stream_continue_still_streams_reasoning(self):
        """思考泄漏续问时，两轮思维链都流式发给客户端，碎碎念不会混进正文。"""
        self.config.prompt_tool_calling = True
        self.config.tool_continue_tries = 1
        self.fake.script = [
            ("sse", sse_with_think_and_text("换个工具再搜一次", ThinkLeakTest.LEAKED)),
            ("sse", sse_with_think_and_text("直接给结论", "没查到今日客流，建议看高德地图。")),
        ]
        raw = self.post_raw({"model": "glm-4", "stream": True,
                             "messages": [{"role": "user", "content": "北京今天景区人多吗"}],
                             "tools": [WEATHER_TOOL]})
        deltas = [f["choices"][0]["delta"] for f in self._stream_frames(raw)]
        reasoning = "".join(d["reasoning_content"] for d in deltas if "reasoning_content" in d)
        content = "".join(d["content"] for d in deltas if d.get("content"))
        # 两轮思维链都在，续问后的正文是结论，碎碎念没有进正文
        self.assertIn("换个工具再搜一次", reasoning)
        self.assertIn("直接给结论", reasoning)
        self.assertEqual(content, "没查到今日客流，建议看高德地图。")
        self.assertNotIn("unhelpful", content)
        self.assertEqual(self.fake.stream_calls, 2)

    def test_tool_mode_translates_textual_calls(self):
        """真实失败场景：模型用 JS 风格写调用（被 Cherry 自带说明带跑），也要能变成 tool_calls。"""
        self.config.prompt_tool_calling = True
        cherry_tools = [
            {"type": "function", "function": {
                "name": "mcp__CherryHub__invoke",
                "description": "调用 MCP 工具",
                "parameters": {"type": "object",
                               "properties": {"name": {"type": "string"},
                                              "params": {"type": "object"}}}}},
        ]
        self.fake.script = [("sse", sse_with_text(
            "我来调用工具：\n\n```javascript\n"
            "invoke({ name: \"writeFile\", params: { path: './a.txt', content: '你好' } })\n"
            "```\n（模拟结果）"
        ))]
        status, data = self.post_json(
            {"model": "glm-5.3", "messages": [BUSY_REQUEST], "tools": cherry_tools}
        )
        self.assertEqual(status, 200)
        choice = data["choices"][0]
        self.assertEqual(choice["finish_reason"], "tool_calls")
        call = choice["message"]["tool_calls"][0]
        self.assertEqual(call["function"]["name"], "mcp__CherryHub__invoke")
        self.assertEqual(
            json.loads(call["function"]["arguments"]),
            {"name": "writeFile", "params": {"path": "./a.txt", "content": "你好"}},
        )

    def test_tool_mode_rescues_windows_path_arguments(self):
        """真机高频畸形：模型把 Windows 路径的反斜杠只写一个 → 整段 JSON 非法 →
        以前这条工具链直接作废（日志里看不出原因）。现在要救回参数完整的调用。"""
        self.config.prompt_tool_calling = True
        path = "C:\\Users\\silzh\\docs\\STATE.md"
        self.fake.script = [("sse", sse_with_text(
            '{"tool_calls":[{"name":"read_file","arguments":{"path":"' + path + '"}}]}'))]
        status, data = self.post_json({
            "model": "glm-5.3", "messages": [BUSY_REQUEST],
            "tools": [{"type": "function", "function": {
                "name": "read_file", "description": "读取文件",
                "parameters": {"type": "object",
                               "properties": {"path": {"type": "string"}},
                               "required": ["path"]}}}],
        })
        self.assertEqual(status, 200)
        choice = data["choices"][0]
        self.assertEqual(choice["finish_reason"], "tool_calls")
        self.assertEqual(
            json.loads(choice["message"]["tool_calls"][0]["function"]["arguments"]),
            {"path": path})

    def test_tool_mode_falls_back_to_plain_answer(self):
        """模型没调工具（或工具名不认识）时，必须退化成普通回答，不能瞎报 tool_calls。"""
        self.config.prompt_tool_calling = True
        self.fake.script = [("sse", sse_with_text("北京今天晴，25 度。"))]
        status, data = self.post_json(
            {"model": "glm-4", "messages": [BUSY_REQUEST], "tools": [WEATHER_TOOL]}
        )
        self.assertEqual(status, 200)
        self.assertEqual(data["choices"][0]["message"]["content"], "北京今天晴，25 度。")
        self.assertEqual(data["choices"][0]["finish_reason"], "stop")

        self.fake.script = [("sse", sse_with_text('{"tool_calls":[{"name":"not_my_tool","arguments":{}}]}'))]
        status, data = self.post_json(
            {"model": "glm-4", "messages": [BUSY_REQUEST], "tools": [WEATHER_TOOL]}
        )
        self.assertEqual(status, 200)
        self.assertNotIn("tool_calls", data["choices"][0]["message"])
        self.assertEqual(data["choices"][0]["finish_reason"], "stop")

    def test_tool_result_message_reaches_upstream(self):
        """客户端把工具结果（role:"tool"）发回来时，必须出现在给上游的提示词里。"""
        self.config.prompt_tool_calling = True
        status, _ = self.post_json({
            "model": "glm-4",
            "messages": [
                {"role": "user", "content": "北京天气"},
                {"role": "assistant", "content": None, "tool_calls": [
                    {"id": "c1", "type": "function",
                     "function": {"name": "get_weather", "arguments": '{"city":"北京"}'}},
                ]},
                {"role": "tool", "tool_call_id": "c1", "content": "晴 25 度"},
            ],
            "tools": [WEATHER_TOOL],
        })
        self.assertEqual(status, 200)
        prompt = json.loads(self.fake.bodies[-1])["messages"][0]["content"][0]["text"]
        self.assertIn("Tool(get_weather): 晴 25 度", prompt)

    def test_model_assistant_map_is_used(self):
        """配了 GLM_MODEL_ASSISTANT_MAP 时，不同模型名要走不同 assistant_id（含删会话）。"""
        self.config.model_assistant_map = {"glm-5.3": "aaaaaaaabbbbbbbbcccccccc"}
        status, _ = self.post_json({"model": "glm-5.3", "messages": [BUSY_REQUEST]})
        self.assertEqual(status, 200)
        body = json.loads(self.fake.bodies[-1])
        self.assertEqual(body["assistant_id"], "aaaaaaaabbbbbbbbcccccccc")

        # 未配置映射的模型名 → 仍走默认 assistant_id
        status, _ = self.post_json({"model": "glm-4", "messages": [BUSY_REQUEST]})
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(self.fake.bodies[-1])["assistant_id"],
                         gp.DEFAULT_ASSISTANT_ID)

    def test_networking_flag_is_forwarded(self):
        status, _ = self.post_json(
            {"model": "glm-4", "messages": [BUSY_REQUEST], "glm": {"networking": True}}
        )
        self.assertEqual(status, 200)
        body = json.loads(self.fake.bodies[-1])
        self.assertTrue(body["meta_data"]["is_networking"])
        self.assertEqual(body["meta_data"]["platform"], "pc")
        self.assertTrue(body["messages"][0]["content"][0]["text"].endswith("Assistant: "))

    def test_selected_model_is_forwarded(self):
        """客户端传的 model 名要写进 meta_data.selected_model（网页用它标明选中模型）。"""
        status, _ = self.post_json({"model": "glm-5.3", "messages": [BUSY_REQUEST]})
        self.assertEqual(status, 200)
        body = json.loads(self.fake.bodies[-1])
        self.assertEqual(body["meta_data"]["selected_model"], "glm-5.3")
        # assistant_id 仍是默认助手：网页切换模型时该字段不变
        self.assertEqual(body["assistant_id"], gp.DEFAULT_ASSISTANT_ID)

    def test_selected_model_can_be_disabled(self):
        self.config.selected_model = False
        status, _ = self.post_json({"model": "glm-5.3", "messages": [BUSY_REQUEST]})
        self.assertEqual(status, 200)
        self.assertNotIn("selected_model", json.loads(self.fake.bodies[-1])["meta_data"])

    def test_thinking_mode_fields_are_forwarded(self):
        self.config.chat_mode = "deep_thinking"
        self.config.reasoning_effort = "max"
        status, _ = self.post_json({"model": "glm-5.3", "messages": [BUSY_REQUEST]})
        self.assertEqual(status, 200)
        meta = json.loads(self.fake.bodies[-1])["meta_data"]
        self.assertEqual(meta["chat_mode"], "deep_thinking")
        self.assertEqual(meta["reasoning_effort"], "max")

    def test_deep_thinking_per_request_is_forwarded(self):
        """按请求开深度思考：三种写法都要让上游收到 chat_mode + reasoning_effort。"""
        for extra in ({"glm": {"deep_thinking": True}},
                      {"glm_deep_thinking": True},
                      {"deep_thinking": True}):
            status, _ = self.post_json(
                dict({"model": "glm-5.3", "messages": [BUSY_REQUEST]}, **extra)
            )
            self.assertEqual(status, 200, extra)
            meta = json.loads(self.fake.bodies[-1])["meta_data"]
            self.assertEqual(meta["chat_mode"], "deep_thinking", extra)
            self.assertEqual(meta["reasoning_effort"], "max", extra)

    def test_deep_thinking_default_off_and_overridable(self):
        """默认不开（不发 reasoning_effort）；全局默认开时单次可关掉。"""
        status, _ = self.post_json({"model": "glm-5.3", "messages": [BUSY_REQUEST]})
        self.assertEqual(status, 200)
        meta = json.loads(self.fake.bodies[-1])["meta_data"]
        self.assertEqual(meta["chat_mode"], "")
        self.assertNotIn("reasoning_effort", meta)

        self.config.deep_thinking = True   # 等价 GLM_DEEP_THINKING=true
        status, _ = self.post_json({"model": "glm-5.3", "messages": [BUSY_REQUEST]})
        self.assertEqual(status, 200)
        meta = json.loads(self.fake.bodies[-1])["meta_data"]
        self.assertEqual(meta["chat_mode"], "deep_thinking")
        self.assertEqual(meta["reasoning_effort"], "max")

        # 全局开着也能单次关掉
        status, _ = self.post_json({"model": "glm-5.3", "messages": [BUSY_REQUEST],
                                    "glm": {"deep_thinking": False}})
        self.assertEqual(status, 200)
        meta = json.loads(self.fake.bodies[-1])["meta_data"]
        self.assertEqual(meta["chat_mode"], "")
        self.assertNotIn("reasoning_effort", meta)

    def test_chat_mode_alone_also_gets_default_reasoning_effort(self):
        """老写法只配 GLM_CHAT_MODE=deep_thinking 时也要补 max（网页版两个字段成对）。"""
        self.config.chat_mode = "deep_thinking"
        status, _ = self.post_json({"model": "glm-5.3", "messages": [BUSY_REQUEST]})
        self.assertEqual(status, 200)
        meta = json.loads(self.fake.bodies[-1])["meta_data"]
        self.assertEqual(meta["chat_mode"], "deep_thinking")
        self.assertEqual(meta["reasoning_effort"], "max")

    def test_non_deep_chat_mode_does_not_get_reasoning_effort(self):
        """配了别的 chat_mode（如 agent）时不乱补 reasoning_effort。"""
        self.config.chat_mode = "agent"
        status, _ = self.post_json({"model": "glm-5.3", "messages": [BUSY_REQUEST]})
        self.assertEqual(status, 200)
        meta = json.loads(self.fake.bodies[-1])["meta_data"]
        self.assertEqual(meta["chat_mode"], "agent")
        self.assertNotIn("reasoning_effort", meta)

    def test_deep_thinking_respects_configured_reasoning_effort(self):
        """.env 显式配了 reasoning_effort 时，按请求开深度思考也不覆盖它。"""
        self.config.reasoning_effort = "min"
        status, _ = self.post_json({"model": "glm-5.3", "messages": [BUSY_REQUEST],
                                    "glm": {"deep_thinking": True}})
        self.assertEqual(status, 200)
        meta = json.loads(self.fake.bodies[-1])["meta_data"]
        self.assertEqual(meta["chat_mode"], "deep_thinking")
        self.assertEqual(meta["reasoning_effort"], "min")

    def test_deep_thinking_with_tools_keeps_flag_on_continue(self):
        """工具模式续问那一轮也要带深度思考，否则第二次会悄悄退回普通档。"""
        self.config.tool_continue_tries = 1
        self.fake.script = [
            ("sse", sse_with_text(ThinkLeakTest.LEAKED)),
            ("sse", sse_with_text("没查到今天的景区客流，建议看高德地图实时路况。")),
        ]
        status, data = self.post_json({
            "model": "glm-5.3",
            "messages": [{"role": "user", "content": "北京今天景区人多吗"}],
            "tools": [WEATHER_TOOL],
            "glm": {"deep_thinking": True},
        })
        self.assertEqual(status, 200)
        self.assertIn("高德地图", data["choices"][0]["message"]["content"])
        # 首发 + 1 次续问，两次上游请求都必须带深度思考
        chat_bodies = [b for b in self.fake.bodies if "assistant_id" in b]
        self.assertEqual(len(chat_bodies), 2)
        for body in chat_bodies:
            meta = json.loads(body)["meta_data"]
            self.assertEqual(meta["chat_mode"], "deep_thinking")
            self.assertEqual(meta["reasoning_effort"], "max")


# ─────────────────────────── Anthropic /v1/messages ───────────────────────────
ANTHROPIC_TOOL = {
    "name": "get_weather",
    "description": "查询指定城市的天气",
    "input_schema": {
        "type": "object",
        "properties": {"city": {"type": "string"}},
        "required": ["city"],
    },
}


def sse_with_think_and_text(thinking: str, text: str) -> str:
    """造一段上游同时输出思维链与正文的 SSE（两者各占一个 logic_id）。"""
    return (
        'data: {"conversation_id":"conv-1","parts":[{"logic_id":"p1","content":'
        '[{"type":"think","think":%s}]}],"status":"generating"}\n\n'
        'data: {"conversation_id":"conv-1","parts":[{"logic_id":"p2","content":'
        '[{"type":"text","text":%s}]}],"status":"finish"}\n\n'
    ) % (json.dumps(thinking, ensure_ascii=False), json.dumps(text, ensure_ascii=False))


class AnthropicEndToEndTest(FakeUpstreamCase):
    """真 HTTP 端到端验证 Anthropic Messages API（/v1/messages）。"""

    def setUp(self) -> None:
        super().setUp()
        self.config = make_config(server_api_keys=["secret"], networking=False)
        self.accounts = [gp.Account(self.config, "账号1", "seed-1", gp.TokenStore("", False))]
        self.pool = gp.AccountPool(self.config, self.accounts)
        self.client = gp.GLMClient(self.config, self.pool)
        self._saved = (gp.Handler.config, gp.Handler.client)
        gp.Handler.config = self.config
        gp.Handler.client = self.client
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), gp.Handler)
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        self.base = f"http://127.0.0.1:{self.port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        gp.Handler.config, gp.Handler.client = self._saved
        super().tearDown()

    def post(self, payload: dict, path: str = "/v1/messages", key: str | None = "secret",
             key_header: str = "Authorization"):
        headers = {"Content-Type": "application/json"}
        if key and key_header == "Authorization":
            headers["Authorization"] = f"Bearer {key}"
        elif key:
            headers[key_header] = key
        req = urllib.request.Request(
            self.base + path, data=json.dumps(payload).encode(), headers=headers
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def post_stream(self, payload: dict, path: str = "/v1/messages") -> list:
        req = urllib.request.Request(
            self.base + path, data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json", "Authorization": "Bearer secret"},
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8")
        events = []
        for block in raw.split("\n\n"):
            lines = block.strip().splitlines()
            if not lines or not lines[0].startswith("event:"):
                continue
            events.append((lines[0][6:].strip(), json.loads(lines[1][5:].strip())))
        return events

    def test_non_stream_message(self):
        status, data = self.post({
            "model": "glm-4", "max_tokens": 100,
            "messages": [{"role": "user", "content": "你好"}],
        })
        self.assertEqual(status, 200)
        self.assertEqual(data["type"], "message")
        self.assertEqual(data["role"], "assistant")
        self.assertEqual(data["content"], [{"type": "text", "text": "你好世界"}])
        self.assertEqual(data["stop_reason"], "end_turn")
        self.assertTrue(data["id"].startswith("msg_"))
        self.assertGreater(data["usage"]["input_tokens"], 0)
        self.assertGreater(data["usage"]["output_tokens"], 0)

    def test_system_prompt_reaches_upstream(self):
        status, _ = self.post({
            "model": "glm-4", "system": "你是猫娘",
            "messages": [{"role": "user", "content": "你好"}],
        })
        self.assertEqual(status, 200)
        prompt = json.loads(self.fake.bodies[-1])["messages"][0]["content"][0]["text"]
        self.assertIn("你是猫娘", prompt)
        self.assertIn("User: 你好", prompt)

    def test_tools_protocol_block_follows_conversation(self):
        """Anthropic 面共用同一条渲染路径：协议块也要排在对话之后、示例用真工具名。"""
        status, _ = self.post({
            "model": "glm-4", "system": "你是助手",
            "messages": [{"role": "user", "content": "北京天气"}],
            "tools": [{"name": "get_weather", "description": "查天气",
                       "input_schema": {"type": "object",
                                         "properties": {"city": {"type": "string"}},
                                         "required": ["city"]}}],
        })
        self.assertEqual(status, 200)
        prompt = json.loads(self.fake.bodies[-1])["messages"][0]["content"][0]["text"]
        head = prompt.index("# TOOLS")   # index 而非 rindex：生成提示里也提到 # TOOLS
        self.assertLess(prompt.rindex("# CONVERSATION"), head)
        self.assertLess(head, prompt.rindex(gp.TOOL_FRAME_TAIL))
        self.assertIn('get_weather({"city": "北京"})', prompt)

    def test_stream_event_sequence(self):
        events = self.post_stream({
            "model": "glm-4", "stream": True,
            "messages": [{"role": "user", "content": "你好"}],
        })
        names = [name for name, _ in events]
        self.assertEqual(names[0], "message_start")
        self.assertEqual(names[1], "ping")
        self.assertIn("content_block_start", names)
        self.assertIn("content_block_delta", names)
        self.assertIn("content_block_stop", names)
        self.assertEqual(names[-2], "message_delta")
        self.assertEqual(names[-1], "message_stop")

        started = next(data for name, data in events if name == "message_start")
        self.assertTrue(started["message"]["id"].startswith("msg_"))
        text_deltas = [
            data["delta"]["text"] for name, data in events
            if name == "content_block_delta" and data["delta"]["type"] == "text_delta"
        ]
        self.assertEqual("".join(text_deltas), "你好世界")
        final = next(data for name, data in events if name == "message_delta")
        self.assertEqual(final["delta"]["stop_reason"], "end_turn")

    def test_thinking_block_is_emitted(self):
        self.fake.script = [("sse", sse_with_think_and_text("让我想想…", "答案是 42"))]
        status, data = self.post({
            "model": "glm-4", "thinking": {"type": "enabled", "budget_tokens": 1000},
            "messages": [{"role": "user", "content": "6*7=?"}],
        })
        self.assertEqual(status, 200)
        self.assertEqual(data["content"][0]["type"], "thinking")
        self.assertEqual(data["content"][0]["thinking"], "让我想想…")
        self.assertIn("signature", data["content"][0])
        self.assertEqual(data["content"][1], {"type": "text", "text": "答案是 42"})

    def test_thinking_can_be_disabled_by_env(self):
        self.fake.script = [("sse", sse_with_think_and_text("让我想想…", "答案是 42"))]
        os.environ["ANTHROPIC_EMIT_THINKING"] = "false"
        try:
            status, data = self.post({
                "model": "glm-4", "thinking": {"type": "enabled", "budget_tokens": 1000},
                "messages": [{"role": "user", "content": "6*7=?"}],
            })
        finally:
            del os.environ["ANTHROPIC_EMIT_THINKING"]
        self.assertEqual(status, 200)
        self.assertEqual(data["content"], [{"type": "text", "text": "答案是 42"}])

    def test_thinking_param_enables_upstream_deep_thinking(self):
        """Anthropic 客户端发 thinking 参数 = 要上游深度思考（两个 meta_data 都要带）。"""
        self.fake.script = [("sse", sse_with_think_and_text("让我想想…", "答案是 42"))]
        status, data = self.post({
            "model": "glm-4", "thinking": {"type": "enabled", "budget_tokens": 1000},
            "messages": [{"role": "user", "content": "6*7=?"}],
        })
        self.assertEqual(status, 200)
        meta = json.loads(self.fake.bodies[-1])["meta_data"]
        self.assertEqual(meta["chat_mode"], "deep_thinking")
        self.assertEqual(meta["reasoning_effort"], "max")
        # thinking block 仍然照常输出
        self.assertEqual(data["content"][0]["type"], "thinking")

    def test_emit_thinking_off_does_not_request_deep_thinking(self):
        """ANTHROPIC_EMIT_THINKING=false 时既不输出思维链，也不请求上游深度思考。"""
        self.fake.script = [("sse", sse_with_think_and_text("让我想想…", "答案是 42"))]
        os.environ["ANTHROPIC_EMIT_THINKING"] = "false"
        try:
            status, _ = self.post({
                "model": "glm-4", "thinking": {"type": "enabled", "budget_tokens": 1000},
                "messages": [{"role": "user", "content": "6*7=?"}],
            })
        finally:
            del os.environ["ANTHROPIC_EMIT_THINKING"]
        self.assertEqual(status, 200)
        meta = json.loads(self.fake.bodies[-1])["meta_data"]
        self.assertEqual(meta["chat_mode"], "")

    def test_messages_accepts_glm_deep_thinking(self):
        """/v1/messages 也认 OpenAI 侧的 glm.deep_thinking 写法。"""
        status, _ = self.post({
            "model": "glm-4", "max_tokens": 100,
            "messages": [{"role": "user", "content": "你好"}],
            "glm": {"deep_thinking": True},
        })
        self.assertEqual(status, 200)
        meta = json.loads(self.fake.bodies[-1])["meta_data"]
        self.assertEqual(meta["chat_mode"], "deep_thinking")
        self.assertEqual(meta["reasoning_effort"], "max")

    def test_no_thinking_param_means_no_deep_thinking(self):
        """不带 thinking / glm.deep_thinking 时不碰上游思考桥位（保持默认行为）。"""
        status, _ = self.post({
            "model": "glm-4", "max_tokens": 100,
            "messages": [{"role": "user", "content": "你好"}],
        })
        self.assertEqual(status, 200)
        meta = json.loads(self.fake.bodies[-1])["meta_data"]
        self.assertEqual(meta["chat_mode"], "")
        self.assertNotIn("reasoning_effort", meta)

    def test_textual_tool_call_is_translated(self):
        self.fake.script = [("sse", sse_with_text(
            "我来调用工具：invoke({ name: \"writeFile\", params: { path: './a.txt' } })"
        ))]
        status, data = self.post({
            "model": "glm-4",
            "messages": [{"role": "user", "content": "写文件"}],
            "tools": [{"name": "mcp__CherryHub__invoke", "description": "MCP",
                       "input_schema": {"type": "object"}}],
        })
        self.assertEqual(status, 200)
        block = data["content"][0]
        self.assertEqual(block["type"], "tool_use")
        self.assertEqual(block["name"], "mcp__CherryHub__invoke")
        self.assertEqual(block["input"], {"name": "writeFile", "params": {"path": "./a.txt"}})
        self.assertEqual(data["stop_reason"], "tool_use")

    def test_tools_return_tool_use_block(self):
        self.fake.script = [("sse", sse_with_text(
            '{"tool_calls":[{"name":"get_weather","arguments":{"city":"北京"}}]}'
        ))]
        status, data = self.post({
            "model": "glm-4",
            "messages": [{"role": "user", "content": "北京天气"}],
            "tools": [ANTHROPIC_TOOL],
        })
        self.assertEqual(status, 200)
        block = data["content"][0]
        self.assertEqual(block["type"], "tool_use")
        self.assertTrue(block["id"].startswith("toolu_"))
        self.assertEqual(block["name"], "get_weather")
        self.assertEqual(block["input"], {"city": "北京"})
        self.assertEqual(data["stop_reason"], "tool_use")
        # 工具协议注入提示词，且 input_schema 被转成 parameters
        prompt = json.loads(self.fake.bodies[-1])["messages"][0]["content"][0]["text"]
        self.assertIn("# TOOLS", prompt)
        self.assertIn("get_weather", prompt)

    def test_tools_stream_emits_tool_use(self):
        self.fake.script = [("sse", sse_with_text(
            '{"tool_calls":[{"name":"get_weather","arguments":{"city":"上海"}}]}'
        ))]
        events = self.post_stream({
            "model": "glm-4", "stream": True,
            "messages": [{"role": "user", "content": "上海天气"}],
            "tools": [ANTHROPIC_TOOL],
        })
        names = [name for name, _ in events]
        self.assertEqual(names[-1], "message_stop")
        start = next(data for name, data in events
                     if name == "content_block_start" and data["content_block"]["type"] == "tool_use")
        self.assertEqual(start["content_block"]["name"], "get_weather")
        json_deltas = [
            data["delta"]["partial_json"] for name, data in events
            if name == "content_block_delta" and data["delta"]["type"] == "input_json_delta"
        ]
        self.assertEqual(json.loads("".join(json_deltas)), {"city": "上海"})

    def test_tool_result_is_flattened_for_upstream(self):
        self.fake.script = [("sse", sse_with_text("北京今天晴，25 度。"))]
        status, data = self.post({
            "model": "glm-4",
            "messages": [
                {"role": "user", "content": "北京天气"},
                {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "toolu_1", "name": "get_weather",
                     "input": {"city": "北京"}},
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "toolu_1",
                     "content": [{"type": "text", "text": "晴 25 度"}]},
                ]},
            ],
            "tools": [ANTHROPIC_TOOL],
        })
        self.assertEqual(status, 200)
        prompt = json.loads(self.fake.bodies[-1])["messages"][0]["content"][0]["text"]
        self.assertIn("get_weather({", prompt)
        self.assertIn("Tool(get_weather): 晴 25 度", prompt)
        self.assertEqual(data["content"], [{"type": "text", "text": "北京今天晴，25 度。"}])

    def test_count_tokens(self):
        status, data = self.post({
            "model": "glm-4",
            "messages": [{"role": "user", "content": "你好，请介绍一下你自己"}],
        }, path="/v1/messages/count_tokens")
        self.assertEqual(status, 200)
        self.assertGreater(data["input_tokens"], 0)

    def test_x_api_key_header_is_accepted(self):
        status, data = self.post(
            {"model": "glm-4", "messages": [{"role": "user", "content": "你好"}]},
            key="secret", key_header="x-api-key",
        )
        self.assertEqual(status, 200)
        self.assertEqual(data["content"][0]["text"], "你好世界")

    def test_wrong_key_returns_anthropic_error(self):
        status, data = self.post(
            {"model": "glm-4", "messages": [{"role": "user", "content": "你好"}]}, key="wrong"
        )
        self.assertEqual(status, 401)
        self.assertEqual(data["type"], "error")
        self.assertEqual(data["error"]["type"], "authentication_error")

    def test_missing_messages_returns_400(self):
        status, data = self.post({"model": "glm-4"})
        self.assertEqual(status, 400)
        self.assertEqual(data["error"]["type"], "invalid_request_error")
        self.assertIn("messages", data["error"]["message"])

    def test_busy_maps_to_overloaded_error(self):
        self.fake.script = [("busy", BUSY_PAYLOAD)] * (self.config.busy_retries + 1)
        status, data = self.post({
            "model": "glm-4", "messages": [{"role": "user", "content": "你好"}],
        })
        self.assertEqual(status, 503)
        self.assertEqual(data["error"]["type"], "overloaded_error")
        self.assertTrue(self.accounts[0].try_acquire())
        self.accounts[0].release()

    def test_path_aliases(self):
        for path in ("/v1/messages", "/messages", "/v1/v1/messages"):
            status, data = self.post(
                {"model": "glm-4", "messages": [{"role": "user", "content": "你好"}]}, path=path
            )
            self.assertEqual(status, 200, path)
            self.assertEqual(data["content"][0]["text"], "你好世界", path)

    def test_stop_sequence_truncates_text(self):
        self.fake.script = [("sse", sse_with_text("第一段\nSTOP\n第二段"))]
        status, data = self.post({
            "model": "glm-4",
            "messages": [{"role": "user", "content": "你好"}],
            "stop_sequences": ["STOP"],
        })
        self.assertEqual(status, 200)
        self.assertEqual(data["content"], [{"type": "text", "text": "第一段\n"}])
        self.assertEqual(data["stop_reason"], "stop_sequence")
        self.assertEqual(data["stop_sequence"], "STOP")

    def test_no_stop_sequence_hit_keeps_end_turn(self):
        self.fake.script = [("sse", sse_with_text("你好世界"))]
        status, data = self.post({
            "model": "glm-4",
            "messages": [{"role": "user", "content": "你好"}],
            "stop_sequences": ["不存在的串"],
        })
        self.assertEqual(status, 200)
        self.assertEqual(data["stop_reason"], "end_turn")
        self.assertIsNone(data["stop_sequence"])
        self.assertEqual(data["content"][0]["text"], "你好世界")


class TruncatedJsonRescueTest(unittest.TestCase):
    """分片被截断时的 JSON 抢救。

    实测故障（22:24:33）：模型正确输出了两个并行 tool_calls，但因上游
    「分片乱序/改写」导致累积器拿到的文本尾部 `}]}` 缺失，解析失败 →
    明明是工具调用却被当成普通回答返回，用户看到裸 JSON。
    """

    FULL = ('{"tool_calls":[{"name":"mcp__CherryHub__invoke","arguments":'
            '{"name":"tavily","params":{"query":"北京 国庆 假期第四天 10月4日 '
            '公园景区 接待游客 万人次","search_depth":"advanced","max_results":10}}},'
            '{"name":"mcp__CherryHub__invoke","arguments":'
            '{"name":"tavily","params":{"query":"北京 昨天 各景区 游客量",'
            '"search_depth":"advanced","max_results":10}}}]}')

    TOOLS = {"mcp__CherryHub__invoke"}

    def test_missing_trailing_closers_is_rescued(self):
        """末尾缺 `}]}`（切在结构边界）→ 补齐后应救回两个调用。"""
        calls = gp.parse_tool_calls(self.FULL[:-3], self.TOOLS)
        self.assertIsNotNone(calls)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0]["function"]["name"], "mcp__CherryHub__invoke")

    def test_rescued_call_keeps_arguments_intact(self):
        """抢救后参数不能被腰斩 —— query 必须完整。"""
        calls = gp.parse_tool_calls(self.FULL[:-3], self.TOOLS)
        args = json.loads(calls[0]["function"]["arguments"])
        self.assertEqual(args["params"]["query"],
                         "北京 国庆 假期第四天 10月4日 公园景区 接待游客 万人次")
        self.assertEqual(args["params"]["search_depth"], "advanced")

    def test_cut_before_second_call_rescues_one(self):
        """切在第二个调用之前 → 至少救回第一个。"""
        cut = self.FULL[:self.FULL.rfind('},{"name"')]
        calls = gp.parse_tool_calls(cut, self.TOOLS)
        self.assertIsNotNone(calls)
        self.assertEqual(len(calls), 1)

    def test_no_rescue_when_cut_inside_string(self):
        """切在字符串内部（query 只写一半）→ 不抢救，绝不给残缺参数。"""
        cut = self.FULL[:self.FULL.find('"query":"北京 昨天') + 12]
        self.assertIsNone(gp.parse_tool_calls(cut, self.TOOLS))

    def test_no_rescue_when_key_has_no_value(self):
        """`..."name":"get_weather"` 这种键有值无的残缺 → 不抢救。"""
        self.assertIsNone(gp.parse_tool_calls(
            '{"tool_calls":[{"name":"get_weather"',
            {"mcp__CherryHub__invoke"}))

    def test_no_rescue_when_arguments_would_be_empty(self):
        """补齐后 arguments 会变成空对象的，说明参数被整个丢掉 → 不救。"""
        self.assertIsNone(gp.parse_tool_calls(
            '{"tool_calls":[{"name":"mcp__CherryHub__invoke","arguments":',
            self.TOOLS))

    def test_complete_json_still_works(self):
        """基准：完整 JSON 解析正常。"""
        calls = gp.parse_tool_calls(self.FULL, self.TOOLS)
        self.assertEqual(len(calls), 2)


class UnescapedQuoteJsonTest(unittest.TestCase):
    """参数里写代码、内层双引号没转义时的 JSON 修复。

    实测故障：Cherry Studio 的 ``mcp__CherryHub__exec``，模型把 JS 代码塞进 ``code``，
    代码里的 ``"tavilyMcpTavilySearch"`` 这类引号全是裸的 —— 整段 JSON 非法（括号倒是配平的，
    截断抢救帮不上），于是本该执行的工具调用被当成普通回答，用户看到一坨裸 JSON。
    """

    CODE = (
        "const [a, b, c] = await parallel(\n"
        '  mcp.callTool("tavilyMcpTavilySearch", { query: "北京 景区 今日 客流", '
        'time_range: "day", max_results: 8, search_depth: "advanced" }),\n'
        '  mcp.callTool("tavilyMcpTavilySearch", { query: "北京 热门景点 客流 故宫", '
        'time_range: "day", max_results: 8 }),\n'
        ");\nreturn { todayFlow: a, hotspots: b, news: c };"
    )
    TOOLS = {"mcp__CherryHub__exec"}

    def body(self, code: str, wrapped: bool = True) -> str:
        inner = ('{"tool_calls":[{"name":"mcp__CherryHub__exec","arguments":'
                 '{"code":"' + code + '"}}]}')
        return inner if wrapped else '{"name":"mcp__CherryHub__exec","arguments":{"code":"' + code + '"}}'

    def test_plain_json_rejects_it_first(self):
        """先确认这段确实非法 —— 否则整个测试场景就是假的。"""
        with self.assertRaises(json.JSONDecodeError):
            json.loads(self.body(self.CODE.replace("\n", "\\n")))

    def test_unescaped_quotes_are_repaired_and_code_survives(self):
        """修复后必须拿到调用，且 code **逐字**不变（引号、换行都还原）。"""
        calls = gp.parse_tool_calls(self.body(self.CODE.replace("\n", "\\n")), self.TOOLS)
        self.assertIsNotNone(calls)
        self.assertEqual(len(calls), 1)
        code = json.loads(calls[0]["function"]["arguments"])["code"]
        self.assertEqual(code, self.CODE)
        self.assertEqual(code.count("callTool"), 2)

    def test_single_call_shape_also_repairs(self):
        """``{"name":..,"arguments":..}`` 这种不带 tool_calls 数组的写法同样能救。"""
        calls = gp.parse_tool_calls(self.body(self.CODE.replace("\n", "\\n"), wrapped=False),
                                    self.TOOLS)
        self.assertEqual([c["function"]["name"] for c in calls or []], ["mcp__CherryHub__exec"])

    def test_raw_newlines_inside_the_value_are_tolerated(self):
        """另一种常见写法：该转成 \\n 的换行直接给真空行（strict 模式会报控制字符错）。"""
        calls = gp.parse_tool_calls(self.body(self.CODE), self.TOOLS)
        self.assertIsNotNone(calls)
        self.assertEqual(json.loads(calls[0]["function"]["arguments"])["code"], self.CODE)

    def test_unknown_tool_name_still_refuses_the_repair(self):
        """修不修得动是一回事，工具名不在客户端名单里就必须拒绝 —— 不许硬给一个调用。"""
        self.assertIsNone(gp.parse_tool_calls(
            self.body(self.CODE.replace("\n", "\\n")), {"other_tool"}))

    def test_properly_escaped_output_is_untouched(self):
        """基准：本来就合法的 JSON 走原路径，不该被修复逻辑改动。"""
        good = json.dumps({"tool_calls": [{"name": "mcp__CherryHub__exec",
                                          "arguments": {"code": self.CODE}}]},
                          ensure_ascii=False)
        calls = gp.parse_tool_calls(good, self.TOOLS)
        self.assertEqual(json.loads(calls[0]["function"]["arguments"])["code"], self.CODE)

    def test_plain_answer_is_not_mistaken_for_a_call(self):
        """普通回答（含引号、含大括号）不能被误判成工具调用。"""
        for text in ("北京今天晴，21 度。", '{"note":"这不是工具调用"}',
                     '他说："调用 inspect({ name: "x" }) 就行"'):
            self.assertIsNone(gp.parse_tool_calls(text, self.TOOLS), msg=text)

    def test_pathological_input_terminates(self):
        """极端输入（一长串裸引号）必须很快放弃，而不是把请求线程卡住。"""
        started = time.time()
        self.assertIsNone(gp.parse_tool_calls('{"name":"x","arguments":{"code":' + '""' * 400,
                                              {"x"}))
        self.assertLess(time.time() - started, 2.0)


class WindowsPathAndFullwidthJsonTest(unittest.TestCase):
    """两类以前一定解析失败的畸形参数（借 dsh-glm-web 的真机教训补的抢救）。

    ① Windows 路径：模型写 ``{"path": "C:\\Users\\x"}`` 时反斜杠只写了一个 ——
       ``\\U``/``\\x`` 不是合法 JSON 转义，整段非法，调用退化成散文，日志看不出原因。
    ② 全角引号：中文模型常把 JSON 的定界符写成 ``“ ”``。
    """

    PATH = "C:\\Users\\silzh\\docs\\STATE.md"
    TOOLS = {"read_file", "get_weather"}

    def json_body(self) -> str:
        return ('{"tool_calls":[{"name":"read_file","arguments":{"path":"'
                + self.PATH + '"}}]}')

    def test_the_raw_text_is_really_invalid(self):
        """先确认场景成立：这段必须让 json.loads 报错，否则整组测试在测空气。"""
        with self.assertRaises(json.JSONDecodeError):
            json.loads(self.json_body())

    def test_single_backslash_path_is_rescued(self):
        calls = gp.parse_tool_calls(self.json_body(), self.TOOLS)
        self.assertEqual(len(calls), 1)
        self.assertEqual(json.loads(calls[0]["function"]["arguments"])["path"], self.PATH)

    def test_textual_call_with_windows_path_is_rescued(self):
        """文字形态（协议要求的写法）走的是另一条解析链，同样得救回来。"""
        calls = gp.parse_textual_tool_calls('read_file({"path": "%s"})' % self.PATH, self.TOOLS)
        self.assertEqual([c["function"]["name"] for c in calls or []], ["read_file"])
        self.assertEqual(json.loads(calls[0]["function"]["arguments"])["path"], self.PATH)

    def test_fullwidth_quotes_are_rescued(self):
        raw = "{“tool_calls”:[{“name”:“get_weather”,“arguments”:{“city”:“北京”}}]}"
        calls = gp.parse_tool_calls(raw, self.TOOLS)
        self.assertEqual(len(calls), 1)
        self.assertEqual(json.loads(calls[0]["function"]["arguments"]), {"city": "北京"})

    def test_escape_fix_is_idempotent_on_valid_json(self):
        """本来就写对的 JSON 不能越修越错（\\n、\\"、\\\\ 都不该被再翻一倍）。"""
        good = json.dumps({"path": self.PATH, "code": 'x = "a"\n'}, ensure_ascii=False)
        self.assertEqual(gp._fix_invalid_escapes(good), good)

    def test_truncated_path_is_still_refused(self):
        """红线不破：路径写到一半就断，补齐只会长出一个参数被腰斩的假调用 —— 必须拒绝。"""
        self.assertIsNone(gp.parse_tool_calls(
            '{"tool_calls":[{"name":"read_file","arguments":{"path":"' + 'C:\\Users\\sil',
            self.TOOLS))


class ToolMissLogTest(unittest.TestCase):
    """「没调用工具」的日志不该把正常回答误报成故障。

    实测：日志里 `模型没按工具协议输出` 出现 10 次，其中 8 次是模型主动选择的
    正常文字回答（协议允许，finish_reason=stop），只有 2 次是真异常。
    文案改成「模型选择直接回答」，并把诊断日志限制到真正可疑时才打。
    """

    TOOLS = {"mcp__CherryHub__list", "mcp__CherryHub__inspect",
             "mcp__CherryHub__invoke", "mcp__CherryHub__exec"}

    def test_normal_answers_do_not_trigger_diagnostics(self):
        """真实日志里的三个正常回答都不该被判为「想调用却没调出来」。"""
        normals = [
            "我通过搜索工具查到了北京近期景区客流情况，整理如下：\n\n"
            "## 北京景区客流情况\n- 国庆假期前三天累计接待游客 584.22 万人次",
            "以下是北京当前最热门的景点排行（数据来自北京旅游网及搜索结果，今天是10月5日）：\n"
            "1. 天坛\n2. 故宫",
            "可以在 Win11 上使用，但需要注意几个 Windows 特有的问题：\n## 注意事项",
        ]
        for text in normals:
            self.assertFalse(gp.looks_like_wanted_tool_call(text, "", self.TOOLS), text[:30])

    def test_malformed_json_triggers_diagnostics(self):
        """正文里有 JSON 痕迹 → 确实想调用但没调出来，该打诊断。"""
        text = '我需要调用工具 {"tool_calls": [{"name": "mcp__CherryHub__invoke" '
        self.assertTrue(gp.looks_like_wanted_tool_call(text, "", self.TOOLS))

    def test_mentioning_tool_name_triggers_diagnostics(self):
        """正文/思维链提到注册工具名 → 可疑。"""
        self.assertTrue(gp.looks_like_wanted_tool_call(
            "我需要用 invoke 去查询一下数据。", "", self.TOOLS))
        self.assertTrue(gp.looks_like_wanted_tool_call(
            "让我想想该用什么工具", "考虑 invoke 这个工具", self.TOOLS))

    def test_empty_text_never_triggers(self):
        """正文为空不算「可疑」——那种情况已由思考泄漏检测与兜底文案处理。"""
        self.assertFalse(gp.looks_like_wanted_tool_call("", "", self.TOOLS))
        self.assertFalse(gp.looks_like_wanted_tool_call("", "invoke", self.TOOLS))

    def test_short_tool_names_do_not_false_positive(self):
        """过短的短名（list/exec 之类）不参与匹配，避免正常回答里出现就误报。"""
        self.assertFalse(gp.looks_like_wanted_tool_call(
            "这是执行结果，请查收。", "", {"mcp__X__list", "mcp__X__exec"}))


class ToolResultClampTest(unittest.TestCase):
    """工具结果体积治理：防止网页垃圾把上下文撑爆，模型被淹没后答非所问。

    实测事故：一次网页抓取回灌 74000 字，峰值 prompt 达 92967 字，
    模型看不见用户原始问题，最后只答了一句「查不到实时客流」。
    """

    def test_oversized_result_is_clamped_and_keeps_both_ends(self):
        text = "开头结论" + ("填充内容。" * 3000) + "结尾来源"
        out, cut = gp.clamp_tool_result("someTool", text)
        self.assertTrue(cut)
        self.assertLessEqual(len(out), gp.TOOL_RESULT_MAX_CHARS + 200)
        self.assertIn("开头结论", out)   # 保留头
        self.assertIn("结尾来源", out)   # 保留尾
        self.assertIn("省略", out)       # 明确标注被截断

    def test_small_result_untouched(self):
        text = "故宫今日游客 6.8 万人次。"
        out, cut = gp.clamp_tool_result("someTool", text)
        self.assertFalse(cut)
        self.assertEqual(out, text)

    def test_web_result_gets_smaller_limit_and_noise_stripped(self):
        """网页类结果：上限更小，且脚本/样式/注释要剥掉。"""
        html = ("<html><head><style>.a{color:red}</style></head><body>"
                "<script>var x=1;</script><!--广告-->" + ("今日客流数据。" * 4000)
                + "</body></html>")
        out, cut = gp.clamp_tool_result("CherryBrowserOpen", html)
        self.assertTrue(cut)
        self.assertLessEqual(len(out), gp.WEB_RESULT_MAX_CHARS + 200)
        self.assertNotIn("<script>", out)
        self.assertNotIn("<style>", out)
        self.assertNotIn("<!--广告-->", out)

    def test_plain_result_keeps_more_than_web_result(self):
        """同样超长，普通结果比网页结果保留得更多（网页噪音多、密度低）。"""
        text = "x" * 20000
        plain, _ = gp.clamp_tool_result("bingSearchBingSearch", text)
        web, _ = gp.clamp_tool_result("CherryBrowserCrawlWebpage", text)
        self.assertGreater(len(plain), len(web))

    def test_convert_messages_clamps_total_budget(self):
        """累计预算生效：多条大结果不会把 prompt 撑爆。"""
        msgs = [{"role": "user", "content": "北京今天景区人数情况"}]
        for k in range(12):
            msgs.append({"role": "assistant", "content": None, "tool_calls": [{
                "id": f"c{k}", "type": "function",
                "function": {"name": "t", "arguments": "{}"}}]})
            msgs.append({"role": "tool", "tool_call_id": f"c{k}", "name": "t",
                         "content": "工具返回内容ABCDEF。" * 900})
        raw_total = 12 * len("工具返回内容ABCDEF。" * 900)
        clamped = gp.convert_messages(msgs, "", clamp_tools=True)[0]["content"][0]["text"]
        unclamped = gp.convert_messages(msgs, "", clamp_tools=False)[0]["content"][0]["text"]
        self.assertLess(len(clamped), len(unclamped) / 3)
        # 关键：用户的原始问题必须还在（之前会被淹没）
        self.assertIn("北京今天景区人数情况", clamped)
        # 也不能把工具结果整条丢掉
        self.assertIn("工具返回内容ABCDEF", clamped)

    def test_clamp_can_be_disabled(self):
        """开关关掉时行为退回旧版（原样回灌）。"""
        msgs = [
            {"role": "user", "content": "Q"},
            {"role": "assistant", "content": None, "tool_calls": [{
                "id": "c1", "type": "function",
                "function": {"name": "t", "arguments": "{}"}}]},
            {"role": "tool", "tool_call_id": "c1", "name": "t", "content": "y" * 30000},
        ]
        self.assertIn("y" * 30000,
                      gp.convert_messages(msgs, "", clamp_tools=False)[0]["content"][0]["text"])
        self.assertNotIn("y" * 30000,
                         gp.convert_messages(msgs, "", clamp_tools=True)[0]["content"][0]["text"])


class ThinkLeakTest(unittest.TestCase):
    """「思考泄漏」检测：模型把打算干什么的碎碎念当正文吐出来就结束。"""

    # 这段是实测真实输出：搜到的东西没用，模型开始自言自语，然后直接结束生成，
    # 用户看到的就是这串碎碎念（既不是工具调用，也不是回答）。
    LEAKED = (
        "Search results unhelpful. Try opening a site like 高德地图 or 新浪 news search. "
        "Try bing with different query or crawl visitbeijing. Let me try browser open of a "
        "news search, e.g. bing search \"北京 景区 客流 新闻\". Or open 高德 traffic. "
        "Try one more invoke"
    )

    def test_detects_real_leaked_monologue(self):
        self.assertTrue(gp.looks_like_think_leak(self.LEAKED, self.LEAKED))

    # 形态 B（第二次实测踩到）：正文长度 0，碎碎念全在思维链里。
    # 客户端会把思维链渲染出来，所以用户看到的仍是这串碎碎念。
    THINK_ONLY = (
        "Search results are useless (same generic results). Try browsing a news site, "
        "e.g. Beijing daily news. Try fetching a search engine directly via browser or "
        "fetch markdown of e.g. Bing news search URL. Let me try browser open with bing "
        "news query.\n\nTry browser open via invoke? Let's try CherryFetchFetchMarkdown "
        "on a news page, or bing news search."
    )

    def test_detects_think_only_leak_with_empty_body(self):
        """正文为空、思维链在自言自语 → 也要判定（修复前的盲区）。"""
        self.assertTrue(gp.looks_like_think_leak("", self.THINK_ONLY))

    def test_think_only_leak_detected_even_when_ending_with_period(self):
        """该形态的思维链可能以句号收尾，不能只靠「结尾缺标点」判断。"""
        self.assertTrue(self.THINK_ONLY.endswith("."))
        self.assertTrue(gp.looks_like_think_leak("", self.THINK_ONLY))

    def test_empty_body_and_empty_reasoning_is_flagged(self):
        """形态 C：正文与思维链都空（模型完全没输出）→ 必须续问，否则用户收到空气。

        这条曾经被写成 return False 放过（注释写「属于另一类问题」），
        结果工具已搜到数据却不给结论。现在必须判定为异常。
        """
        self.assertTrue(gp.looks_like_think_leak("", ""))
        self.assertTrue(gp.looks_like_think_leak("   ", "  \n "))

    def test_empty_body_with_normal_reasoning_is_not_flagged(self):
        """正文空但思维链是正常推理（不是碎碎念）→ 不判定，避免无谓续问。"""
        normal = "用户在问今天的景区客流。我先查一下搜索工具能否返回实时数据，再据此回答。"
        self.assertFalse(gp.looks_like_think_leak("", normal))

    def test_plausible_answer_with_numbers_is_not_flagged(self):
        """正文给出了数字/结论，即便提到 search 也不该判定。"""
        text = "今天故宫接待 6.8 万人次。建议你参考这个 search results 里的数字。"
        self.assertFalse(gp.looks_like_think_leak(text, ""))

    def test_detects_leak_without_reasoning(self):
        """思维链为空时，仅凭「碎碎念词 + 结尾被截断」也应判定。"""
        self.assertTrue(gp.looks_like_think_leak(self.LEAKED))

    def test_normal_answer_is_not_flagged(self):
        """正常回答不能被误杀 —— 这是最关键的回归点。"""
        for text in [
            "故宫今天游客约 7 万人，建议早上 8 点前入园避开高峰。",
            "我查了一下，故宫博物院今日接待量为 6.8 万人次。",
            "Let me search for that. Sorry, I could not find the data.",
            "没查到今天的客流数据，你可以试试高德地图实时看景区的拥挤程度。",
        ]:
            self.assertFalse(gp.looks_like_think_leak(text, ""), text)

    def test_complete_sentence_mentioning_search_is_not_flagged(self):
        """句子完整收尾 + 只是提到了搜索 → 是正常回答，不该续问。"""
        text = "Search results did not include today's figures; it seems like the park has not published them yet."
        self.assertFalse(gp.looks_like_think_leak(text, ""))

    def test_config_tries_is_readable_and_clamped(self):
        with env_patch({"GLM_TOOL_CONTINUE_TRIES": "3"}):
            self.assertEqual(gp.Config().tool_continue_tries, 3)
        with env_patch({"GLM_TOOL_CONTINUE_TRIES": "-1"}):
            self.assertEqual(gp.Config().tool_continue_tries, 0)
        with env_patch({}):
            self.assertEqual(gp.Config().tool_continue_tries, 2)


class ThinkLeakEndToEndTest(FakeUpstreamCase):
    """端到端：思考泄漏时自动续问，客户端拿到的是真正的回答/调用而不是碎碎念。"""

    def setUp(self) -> None:
        super().setUp()
        self.config = make_config(server_api_keys=["secret"], networking=False,
                                   tool_continue_tries=2)
        self.accounts = [gp.Account(self.config, "账号1", "seed-1", gp.TokenStore("", False))]
        self.pool = gp.AccountPool(self.config, self.accounts)
        self.client = gp.GLMClient(self.config, self.pool)
        self._saved = (gp.Handler.config, gp.Handler.client)
        gp.Handler.config = self.config
        gp.Handler.client = self.client
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), gp.Handler)
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        self.base = f"http://127.0.0.1:{self.port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        gp.Handler.config, gp.Handler.client = self._saved
        super().tearDown()

    def post(self, payload: dict):
        req = urllib.request.Request(
            self.base + "/v1/chat/completions",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json", "Authorization": "Bearer secret"},
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))

    def test_leaked_monologue_triggers_continue_and_returns_real_answer(self):
        leaked = ThinkLeakTest.LEAKED
        # 第一轮吐碎碎念，第二轮给出真正的正面回答
        self.fake.script = [
            ("sse", sse_with_text(leaked)),
            ("sse", sse_with_text("没查到今天的景区客流数据，建议你直接看高德地图实时路况。")),
        ]
        status, data = self.post({
            "model": "glm-4",
            "messages": [{"role": "user", "content": "北京今天景区人多吗"}],
            "tools": [WEATHER_TOOL],
        })
        self.assertEqual(status, 200)
        content = data["choices"][0]["message"]["content"]
        # 关键：客户端拿到的是第二轮的回答，而不是碎碎念
        self.assertNotIn("unhelpful", content)
        self.assertIn("高德地图实时路况", content)
        # 确实多请求了一次上游
        self.assertEqual(self.fake.stream_calls, 2)

    def test_leaked_monologue_then_tool_call(self):
        """续问后模型改为给出工具调用，也应该正常返回 tool_calls。"""
        self.fake.script = [
            ("sse", sse_with_text(ThinkLeakTest.LEAKED)),
            ("sse", sse_with_text(
                '{"tool_calls":[{"name":"get_weather","arguments":{"city":"北京"}}]}')),
        ]
        status, data = self.post({
            "model": "glm-4",
            "messages": [{"role": "user", "content": "北京今天景区人多吗"}],
            "tools": [WEATHER_TOOL],
        })
        self.assertEqual(status, 200)
        choice = data["choices"][0]
        self.assertEqual(choice["finish_reason"], "tool_calls")
        self.assertEqual(choice["message"]["tool_calls"][0]["function"]["name"], "get_weather")

    def test_think_only_leak_triggers_continue(self):
        """形态 B 端到端：正文空 + 思维链碎碎念 → 自动续问并拿到真正回答。"""
        self.fake.script = [
            ("sse", sse_with_think_only(ThinkLeakTest.THINK_ONLY)),
            ("sse", sse_with_text("今天故宫约 6.8 万人次，建议早上 8 点前入园。")),
        ]
        status, data = self.post({
            "model": "glm-4",
            "messages": [{"role": "user", "content": "北京今天景区人多吗"}],
            "tools": [WEATHER_TOOL],
        })
        self.assertEqual(status, 200)
        content = data["choices"][0]["message"]["content"]
        self.assertIn("6.8 万人次", content)
        self.assertNotIn("CherryFetchFetchMarkdown", content)
        self.assertEqual(self.fake.stream_calls, 2)

    def test_think_only_leak_exhausted_returns_fallback_not_raw_thinking(self):
        """续问次数用尽仍只有思维链：返回明确提示，绝不把裸思维链丢给用户。"""
        self.config.tool_continue_tries = 1
        self.fake.script = [
            ("sse", sse_with_think_only(ThinkLeakTest.THINK_ONLY)),
            ("sse", sse_with_think_only(ThinkLeakTest.THINK_ONLY)),
        ]
        status, data = self.post({
            "model": "glm-4",
            "messages": [{"role": "user", "content": "北京今天景区人多吗"}],
            "tools": [WEATHER_TOOL],
        })
        self.assertEqual(status, 200)
        content = data["choices"][0]["message"]["content"]
        self.assertEqual(content, gp.THINK_LEAK_FALLBACK)
        # 关键：用户的回答里不能出现模型的计划文本
        self.assertNotIn("CherryFetchFetchMarkdown", content)
        self.assertNotIn("bing news search", content)
        self.assertEqual(self.fake.stream_calls, 2)
        # 账号槽位必须已归还
        self.assertTrue(self.accounts[0].try_acquire(), "续问失败后账号槽位应已释放")
        self.accounts[0].release()

    def test_completely_empty_output_triggers_continue(self):
        """形态 C 端到端：模型收尾时什么都没吐 → 自动续问并拿到真正的回答。"""
        # 第一轮：完全没有输出（正文与思维链皆空）
        self.fake.script = [
            ("sse", 'data: {"conversation_id":"conv-1","parts":[],"status":"finish"}\n\n'),
            ("sse", sse_with_text("北京市公园管理中心通报：10月4日14家市属公园共接待游客72.97万人次。")),
        ]
        status, data = self.post({
            "model": "glm-4",
            "messages": [{"role": "user", "content": "北京今天景区人数"}],
            "tools": [WEATHER_TOOL],
        })
        self.assertEqual(status, 200)
        content = data["choices"][0]["message"]["content"]
        # 关键：不能是空回答
        self.assertNotEqual(content.strip(), "")
        self.assertIn("72.97万人次", content)
        self.assertEqual(self.fake.stream_calls, 2)

    def test_repeated_empty_output_falls_back_not_silence(self):
        """一直空输出也不能返回空气：续问用尽后给明确提示。"""
        self.config.tool_continue_tries = 1
        empty = 'data: {"conversation_id":"conv-1","parts":[],"status":"finish"}\n\n'
        self.fake.script = [("sse", empty), ("sse", empty)]
        status, data = self.post({
            "model": "glm-4",
            "messages": [{"role": "user", "content": "北京今天景区人数"}],
            "tools": [WEATHER_TOOL],
        })
        self.assertEqual(status, 200)
        content = data["choices"][0]["message"]["content"]
        self.assertEqual(content, gp.THINK_LEAK_FALLBACK)
        self.assertNotEqual(content.strip(), "")
        # 账号槽位必须已归还
        self.assertTrue(self.accounts[0].try_acquire(), "续问后账号槽位应已释放")
        self.accounts[0].release()

    def test_normal_answer_is_returned_without_extra_call(self):
        """正常回答不该被续问（避免每次都多打一次上游）。"""
        self.fake.script = [("sse", sse_with_text("故宫今天游客约 7 万人。"))]
        status, data = self.post({
            "model": "glm-4",
            "messages": [{"role": "user", "content": "北京今天景区人多吗"}],
            "tools": [WEATHER_TOOL],
        })
        self.assertEqual(status, 200)
        self.assertIn("7 万人", data["choices"][0]["message"]["content"])
        self.assertEqual(self.fake.stream_calls, 1)

    def test_continue_disabled_returns_leaked_text_as_is(self):
        """开关关掉时行为退回旧版：原样返回，不续问。"""
        self.config.tool_continue_tries = 0
        self.fake.script = [("sse", sse_with_text(ThinkLeakTest.LEAKED))]
        status, data = self.post({
            "model": "glm-4",
            "messages": [{"role": "user", "content": "北京今天景区人多吗"}],
            "tools": [WEATHER_TOOL],
        })
        self.assertEqual(status, 200)
        self.assertIn("unhelpful", data["choices"][0]["message"]["content"])
        self.assertEqual(self.fake.stream_calls, 1)

    def test_repeated_leak_stops_after_max_tries(self):
        """一直泄漏也不能无限续问：到达上限后返回最后一次内容并释放账号。"""
        self.config.tool_continue_tries = 1
        self.fake.script = [
            ("sse", sse_with_text(ThinkLeakTest.LEAKED)),
            ("sse", sse_with_text(ThinkLeakTest.LEAKED)),
        ]
        status, data = self.post({
            "model": "glm-4",
            "messages": [{"role": "user", "content": "北京今天景区人多吗"}],
            "tools": [WEATHER_TOOL],
        })
        self.assertEqual(status, 200)
        # 首次 + 1 次续问 = 2 次，不多打
        self.assertEqual(self.fake.stream_calls, 2)
        # 关键：账号槽位必须已归还，否则后续请求会一直排队到超时
        self.assertTrue(self.accounts[0].try_acquire(), "续问后账号槽位应已释放")
        self.accounts[0].release()


class ThinkLeakCopiedReasoningTest(unittest.TestCase):
    """形态 D：上游把思维链**原样复制**进正文槽（实测正文 493 字 == 思维链 493 字）。

    这段是真实漏判：措辞是「Let me search / Now let me search」，一条都不在
    THINK_LEAK_PHRASES 里，于是裸思维链被当成答案发给了客户端。
    """

    LEAKED = (
        'The user is asking "cpa怎么安装ClinePassBridge" - which translates to '
        '"how to install ClinePassBridge for cpa". This seems to be about installing '
        'some tool called "ClinePassBridge". Let me search for this to get current information.\n\n'
        'Actually, "cpa" might refer to CherryStudio\'s Claude Pass Bridge or something '
        'similar. Let me search the web for "ClinePassBridge" to understand what it is and '
        'how to install it.\n\nI already inspected the tavilySearch tool. '
        'Now let me search for "ClinePassBridge".'
    )

    def test_copied_reasoning_is_the_signal_that_catches_it(self):
        """措辞补全也救不了这条：它以句号收尾，短语门槛要求「话被截断」才判定。

        真正拦住它的是不依赖措辞的「思考副本」硬信号 —— 这就是为什么那条判定
        必须排在短语表门槛之前，而不是像早先那样写在门槛之后走不到。
        """
        self.assertFalse(gp.looks_like_think_leak(self.LEAKED, ""))
        self.assertTrue(gp.looks_like_think_leak(self.LEAKED, self.LEAKED))

    def test_copied_reasoning_is_flagged(self):
        self.assertTrue(gp.looks_like_think_leak(self.LEAKED, self.LEAKED))

    def test_partial_overlap_with_reasoning_is_flagged(self):
        reason = self.LEAKED + "\n\n然后我就直接收尾了，什么结论都没给。"
        self.assertTrue(gp.looks_like_think_leak(self.LEAKED, reason))

    def test_short_answer_mentioned_in_reasoning_is_not_flagged(self):
        """短回答整段出现在思考里是常态（「好的。」），不能按副本误杀。"""
        self.assertFalse(gp.looks_like_think_leak("好的。", "好的，我先确认一下工具参数怎么填。"))

    def test_new_phrases_catch_unterminated_monologue(self):
        """补进去的措辞：话说到一半（无收尾标点）也要判定。"""
        self.assertTrue(gp.looks_like_think_leak(
            "The user wants the install steps. Let me search the official docs", ""))


class ProcessNarrationTest(unittest.TestCase):
    """上游 agent 分步消息里的「我再进一步查找…」不该混进答案。"""

    ANSWER = (
        "## 北京景点人数情况\n\n中秋假期市属公园接待游客约 25.13 万人次，"
        "天坛公园、颐和园、北海公园游客量位列前三，建议错峰出行。"
    )
    NARRATIONS = [
        "北京旅游网首页已成功打开，可以看到一些中秋·国庆期间的游客数据。"
        "我再进一步查找今天各景区的实时人数信息。",
        "园林局官网已打开，但没有直接的今日实时客流数据。"
        "我再尝试通过搜索引擎和热门景区（如故宫、颐和园）官网查具体数据。",
    ]

    def test_narration_parts_are_dropped(self):
        text = gp.join_answer_parts([self.ANSWER] + self.NARRATIONS)
        self.assertEqual(text, self.ANSWER)
        self.assertNotIn("我再进一步查找", text)

    def test_genuine_multi_part_answer_survives_intact(self):
        """按段落切片的正常回答，一份都不能丢。"""
        parts = ["整体情况如下。", "客流方面，市属公园昨日接待 67 万人次。", "建议早上入园。"]
        self.assertEqual(gp.join_answer_parts(parts), "\n\n".join(parts))

    def test_digit_bearing_part_is_never_treated_as_narration(self):
        self.assertFalse(gp.is_process_narration("我再查一次，结果是 12 万人次。"))

    def test_all_narration_falls_back_to_original(self):
        """全是旁白时宁可原样返回，也不能把内容吃干净。"""
        self.assertEqual(gp.join_answer_parts(self.NARRATIONS),
                         "\n\n".join(self.NARRATIONS))

    def test_flag_off_keeps_everything(self):
        text = gp.join_answer_parts([self.ANSWER] + self.NARRATIONS, strip_narration=False)
        self.assertIn("我再进一步查找", text)

    def test_parts_join_in_arrival_order_not_string_sort(self):
        """logic_id 是 p1…p9、p10 时，字符串排序会把 'p10' 排到 'p9' 前面。"""
        acc = gp.StreamAccumulator()
        acc.consume(sse_event("甲", logic_id="p10"))
        acc.consume(sse_event("乙", logic_id="p9"))
        self.assertEqual(acc.full_text(), "甲\n\n乙")
        self.assertEqual(acc.part_texts(), ["甲", "乙"])


class PlatformToolResultTest(unittest.TestCase):
    """上游自带工具的结果只喂给上游模型，代理必须落一头日志，否则无从定责。"""

    def test_result_is_paired_by_call_id(self):
        part = {"logic_id": "p1", "content": [
            {"type": "tool_calls", "tool_calls": {
                "id": "c1", "name": "mcp__CherryHub__exec", "arguments": "{}"}},
            {"type": "tool_result", "tool_result": {"id": "c1", "content": "unknown tool call"}},
        ]}
        self.assertEqual(gp._platform_tool_calls(part),
                         [("mcp__CherryHub__exec", "unknown tool call")])

    def test_result_is_paired_by_position_when_ids_are_missing(self):
        part = {"logic_id": "p1", "content": [
            {"type": "tool_calls", "tool_calls": {"name": "search", "arguments": "{}"}},
            {"type": "tool_result", "tool_result": {"content": "北京今天晴"}},
        ]}
        self.assertEqual(gp._platform_tool_calls(part), [("search", "北京今天晴")])

    def test_result_head_is_recorded_on_accumulator(self):
        acc = gp.StreamAccumulator()
        acc.consume({"conversation_id": "c1", "status": "generating", "parts": [
            {"logic_id": "p1", "content": [
                {"type": "tool_calls", "tool_calls": {"id": "c1", "name": "open_url"}},
                {"type": "tool_result", "tool_result": {"id": "c1", "content": "页面内容"}},
            ]}]})
        self.assertEqual(acc.platform_tools, ["open_url"])
        self.assertEqual(acc.platform_results, ["open_url → 页面内容"])


class ToolProtocolHintTest(unittest.TestCase):
    """协议与回灌历史必须是同一种形态，否则模型在两种写法之间来回摆。"""

    def test_prompt_instructs_the_textual_form_used_in_history(self):
        tools = gp.extract_tool_definitions({"tools": [WEATHER_TOOL]})
        prompt = gp.render_tools_prompt(tools)
        self.assertIn("# TOOLS", prompt)
        self.assertIn('工具名({"参数名": "参数值"})', prompt)
        # 历史渲染（render_tool_calls）用的就是这个形态，两边要对得上
        rendered = gp.render_tool_calls(
            {"tool_calls": [{"id": "c1", "function": {"name": "get_weather",
                                                      "arguments": '{"city":"北京"}'}}]}, {})
        self.assertEqual(rendered, 'get_weather({"city":"北京"})')

    def test_no_bracketed_marker_is_emitted(self):
        """实测带方括号的调用前缀会被网页端按它自己的工具语法吃掉（连同半截工具名一起吞，
        回来的报错是「unknown tool call,tool_call] mcp__X__invoke」）。协议与历史都不许再写。"""
        prompt = gp.render_tools_prompt(gp.extract_tool_definitions({"tools": [WEATHER_TOOL]}))
        self.assertNotIn("[tool_call]", prompt)
        rendered = gp.render_tool_calls(
            {"tool_calls": [{"id": "c1", "function": {"name": "get_weather", "arguments": "{}"}}]}, {})
        self.assertNotIn("[", rendered)

    def test_prefix_free_call_line_still_parsed(self):
        """去掉前缀后兜底翻译仍要认得，否则整条 MCP 链路会断在这里。"""
        calls = gp.parse_textual_tool_calls('get_weather({"city": "北京"})', {"get_weather"})
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["function"]["name"], "get_weather")

    def test_legacy_bracketed_line_is_still_tolerated(self):
        """模型惯性（以及旧会话历史里的旧写法）仍可能带方括号前缀，解析要照旧认得。"""
        calls = gp.parse_textual_tool_calls(
            '[tool_call] get_weather({"city": "北京"})', {"get_weather"})
        self.assertEqual([c["function"]["name"] for c in calls], ["get_weather"])

    def test_json_shape_is_discouraged(self):
        """{"tool_calls": [...]} 会被网页端当成它自带的工具拦下执行 —— 提示里必须明令禁止。"""
        tools = gp.extract_tool_definitions({"tools": [WEATHER_TOOL]})
        self.assertIn("会被网页端", gp.render_tools_prompt(tools))

    def test_example_uses_a_real_declared_tool(self):
        """示例必须用本会话真实存在的工具名 + 真实必填参数。

        真机教训：示例写抽象名字时，模型照抄形状却把必填参数漏掉（实测产出 ``invoke({})``）。
        """
        tools = gp.extract_tool_definitions({"tools": [{
            "type": "function", "function": {
                "name": "read_file", "description": "读本地文件",
                "parameters": {"type": "object",
                               "properties": {"path": {"type": "string"}},
                               "required": ["path"]}},
        }]})
        prompt = gp.render_tools_prompt(tools)
        self.assertIn('read_file({"path": "./a.txt"})', prompt)
        self.assertIn("正确示例", prompt)

    def test_counterexamples_present(self):
        tools = gp.extract_tool_definitions({"tools": [WEATHER_TOOL]})
        prompt = gp.render_tools_prompt(tools)
        self.assertIn("常见错误", prompt)
        self.assertIn("漏参数：get_weather({})", prompt)
        self.assertIn("上面列表之外的任何工具名在本端都不存在", prompt)
        # 示例段本身不能带方括号（方括号前缀会被上游自己的工具语法吃掉）
        self.assertIn('编造工具名：open_url({"q": "示例值"})', prompt)

    def test_counterexample_never_names_a_declared_tool(self):
        """反例里的「编造工具名」不能恰好是客户端真注册的工具，否则反例会劝退真调用。"""
        tools = gp.extract_tool_definitions({"tools": [{
            "type": "function", "function": {
                "name": "open_url", "description": "打开网页",
                "parameters": {"type": "object",
                               "properties": {"url": {"type": "string"}},
                               "required": ["url"]}},
        }]})
        prompt = gp.render_tools_prompt(tools)
        self.assertNotIn("编造工具名：open_url(", prompt)
        self.assertIn("编造工具名：search_web(", prompt)

    def test_example_skipped_when_required_params_are_unsafe_to_guess(self):
        """必填参数多于一个就别举例：半截示例比没有示例更坏。"""
        tools = gp.extract_tool_definitions({"tools": [{
            "type": "function", "function": {
                "name": "invoke", "description": "调用 MCP 工具",
                "parameters": {"type": "object",
                               "properties": {"name": {"type": "string"},
                                              "params": {"type": "object"}},
                               "required": ["name", "params"]}},
        }]})
        prompt = gp.render_tools_prompt(tools)
        self.assertNotIn("正确示例", prompt)
        self.assertNotIn("常见错误", prompt)
        self.assertIn('工具名({"参数名": "参数值"})', prompt)   # 抽象占位行仍保留


class ToolsPromptBudgetTest(unittest.TestCase):
    """几十个 MCP Schema 全量塞进提示词，会把「协议本身」埋掉（真机：模型无视格式、编工具名）。"""

    @staticmethod
    def fat_tools(count: int = 20) -> list[dict]:
        return gp.extract_tool_definitions({"tools": [
            {"type": "function", "function": {
                "name": f"tool_{i}", "description": f"第 {i} 个工具",
                "parameters": {"type": "object", "properties": {
                    "query": {"type": "string", "description": "关键词" * 300},
                    "mode": {"type": "string", "enum": [f"v{j}" for j in range(120)]}},
                "required": ["query"]}}}
            for i in range(count)]})

    def test_definitions_are_trimmed_but_protocol_is_not(self):
        tools = self.fat_tools()
        full = gp.render_tools_prompt(tools, max_chars=0)
        trimmed = gp.render_tools_prompt(tools, max_chars=4000)
        self.assertLess(len(trimmed), len(full) / 3)
        # 协议说明永远全文保留（它才是被埋掉的那部分）
        for marker in ("# TOOLS", "行首硬要求", "形状硬要求", "正确示例"):
            self.assertIn(marker, trimmed)
        self.assertLess(trimmed.count("参数 JSON Schema"), len(tools))

    def test_omitted_tools_are_still_listed_by_name(self):
        trimmed = gp.render_tools_prompt(self.fat_tools(), max_chars=4000)
        self.assertIn("未展开的工具（参数请勿猜测，猜了必失败）：", trimmed)
        self.assertIn("tool_19", trimmed)              # 名字还在（模型得知道它存在）
        self.assertNotIn("- tool_19：", trimmed)        # 但参数不展开
        self.assertIn("仅列名字", trimmed)

    def test_budget_zero_disables_trimming(self):
        prompt = gp.render_tools_prompt(self.fat_tools(), max_chars=0)
        self.assertNotIn("未展开的工具", prompt)
        self.assertEqual(prompt.count("- tool_"), 20)

    def test_trimmed_tool_is_still_parsable(self):
        """裁剪只影响提示词：被省略参数的工具照样要能被调用。"""
        tools = self.fat_tools()
        allowed = {t["name"] for t in tools}
        self.assertNotIn("- tool_19：", gp.render_tools_prompt(tools, max_chars=4000))
        calls = gp.parse_tool_calls(
            '{"tool_calls":[{"name":"tool_19","arguments":{"query":"北京"}}]}', allowed)
        self.assertEqual(calls[0]["function"]["name"], "tool_19")


def sse_hijack(tool_name: str, result: str, answer: str) -> str:
    """造一段「上游把客户端的工具抢去自己执行了」的 SSE。"""
    part = {"logic_id": "p1", "content": [
        {"type": "tool_calls", "tool_calls": {
            "id": "c1", "name": tool_name, "arguments": "{}"}},
        {"type": "tool_result", "tool_result": {"id": "c1", "content": result}},
        {"type": "text", "text": answer},
    ]}
    return (
        'data: %s\n\n'
        'data: {"conversation_id":"conv-1","parts":[],"status":"finish"}\n\n'
    ) % json.dumps({"conversation_id": "conv-1", "parts": [part],
                    "status": "generating"}, ensure_ascii=False)


class UpstreamHijackEndToEndTest(FakeUpstreamCase):
    """上游抢跑客户端工具：这一轮不能当「模型选择直接回答」返回，必须续问重试。"""

    def setUp(self) -> None:
        super().setUp()
        self.config = make_config(server_api_keys=["secret"], networking=False,
                                  tool_continue_tries=2)
        self.accounts = [gp.Account(self.config, "账号1", "seed-1", gp.TokenStore("", False))]
        self.pool = gp.AccountPool(self.config, self.accounts)
        self.client = gp.GLMClient(self.config, self.pool)
        self._saved = (gp.Handler.config, gp.Handler.client)
        gp.Handler.config = self.config
        gp.Handler.client = self.client
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), gp.Handler)
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        self.base = f"http://127.0.0.1:{self.port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        gp.Handler.config, gp.Handler.client = self._saved
        super().tearDown()

    def post(self, payload: dict):
        req = urllib.request.Request(
            self.base + "/v1/chat/completions",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json", "Authorization": "Bearer secret"},
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))

    def test_mangled_hijack_name_is_still_detected(self):
        """上游按它自己的语法把 `[tool_` 吞掉后，回传的调用名只剩半截 —— 仍要认出是抢跑。"""
        self.fake.script = [
            ("sse", sse_hijack("tool_call] get_weather", "unknown tool call",
                               "工具调用的格式错了，我换个写法。")),
            ("sse", sse_with_text('get_weather({"city": "北京"})')),
        ]
        status, data = self.post({
            "model": "glm-4",
            "messages": [{"role": "user", "content": "北京今天景区人多吗"}],
            "tools": [WEATHER_TOOL],
        })
        self.assertEqual(status, 200)
        self.assertEqual(data["choices"][0]["finish_reason"], "tool_calls")
        self.assertEqual(self.fake.stream_calls, 2)

    def test_hijacked_call_is_reasked_then_dispatched(self):
        self.fake.script = [
            # 第一轮：模型写的调用被上游执行了，客户端拿到的是上游沙箱里的失败结果
            ("sse", sse_hijack("get_weather", "unknown tool call",
                               "由于查询渠道受限，实时数据未能获取，建议查看官方渠道。")),
            # 第二轮：改用无标记的文字形态（行首直接是工具名），客户端的工具这才真正跑起来
            ("sse", sse_with_text('get_weather({"city": "北京"})')),
        ]
        status, data = self.post({
            "model": "glm-4",
            "messages": [{"role": "user", "content": "北京今天景区人多吗"}],
            "tools": [WEATHER_TOOL],
        })
        self.assertEqual(status, 200)
        choice = data["choices"][0]
        self.assertEqual(choice["finish_reason"], "tool_calls")
        self.assertEqual(choice["message"]["tool_calls"][0]["function"]["name"], "get_weather")
        self.assertEqual(self.fake.stream_calls, 2)

    def test_hijack_retries_are_bounded_and_lease_released(self):
        self.config.tool_continue_tries = 1
        self.fake.script = [
            ("sse", sse_hijack("get_weather", "unknown tool call", "渠道受限。")),
            ("sse", sse_hijack("get_weather", "unknown tool call", "渠道受限。")),
        ]
        status, data = self.post({
            "model": "glm-4",
            "messages": [{"role": "user", "content": "北京今天景区人多吗"}],
            "tools": [WEATHER_TOOL],
        })
        self.assertEqual(status, 200)
        self.assertEqual(self.fake.stream_calls, 2)
        self.assertTrue(self.accounts[0].try_acquire(), "续问后账号槽位应已释放")
        self.accounts[0].release()


if __name__ == "__main__":
    unittest.main(verbosity=2)

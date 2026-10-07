#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
chatglm.cn 最小反代 —— 把智谱清言网页版接口暴露成 OpenAI 兼容的 /v1/chat/completions

纯标准库实现，无任何第三方依赖，代码完全可读可控。仅供本地学习/自用测试。

相对第一版补齐的稳定性能力：
    * 账号串行闸：上游对同一账号只允许一个生成在跑，本地按账号排队，
      并发请求不再直接吃「请等待其他对话生成完毕」
    * 撞闸退避重试：识别并发闸/限流类业务错误与 429/5xx，指数退避后重试
    * 多账号：GLM_REFRESH_TOKENS 逗号分隔，账号间可并行、失效自动轮换
    * refresh_token 落盘：上游轮换后的 token 写入 .glm_tokens.json，重启不再用回旧值

签名算法逆向自智谱清言桌面客户端 resources/app.asar 的 src/main/auth-headers.js：
    X-Timestamp: 毫秒时间戳，把「倒数第 2 位」替换为 (各位数字之和 - 倒数第 2 位) % 10
    X-Nonce:     uuid4 hex
    X-Sign:      md5(f"{timestamp}-{nonce}-{SIGN_SECRET}")

用法：
    set GLM_REFRESH_TOKEN=xxxx   (Windows cmd)   /   export GLM_REFRESH_TOKEN=xxxx
    python glm_proxy.py
    # 不配 token 则自动走游客模式（能力受限）
"""

from __future__ import annotations

import argparse
import codecs
import gzip
import hashlib
import http.client
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Anthropic Messages API（/v1/messages）适配层。注意 anthropic_api 反过来不 import 本模块：
# 以 `python glm_proxy.py` 运行时本模块名是 __main__，再 import 会拿到第二份模块副本，
# 异常类身份不一致会让 except 全部失效，所以需要的少量对象由 Handler 显式注入。
import anthropic_api

# ─────────────────────────── 常量 ───────────────────────────
# 与桌面客户端 auth-headers.js 中的 SIGN_SECRET_PROD 完全一致
SIGN_SECRET = "8a1317a7468aa3ad86e997d08f3f31cb"
DEFAULT_BASE_URL = "https://chatglm.cn/chatglm"
DEFAULT_ASSISTANT_ID = "65940acff94777010aa6b796"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0"
)
EXPOSED_MODELS = [
    "glm-4",
    "glm-4-flash",
    "glm-4-air",
    "glm-4.6",
    "glm-4.7",
    "glm-5",
    "glm-5.3",
]
ACCESS_TOKEN_TTL = 3600
DEFAULT_TOKEN_FILE = ".glm_tokens.json"

# 上游并发闸/限流的业务文案特征。命中即认定「同一账号同时只能有一个生成」，
# 走退避重试而不是直接把 502 甩给客户端。
BUSY_MARKERS = (
    "等待其他对话",
    "其他对话生成",
    "并发",
    "频繁",
    "太快",
    "请稍后",
    "too many",
    "rate limit",
    "ratelimit",
    "concurrency",
    "busy",
)


# ─────────────────────────── 签名 ───────────────────────────
def build_sign() -> tuple[str, str, str]:
    """返回 (X-Timestamp, X-Nonce, X-Sign)，逻辑与客户端 JS 逐位等价。"""
    now = str(int(time.time() * 1000))
    digits = [int(ch) for ch in now]
    # 客户端：sum(digits) - digits[len - 2]
    checksum = (sum(digits) - digits[-2]) % 10
    timestamp = now[:-2] + str(checksum) + now[-1]
    nonce = uuid.uuid4().hex
    sign = hashlib.md5(f"{timestamp}-{nonce}-{SIGN_SECRET}".encode("utf-8")).hexdigest()
    return timestamp, nonce, sign


def browser_headers(user_agent: str, app_fr: str = "browser_extension") -> dict[str, str]:
    streaming = app_fr != "default"
    return {
        "Accept": "text/event-stream" if streaming else "application/json, text/plain, */*",
        "Accept-Encoding": "identity" if streaming else "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "App-Name": "chatglm",
        "Cache-Control": "no-cache",
        "Content-Type": "application/json",
        "Origin": "https://chatglm.cn",
        "Pragma": "no-cache",
        "User-Agent": user_agent,
        "X-App-Fr": app_fr,
        "X-App-Platform": "pc",
        "X-App-Version": "0.0.1",
        "X-Lang": "zh",
    }


def signed_headers(access_token: str, user_agent: str, app_fr: str = "browser_extension"):
    timestamp, nonce, sign = build_sign()
    return {
        **browser_headers(user_agent, app_fr),
        "Authorization": f"Bearer {access_token}",
        "X-Device-Id": uuid.uuid4().hex,
        "X-Nonce": nonce,
        "X-Request-Id": uuid.uuid4().hex,
        "X-Sign": sign,
        "X-Timestamp": timestamp,
    }


def read_json(resp) -> dict:
    raw = resp.read()
    if resp.headers.get("Content-Encoding", "").lower() == "gzip":
        raw = gzip.decompress(raw)
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"上游返回非 JSON 对象: {type(payload).__name__}")
    return payload


# ─────────────────────────── 异常 ───────────────────────────
class QueueTimeout(RuntimeError):
    """本地串行队列等待超时：所有账号长时间都在生成中。"""


class UpstreamBusy(RuntimeError):
    """撞上游并发闸：同一账号同时只允许一个对话生成。"""


class UpstreamAuthError(RuntimeError):
    """上游鉴权失败，且本账号的 refresh_token 已无法换取 access_token。"""


# ─────────────────────────── 配置 ───────────────────────────
def env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def split_tokens(raw: str, *extra: str) -> list[str]:
    """把逗号/分号/空白分隔的 token 串拆成列表（保持顺序、去重）。"""
    tokens: list[str] = []
    for chunk in [raw or ""] + [e or "" for e in extra]:
        for token in chunk.replace(";", ",").replace("\n", ",").split(","):
            token = token.strip()
            if token and token not in tokens:
                tokens.append(token)
    return tokens


def parse_model_map(raw: str) -> dict[str, str]:
    """解析 ``GLM_MODEL_ASSISTANT_MAP``，形如 ``glm-5.3=65940acff94777010aa6b796,glm-4.6=...``。

    用于把「模型名」映射到不同的上游 assistant_id（网页版里不同助手/模型对应不同 ID）。
    键不区分大小写，``=`` 与 ``:`` 都能当分隔符。
    """
    mapping: dict[str, str] = {}
    for pair in split_tokens(raw):
        for separator in ("=", ":"):
            if separator in pair:
                model, _, assistant = pair.partition(separator)
                model, assistant = model.strip().lower(), assistant.strip()
                if model and assistant:
                    mapping[model] = assistant
                break
    return mapping


class Config:
    def __init__(self) -> None:
        self.base_url = os.environ.get("GLM_BASE_URL", DEFAULT_BASE_URL).rstrip("/")
        self.refresh_token = os.environ.get("GLM_REFRESH_TOKEN", "").strip()
        # 多账号：GLM_REFRESH_TOKENS 与 GLM_REFRESH_TOKEN 合并（单 token 作为兼容写法，排在最前）
        self.refresh_tokens = split_tokens(
            self.refresh_token, os.environ.get("GLM_REFRESH_TOKENS", "")
        )
        self.force_guest = env_bool("GLM_USE_GUEST", False)
        self.guest_fallback = env_bool("GLM_GUEST_FALLBACK", True)
        self.assistant_id = os.environ.get("GLM_ASSISTANT_ID", DEFAULT_ASSISTANT_ID).strip()
        self.user_agent = os.environ.get("GLM_USER_AGENT", DEFAULT_USER_AGENT).strip()
        self.timeout = int(os.environ.get("GLM_TIMEOUT", "180"))
        self.host = os.environ.get("HOST", "127.0.0.1").strip()
        self.port = int(os.environ.get("PORT", "8000"))
        self.delete_conversation = env_bool("GLM_DELETE_CONVERSATION", True)
        self.server_api_keys = [k.strip() for k in os.environ.get("SERVER_API_KEYS", "").split(",") if k.strip()]
        self.verbose = env_bool("GLM_VERBOSE", False)

        # ── 并发治理 ──
        # 同一账号同时只跑一个生成是上游硬约束，GLM_QUEUE_TIMEOUT 决定本地排队等多久
        self.queue_timeout = float(os.environ.get("GLM_QUEUE_TIMEOUT", "180"))
        self.busy_retries = int(os.environ.get("GLM_BUSY_RETRIES", "3"))
        self.busy_backoff = float(os.environ.get("GLM_BUSY_BACKOFF", "3"))
        self.account_cooldown = float(os.environ.get("GLM_ACCOUNT_COOLDOWN", "300"))

        # ── refresh_token 落盘 ──
        self.persist_tokens = env_bool("GLM_PERSIST_TOKENS", True)
        self.token_file = os.environ.get("GLM_TOKEN_FILE", DEFAULT_TOKEN_FILE).strip() or DEFAULT_TOKEN_FILE

        # ── 能力开关 ──
        self.networking = env_bool("GLM_NETWORKING", False)
        # 对外暴露的模型名清单（只影响 GET /v1/models 的展示与客户端下拉框，
        # 不影响上游：网页版接口不吃 model 参数，真正决定模型的是 assistant_id）
        self.models = split_tokens(os.environ.get("GLM_MODELS", "")) or list(EXPOSED_MODELS)
        # 模型名 -> 上游 assistant_id（可选）。填了就能让不同模型名真的走不同上游助手
        self.model_assistant_map = parse_model_map(os.environ.get("GLM_MODEL_ASSISTANT_MAP", ""))

        # ── meta_data 里与「选中模型」相关的字段 ──
        # 网页真实请求里切换模型时 assistant_id 不变，唯一标明「选的是哪个模型」的字段是
        # meta_data.selected_model（如 "glm-5.3"）。本代理此前不带该字段。
        # 传了能否真的路由到别的上游模型，需要用 system_fingerprint 实测确认。
        self.selected_model = env_bool("GLM_SELECTED_MODEL", True)
        # 网页「深度思考」模式：GLM_CHAT_MODE=deep_thinking 常与 reasoning_effort 一起出现
        self.chat_mode = os.environ.get("GLM_CHAT_MODE", "").strip()
        self.reasoning_effort = os.environ.get("GLM_REASONING_EFFORT", "").strip()
        # 深度思考总开关（默认关）。这只是一个**全局默认值**：单个请求可以用
        # {"glm":{"deep_thinking":true}} / {"glm_deep_thinking":true} / {"deep_thinking":true}
        # 临时打开或关掉，不必改 .env 重启（见 extract_deep_thinking）。
        self.deep_thinking = env_bool("GLM_DEEP_THINKING", False)
        # 真实网页请求不带 if_plus_model（本代理一直在发）；留开关方便对照测试
        self.if_plus_model = env_bool("GLM_IF_PLUS_MODEL", True)
        # 网页版接口没有原生 function calling；开启后由本代理用「提示词 + JSON 解析」
        # 模拟出 OpenAI 的 tool_calls（客户端负责真正执行工具）。
        # 默认开启：客户端既然发了 tools，就是要用工具；解析不出来会安全退化成普通回答。
        # 想恢复旧行为（忽略 tools）可显式设 GLM_PROMPT_TOOL_CALLING=false。
        self.prompt_tool_calling = env_bool("GLM_PROMPT_TOOL_CALLING", True)

        # 工具链里模型偶尔会把「我接下来打算干什么」的碎碎念当正文吐出来就结束生成。
        # 命中这种「思考泄漏」时自动续问一次，最多 GLM_TOOL_CONTINUE_TRIES 次（0 = 关闭）。
        self.tool_continue_tries = max(0, int(os.environ.get("GLM_TOOL_CONTINUE_TRIES", "2") or 0))

        # 工具结果体积治理：回灌给上游时裁剪，防止网页垃圾撑爆上下文（见 clamp_tool_result）
        self.clamp_tool_result = env_bool("GLM_CLAMP_TOOL_RESULT", True)
        # 上游 agent 每一步都会产出一个 part，全拼进正文就会把
        # 「我再进一步查找…」这类过程旁白当答案发给客户端（实测污染过 950 字答案）。
        self.strip_process_narration = env_bool("GLM_STRIP_PROCESS_NARRATION", True)
        self.tool_result_max_chars = int(os.environ.get("GLM_TOOL_RESULT_MAX_CHARS", "8000") or 8000)
        self.tool_result_total_max_chars = int(
            os.environ.get("GLM_TOOL_RESULT_TOTAL_MAX_CHARS", "20000") or 20000)
        self.web_result_max_chars = int(os.environ.get("GLM_WEB_RESULT_MAX_CHARS", "4000") or 4000)
        # 工具定义体积上限（见 TOOLS_PROMPT_MAX_CHARS）；留空取默认，显式写 0 = 不裁剪
        raw_tools_budget = os.environ.get("GLM_TOOLS_PROMPT_MAX_CHARS", "").strip()
        self.tools_prompt_max_chars = (int(raw_tools_budget) if raw_tools_budget
                                       else TOOLS_PROMPT_MAX_CHARS)

    @property
    def use_guest(self) -> bool:
        return self.force_guest or not self.refresh_tokens


_LOG_SINKS: list = []


def log(*args) -> None:
    line = " ".join(str(a) for a in args)
    print(time.strftime("[%H:%M:%S]"), *args, flush=True)
    # 额外写一份纯 UTF-8 日志文件。启动脚本若用 PowerShell Tee-Object 落盘，
    # 默认编码是 UTF-16，中文全是乱码（排查问题时根本读不了）。
    for path in _LOG_SINKS:
        try:
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(f"{time.strftime('[%H:%M:%S]')} {line}\n")
        except OSError:
            pass  # 日志落盘失败不能影响服务


# ─────────────────────────── refresh_token 落盘 ───────────────────────────
class TokenStore:
    """把上游轮换后的 refresh_token 落盘，避免「重启后又用回 .env 里的旧值」。

    存储结构（seed = 你在 .env 里填的原始 token，refresh_token = 当前最新值）::

        {"version": 1, "accounts": [{"seed": "...", "refresh_token": "..."}]}

    注意：文件里是明文凭据，已在 .gitignore 中忽略，请勿提交/外发。
    """

    def __init__(self, path: str, enabled: bool = True) -> None:
        self.path = path
        self.enabled = bool(enabled and path)
        self._lock = threading.Lock()
        self._seeds: dict[str, str] = self._load()

    def _load(self) -> dict[str, str]:
        if not self.enabled or not os.path.exists(self.path):
            return {}
        try:
            with open(self.path, encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception as exc:
            log(f"[token] 读取 {self.path} 失败（忽略并继续）: {exc}")
            return {}
        seeds: dict[str, str] = {}
        if isinstance(data, dict):
            for item in data.get("accounts") or []:
                if isinstance(item, dict) and item.get("seed") and item.get("refresh_token"):
                    seeds[str(item["seed"])] = str(item["refresh_token"])
        if seeds:
            log(f"[token] 已从 {self.path} 载入 {len(seeds)} 个账号的 refresh_token")
        return seeds

    def resolve(self, seed: str) -> str:
        """把 .env 里的原始 token 映射成当前最新 token。"""
        return self._seeds.get(seed, seed)

    def remember(self, seed: str, refresh_token: str) -> None:
        with self._lock:
            if self._seeds.get(seed) == refresh_token:
                return
            self._seeds[seed] = refresh_token
            self._save()

    def _save(self) -> None:
        if not self.enabled:
            return
        payload = {
            "version": 1,
            "accounts": [
                {"seed": s, "refresh_token": t} for s, t in sorted(self._seeds.items())
            ],
        }
        tmp = self.path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2)
            os.replace(tmp, self.path)
        except Exception as exc:
            log(f"[token] 写入 {self.path} 失败（忽略）: {exc}")


# ─────────────────────────── 账号：token + 串行闸 ───────────────────────────
class Account:
    """一个上游账号，负责两件事：

    1. ``refresh_token -> access_token``（带缓存、带轮换落盘）
    2. **串行闸**：上游对同一账号有并发硬限制（同时只允许一个对话生成），
       所以每个账号挂一个大小为 1 的信号量，把同账号的请求排队串行化。
       这是本项目最关键的一处稳定性补丁 —— 没有它，并发请求会直接吃
       「请等待其他对话生成完毕」并把 502 甩给客户端。
    """

    def __init__(self, config: Config, name: str, seed: str, store: TokenStore) -> None:
        self.config = config
        self.name = name
        self.seed = seed
        self.store = store
        # .env 里填的是 seed，落盘文件里可能已有更新的 token
        self.refresh_token = store.resolve(seed) if seed else ""
        self.is_guest = not self.refresh_token
        self._lock = threading.Lock()
        self._access_token = ""
        self._expires_at = 0.0
        self._slot = threading.Semaphore(1)
        self._cooldown_until = 0.0
        # 只给日志用（谁占着这个账号、占了多久、排第几），不参与互斥——
        # 真正的互斥仍是 _slot 那个大小为 1 的信号量。
        self.busy = False
        self.holder = ""
        self.held_since = 0.0

    # ── 串行闸 ──
    def try_acquire(self, desc: str = "") -> bool:
        acquired = self._slot.acquire(blocking=False)
        if acquired:
            self.busy, self.holder = True, desc
            self.held_since = time.time()
        return acquired

    def release(self) -> None:
        self.busy, self.holder = False, ""
        try:
            self._slot.release()
        except ValueError:  # 重复释放不该崩服务
            pass

    def cooldown(self, seconds: float) -> None:
        """临时拉黑本账号（鉴权彻底失效时用），期间不再被调度。"""
        self._cooldown_until = max(self._cooldown_until, time.time() + seconds)

    @property
    def available(self) -> bool:
        return time.time() >= self._cooldown_until

    # ── token ──
    def get_access_token(self, force: bool = False) -> str:
        with self._lock:
            if not force and self._access_token and time.time() < self._expires_at - 60:
                return self._access_token
            self._access_token, self._expires_at = self._fetch()
            return self._access_token

    def _fetch(self) -> tuple[str, float]:
        if self.is_guest:
            return self._fetch_guest_token()
        try:
            return self._refresh_access_token()
        except UpstreamAuthError:
            raise
        except Exception as exc:
            raise UpstreamAuthError(f"[{self.name}] refresh_token 无法换取 access_token: {exc}") from exc

    def _refresh_access_token(self) -> tuple[str, float]:
        url = f"{self.config.base_url}/user-api/user/refresh"
        req = urllib.request.Request(
            url, data=b"{}", method="POST",
            headers={**signed_headers(self.refresh_token, self.config.user_agent)},
        )
        with urllib.request.urlopen(req, timeout=self.config.timeout) as resp:
            payload = read_json(resp)
        result = payload.get("result") or {}
        access_token = result.get("access_token")
        if not access_token:
            raise RuntimeError(f"未返回 access_token: {str(payload)[:300]}")
        new_refresh = result.get("refresh_token")
        if new_refresh and new_refresh != self.refresh_token:
            self.refresh_token = str(new_refresh)
            self.store.remember(self.seed, self.refresh_token)
            if self.store.enabled:
                log(f"[auth] {self.name} 上游下发了新的 refresh_token，已更新并落盘到 {self.store.path}")
            else:
                log(f"[auth] {self.name} 上游下发了新的 refresh_token，已更新（未落盘）")
        log(f"[auth] {self.name} access_token 已刷新（账号模式）")
        return str(access_token), time.time() + ACCESS_TOKEN_TTL

    def _fetch_guest_token(self) -> tuple[str, float]:
        url = f"{self.config.base_url}/user-api/guest/access"
        ts, nonce, sign = build_sign()
        headers = {
            **browser_headers(self.config.user_agent, app_fr="default"),
            "Content-Length": "0",
            "Referer": "https://chatglm.cn/",
            "X-Device-Id": uuid.uuid4().hex,
            "X-Nonce": nonce,
            "X-Request-Id": uuid.uuid4().hex,
            "X-Sign": sign,
            "X-Timestamp": ts,
        }
        req = urllib.request.Request(url, data=b"", method="POST", headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self.config.timeout) as resp:
                payload = read_json(resp)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "ignore")[:200]
            raise UpstreamAuthError(f"获取游客 token 失败 HTTP {exc.code}: {detail}") from exc
        result = payload.get("result") or {}
        access_token = result.get("access_token")
        if not access_token:
            raise RuntimeError(f"未返回游客 access_token: {str(payload)[:300]}")
        log(f"[auth] {self.name} 已获取游客 access_token（能力受限，无账号历史）")
        return str(access_token), time.time() + ACCESS_TOKEN_TTL


class Lease:
    """一次上游生成的账号租约：拿到即代表该账号此刻归你独占，用完必须 release()。

    释放时机是「整个响应流读完」之后，而不是「请求刚发出」——
    上游的闸是按生成时长占用的。
    """

    def __init__(self, account: Account) -> None:
        self.account = account
        self._released = False

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        self.account.release()


class AccountPool:
    """多账号池：轮询挑选空闲账号；全忙则本地排队等待（带超时）。

    - 单账号：等价于一条串行队列，第 2 个并发请求排队而不是撞上游闸
    - 多账号：账号之间可并行，吞吐随账号数上升
    - 账号鉴权彻底失效：冷却一段时间（GLM_ACCOUNT_COOLDOWN）后被跳过
    - 所有账号都不可用且 GLM_GUEST_FALLBACK=true：临时退回游客账号兜底
    """

    def __init__(self, config: Config, accounts: list[Account]) -> None:
        self.config = config
        self._accounts = list(accounts)
        self._lock = threading.Lock()
        self._cursor = 0
        self._waiting: list[object] = []     # 排队票据，保证账号槽位先到先得
        self._guest: Account | None = None

    @property
    def accounts(self) -> list[Account]:
        return list(self._accounts)

    def ensure_guest(self) -> Account:
        with self._lock:
            if self._guest is None:
                log("[auth] 账号均不可用，临时退回游客模式兜底（能力受限）")
                self._guest = Account(self.config, "游客", "", TokenStore("", False))
            return self._guest

    def candidates(self) -> list[Account]:
        """当前可参与的账号；全部冷却中时退回游客（若允许兜底）。"""
        live = [a for a in self._accounts if a.available]
        if live:
            return live
        if self.config.guest_fallback:
            return [self.ensure_guest()]
        return []

    def _rotated(self, accounts: list[Account]) -> list[Account]:
        if not accounts:
            return []
        with self._lock:
            self._cursor = (self._cursor + 1) % len(accounts)
            start = self._cursor
        return accounts[start:] + accounts[:start]

    def acquire(self, desc: str = "") -> Lease:
        """取一个账号槽位；全忙则按**到达顺序**排队（带超时）。

        排队必须先到先得：原来的轮询抢槽不分先后，后到的请求能插到主请求前面
        （真机：一条侧请求先拿到槽，把主请求压住 4.4s）。另外排队日志必须说清
        「谁占着账号、占了多久、我在第几位」，否则光有等待时长根本没法归因。
        """
        deadline = time.time() + self.config.queue_timeout
        ticket = object()
        with self._lock:
            self._waiting.append(ticket)
        queued_at = None
        try:
            while True:
                with self._lock:
                    ahead = self._waiting.index(ticket)      # 前面还排着几个人
                live = self.candidates()
                # 有几个空闲账号，就放队首几个人去抢（多账号时不至于被排队串行化）
                if ahead < max(1, sum(1 for a in live if not a.busy)):
                    for account in self._rotated(live):
                        if account.try_acquire(desc):
                            if queued_at is not None:
                                log(f"[queue] 排队 {time.time() - queued_at:.1f}s"
                                    f"（前面 {ahead} 人）后获得账号槽位：{account.name}")
                            return Lease(account)
                if time.time() >= deadline:
                    raise QueueTimeout(
                        f"等待 {self.config.queue_timeout:.0f}s 仍无空闲账号（可能都卡在上游生成中）"
                    )
                if queued_at is None:
                    queued_at = time.time()
                    holders = "、".join(
                        f"{a.name} 已持有 {time.time() - a.held_since:.1f}s"
                        f"（{a.holder or '未知请求'}）"
                        for a in live if a.busy) or "无"
                    log(f"[queue] 账号均忙，请求进入本地队列"
                        f"（第 {ahead + 1} 位，排队上限 {self.config.queue_timeout:.0f}s；"
                        f"持有者：{holders}）")
                time.sleep(0.2)
        finally:
            with self._lock:
                if ticket in self._waiting:
                    self._waiting.remove(ticket)


# ─────────────────────────── 消息转换 ──────────────────────────
# 客户端工具结果为空时向上游注入的占位说明（见 convert_messages）
EMPTY_TOOL_RESULT_HINT = "（工具未返回任何结果；可能执行失败、超时或被拒绝）"

# ── 工具结果体积治理 ──
# 实测踩坑（很关键）：工具结果回灌给上游时原本是**全量原样塞进 prompt**。
# 一次网页抓取（浏览器工具取搜索页 markdown）就灌进 74000 字，
# 加上工具列表(7002字)与多轮搜索结果，峰值 prompt 达 92967 字。
# 上下文被网页垃圾填满后，模型「看不见用户原始问题」，只在垃圾堆里打转，
# 最后答非所问（例如「查不到实时客流」）。所以回灌必须限体积。
TOOL_RESULT_MAX_CHARS = 8000         # 单条工具结果上限
TOOL_RESULT_TOTAL_MAX_CHARS = 20000  # 所有工具结果累计上限
WEB_RESULT_MAX_CHARS = 4000          # 网页/抓取类结果的额外上限（噪音多、密度低）

# 工具**定义**在提示词里的体积上限（只算定义行，协议说明永远全文保留）。
# 真机：CherryHub 几十个 MCP Schema 全量塞入，光定义就上万字，正好把协议本身埋掉 ——
# 模型开始无视格式、编造工具名。裁剪后被省略的工具仍然可以被调用（解析用的是完整列表）。
TOOLS_PROMPT_MAX_CHARS = 20000
WEB_RESULT_HINTS = (
    "fetch", "markdown", "crawl", "browse", "browser", "webpage", "html",
    "snapshot", "open_url", "scrape",
)


def _looks_like_web_result(name: str, text: str) -> bool:
    """判断工具结果是不是网页抓取类（需要更激进地降噪）。"""
    low = (name or "").lower()
    if any(h in low for h in WEB_RESULT_HINTS):
        return True
    head = (text or "")[:400].lower()
    return head.startswith(("<!doctype", "<html", "```html")) or head.count("\n## ") >= 3


def _strip_web_noise(text: str) -> str:
    """粗略剥掉网页/HTML 里的脚本、样式、注释等噪音。

    刻意保守：只删**明确**是噪音的部分，不做激进清洗，
    以免把真正有用的表格/正文也删掉。
    """
    if not text:
        return text
    out = re.sub(r"(?is)<(script|style|noscript|svg|head)[^>]*>.*?</\1>", " ", text)
    out = re.sub(r"<!--.*?-->", " ", out, flags=re.S)
    out = re.sub(r"[ \t]{3,}", "  ", out)
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out.strip()


def clamp_tool_result(name: str, text: str, limit: int = 0) -> tuple[str, bool]:
    """把单条工具结果裁剪到上限内，返回 ``(裁剪后文本, 是否被裁过)``。

    网页类先降噪再裁剪；裁剪保留**头尾**（开头多是结论/状态，结尾常有来源信息）。
    ``limit`` 为 0 时用按类型决定的默认上限。
    """
    if not text:
        return "", False
    is_web = _looks_like_web_result(name, text)
    if not limit:
        limit = WEB_RESULT_MAX_CHARS if is_web else TOOL_RESULT_MAX_CHARS
    if len(text) <= limit:
        return text, False
    if is_web:
        stripped = _strip_web_noise(text)
        if len(stripped) <= limit:
            return stripped, True
        text = stripped
    head = int(limit * 0.7)
    tail = limit - head
    clipped = (
        f"{text[:head]}\n\n"
        f"……（中间省略 {len(text) - limit} 字，"
        f"如需完整内容请用更精确的查询重新获取）……\n\n"
        f"{text[-tail:]}"
    )
    return clipped, True


def extract_text_content(content) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts = []
    for item in content:
        if not isinstance(item, dict):
            continue
        t = item.get("type")
        if t == "text":
            parts.append(str(item.get("text", "")))
        elif t == "image_url":
            parts.append(f"[image:{item.get('image_url', {}).get('url', '')}]")
    return "\n".join(p for p in parts if p)


def render_tool_calls(message: dict, call_names: dict) -> str:
    """把 assistant 消息里的 tool_calls / function_call 渲染成文本，并登记 call_id -> 名字。"""
    lines = []
    calls = message.get("tool_calls")
    if isinstance(calls, list):
        for call in calls:
            if not isinstance(call, dict):
                continue
            fn = call.get("function") if isinstance(call.get("function"), dict) else call
            name = str(fn.get("name") or "").strip()
            if not name:
                continue
            args = fn.get("arguments", {})
            if not isinstance(args, str):
                args = json.dumps(args, ensure_ascii=False)
            if call.get("id"):
                call_names[str(call["id"])] = name
            lines.append(f"{name}({args})")
    legacy = message.get("function_call")  # 更老的字段名
    if isinstance(legacy, dict) and legacy.get("name"):
        args = legacy.get("arguments", {})
        if not isinstance(args, str):
            args = json.dumps(args, ensure_ascii=False)
        lines.append(f"{legacy['name']}({args})")
    return "\n".join(lines)


def convert_messages(messages: list, extra_instructions: str = "",
                    clamp_tools: bool = True, clamp_cfg=None) -> list:
    """把 OpenAI 的 messages 拍平成单条 user 文本（网页版接口只吃单轮结构）。

    额外说明：
    - ``role: "tool"``（以及老式的 ``role: "function"``）会被渲染成 ``Tool(名字): 结果``，
      否则多轮工具调用的上下文会在拍平时**整条丢失**，模型会以为用户没说过话。
    - ``assistant`` 消息里的 ``tool_calls`` 渲染成 ``名字(参数)``（行首不带任何标记，
      见 TOOL_PROTOCOL_HINT：带方括号前缀会被上游自己的工具语法吃掉）。
    - ``extra_instructions`` 用于注入工具协议等附加说明（见 render_tools_prompt）。
      它会被排在对话**之后**并补一段生成提示 —— 协议块放头部时会被长 system prompt 埋掉。
    - ``clamp_tools``：限制工具结果体积（默认开）。不限制时实测会出现
      单条 7.4 万字的网页结果、峰值 9.3 万字 prompt，模型被淹没后答非所问。
      ``clamp_cfg`` 为 Config 时用其中的阈值，否则用模块默认值。
    """
    instructions, turns = [], []
    call_names: dict[str, str] = {}
    max_chars = getattr(clamp_cfg, "tool_result_max_chars", TOOL_RESULT_MAX_CHARS) \
        if clamp_cfg else TOOL_RESULT_MAX_CHARS
    web_max = getattr(clamp_cfg, "web_result_max_chars", WEB_RESULT_MAX_CHARS) \
        if clamp_cfg else WEB_RESULT_MAX_CHARS
    total_max = getattr(clamp_cfg, "tool_result_total_max_chars", TOOL_RESULT_TOTAL_MAX_CHARS) \
        if clamp_cfg else TOOL_RESULT_TOTAL_MAX_CHARS
    tool_budget = total_max   # 工具结果累计预算
    for m in messages:
        if not isinstance(m, dict):
            continue
        role = str(m.get("role", ""))

        if role in ("tool", "function"):
            text = extract_text_content(m.get("content"))
            name = str(
                m.get("name")
                or call_names.get(str(m.get("tool_call_id") or ""), "")
                or "tool"
            )
            if not text:
                # 工具结果为空（客户端执行失败/超时/返回空）时不能整条丢弃：
                # 否则上游看到的对话是「Assistant 说要用工具，然后就没了」，模型
                # 会因为上下文断层而下一轮吐空或答非所问。注入显式占位，让模型知道
                # 工具已尝试但无返回，从而改用其它途径或如实说明失败。
                text = EMPTY_TOOL_RESULT_HINT
                log(f"[tools] 工具结果为空（{name}），已向上游注入占位说明")
                turns.append((f"Tool({name})", text))
                continue

            if clamp_tools:
                # 网页类有更小的默认上限，这里取「按类型的默认上限」与「预算剩余」的较小者
                default_limit = web_max if _looks_like_web_result(name, text) else max_chars
                room = max(600, min(default_limit, tool_budget))
                text, cut = clamp_tool_result(name, text, room)
                tool_budget -= len(text)
                if cut:
                    log(f"[tools] 工具结果过长已裁剪 {name}："
                        f"{len(text)}字（原上限{room}，累计预算剩{max(0, tool_budget)}）")
            turns.append((f"Tool({name})", text))
            continue

        text = extract_text_content(m.get("content"))
        call_text = render_tool_calls(m, call_names) if role == "assistant" else ""
        if not text and not call_text:
            continue

        if role in ("system", "developer"):
            instructions.append(text)
        elif role == "user":
            turns.append(("User", text))
        elif role == "assistant":
            turns.append(("Assistant", "\n".join(p for p in (text, call_text) if p)))

    blocks = []
    if instructions:
        blocks.append("# INSTRUCTIONS\n\n" + "\n\n".join(instructions))
    blocks.append("# CONVERSATION")
    for label, text in turns:
        blocks.append(f"{label}: {text}")
    # 协议块放在对话**之后**：实测放在最前面时，它会被客户端的超长 system prompt 与
    # 几十个工具定义埋掉，模型直接无视（甚至编出网页端自带的工具名来）。尾部指令的遵守度远高于头部。
    if extra_instructions:
        blocks.append(extra_instructions)
        blocks.append(TOOL_FRAME_TAIL)
    prompt = "\n\n".join(blocks).strip()
    return [{"role": "user", "content": [{"type": "text", "text": prompt + "\n\nAssistant: "}]}]


# ────────────────────────── SSE 解析 ───────────────────────────
def iter_sse_events(resp):
    """逐块解析上游 SSE，yield 每个 JSON 事件。

    上游偶尔会在没有 ``status=finish`` 的情况下掐断连接（实测抛
    ``IncompleteRead``）。这里不把它当异常往上抛：已读到的那部分是有效数据，
    照常解析完再结束，由调用方的 ``StreamAccumulator.finalize()`` 补齐尾部。
    否则整条响应流会以「半句话 + 没有 [DONE]」的形式挂在客户端上。
    """
    stream = resp
    if resp.headers.get("Content-Encoding", "").lower() == "gzip":
        stream = gzip.GzipFile(fileobj=resp)

    decoder = codecs.getincrementaldecoder("utf-8")("ignore")
    pending = ""
    truncated = False
    while True:
        try:
            raw = stream.read(4096)
        except http.client.IncompleteRead as exc:
            raw, truncated = exc.partial or b"", True
        except OSError as exc:      # socket.timeout / 连接被重置
            log(f"[upstream] 读取 SSE 中断（{exc.__class__.__name__}: {exc}），按流结束处理")
            raw, truncated = b"", True
        if raw:
            pending += decoder.decode(raw, False).replace("\r\n", "\n")
            while "\n\n" in pending:
                block, pending = pending.split("\n\n", 1)
                event = _parse_sse_block(block.strip())
                if event is not None:
                    yield event
        if not raw or truncated:
            break
    if truncated:
        log("[upstream] 响应流未正常结束（上游提前断开），已按已有内容收尾")
    pending += decoder.decode(b"", True)
    if pending.strip():
        event = _parse_sse_block(pending.strip())
        if event is not None:
            yield event


def _parse_sse_block(block: str):
    lines = [ln for ln in block.split("\n") if ln.startswith("data:")]
    if not lines:
        return None
    payload = "\n".join(ln[5:].strip() for ln in lines)
    if payload == "[DONE]":
        return None
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _common_prefix_len(a: str, b: str) -> int:
    limit, index = min(len(a), len(b)), 0
    while index < limit and a[index] == b[index]:
        index += 1
    return index


class StreamAccumulator:
    """累积上游 parts，按 logic_id 计算文本增量。

    上游的真实语义（抓帧实测，见 ``probe`` 记录：36 个思维链分片长度之和 == 末态快照 782 字，
    逐字相等）：每个 ``logic_id`` 每帧推的是**有序增量分片**，全部推完后再推**一次完整快照**。
    所以策略是「分片立刻追加、立刻外发」，快照只做覆盖校正 —— 于是：

      * 思维链/正文能真正逐字流出去，客户端不再等到最后一刻才看到内容
      * 中途断流也只剩「少最后一个分片」，而不是像以前那样只剩被覆盖后的一两个碎片
        （旧实现把每帧当成完整状态来覆盖，末态快照没到时 ``parts`` 里就只剩最后一次的分片）

    早先以为上游会「分片乱序/改写」，其实那是拿**分片和上一帧的分片**互相比较产生的错觉；
    真正的重复帧是逐字相同的，用 ``chunk == 已累积`` 就能识别。

    兜底：万一末态与已发内容不再互为前缀（SSE 收不回已发出的字节），
    ``_flush`` 的 rescue 分支会从公共前缀处把缺的部分补出去 —— 接缝重复几个字，
    也比整段丢掉好（旧行为会让正文永久停在 ``to`` 这种两字碎片上）。
    """

    def __init__(self) -> None:
        self.conversation_id = ""
        self.served_model = ""                       # 上游回报的实际模型（如 moe_53f）
        self.parts: dict[str, tuple[str, str]] = {}   # logic_id -> (累积正文, 累积思维链)
        self.part_order: list[str] = []               # logic_id 的**到达顺序**（见 _ordered_parts）
        self._sent: dict[str, list[str]] = {}         # logic_id -> [已发出的正文, 已发出的思维链]
        self._emitted_parts: set[str] = set()         # 已经吐过内容的 part（用于加分隔）
        self.rewrites = 0                             # 快照与已收分片不连续的次数（仅日志）
        self.rescued = 0                              # 终态补齐次数（接缝处会有少量重复）
        self.platform_tools: list[str] = []           # 上游平台自带工具名，只记录不回传
        self.platform_results: list[str] = []         # 这些工具回给上游模型的结果（截断）
        self._seen_tools: set[tuple[str, str]] = set()
        self._final = False
        self._warned = False
        self._rescue_warned = False

    def consume(self, event: dict) -> tuple[str, str]:
        if not self.conversation_id and event.get("conversation_id"):
            self.conversation_id = str(event["conversation_id"])

        parts = event.get("parts")
        if isinstance(parts, list):
            for part in parts:
                if not isinstance(part, dict) or not part.get("logic_id"):
                    continue
                logic_id = str(part["logic_id"])
                if part.get("model"):
                    self.served_model = str(part["model"])
                for name, result_head in _platform_tool_calls(part):
                    if (logic_id, name) in self._seen_tools:
                        continue
                    self._seen_tools.add((logic_id, name))
                    self.platform_tools.append(name)
                    flat = result_head.replace("\r", " ").replace("\n", " ")
                    self.platform_results.append(f"{name} → {flat}")
                    log(f"[upstream] 平台自带工具 {name} 被调用了："
                        "上游自己执行并把结果用于作答，不会回传给客户端"
                        + (f"；上游看到的结果：{flat!r}" if flat else "（无结果文本）"))
                self._absorb(logic_id, _render_part(part))

        return self._flush(str(event.get("status")) in ("finish", "intervene"))

    def _absorb(self, logic_id: str, incoming: tuple[str, str]) -> None:
        """把一帧的 part 内容并进累积态。"""
        if logic_id not in self.parts and logic_id not in self.part_order:
            self.part_order.append(logic_id)
        held = self.parts.get(logic_id, ("", ""))
        merged = list(held)
        for slot, chunk in enumerate(incoming):
            base = held[slot]
            if not chunk or chunk == base or base.startswith(chunk):
                continue                       # 空片 / 重复帧 / 比已收更短的旧快照
            if chunk.startswith(base):
                merged[slot] = chunk           # 末尾的完整快照
            elif base and base in chunk:
                merged[slot] = chunk           # 新片已包含旧内容：也按快照覆盖，别追加
                self.rewrites += 1
            else:
                merged[slot] = base + chunk    # 有序增量分片：追加
        self.parts[logic_id] = (merged[0], merged[1])

    def finalize(self) -> tuple[str, str]:
        """把尚未发出的内容一次性补齐。上游没推 ``finish`` 就断流时靠它收尾。

        幂等：已经终态过就直接返回空，重复调用安全。
        """
        if self._final:
            return "", ""
        return self._flush(True)

    def _flush(self, final: bool) -> tuple[str, str]:
        if final:
            self._final = True
        text_delta: list[str] = []
        reason_delta: list[str] = []

        for logic_id in self._ordered_ids():
            current = self.parts[logic_id]
            sent = self._sent.setdefault(logic_id, ["", ""])

            for slot, bucket in ((0, text_delta), (1, reason_delta)):
                truth = current[slot]
                if not truth:
                    continue
                already = sent[slot]
                if truth.startswith(already):
                    delta = truth[len(already):]
                elif final:
                    # 已发片段和真值对不上：SSE 无法撤回，整段丢弃等于把内容全丢了。
                    # 从公共前缀处补真值的剩余部分 —— 接缝处会重复几个字，但内容完整。
                    common = _common_prefix_len(already, truth)
                    if common >= len(truth):
                        continue               # 真值已被已发内容覆盖
                    self.rescued += 1
                    delta = truth[common:]
                else:
                    continue
                if not delta:
                    continue
                prefix = "\n\n" if (logic_id not in self._emitted_parts and self._emitted_parts) else ""
                self._emitted_parts.add(logic_id)
                sent[slot] += delta                   # sent 只记 part 原文，不含分隔符
                bucket.append(prefix + delta)         # 与 full_text() 的分隔保持一致

        if self.rewrites and not self._warned:
            self._warned = True
            log("[upstream] 上游快照与已收分片不连续，已按末态快照校正")
        if self.rescued and not self._rescue_warned:
            self._rescue_warned = True
            log("[upstream] 已发出的片段被上游改写，已在终态补齐剩余内容"
                "（接缝处可能有少量重复，但不丢内容）")

        return "".join(text_delta), "".join(reason_delta)

    def _ordered_ids(self) -> list[str]:
        """part 的**到达顺序**，兜底补上任何没记进 order 的 id。

        原来这里用 ``sorted(self.parts)`` 按 logic_id 字符串排序：上游的分片消息是
        ``p1…p9、p10``，字符串排序下 ``'p10' < 'p9'``，多 part 的回答会被排成
        前后颠倒的一坨。到达顺序才是上游的叙述顺序。
        """
        ordered = [lid for lid in self.part_order if lid in self.parts]
        ordered += [lid for lid in self.parts if lid not in set(ordered)]
        return ordered

    def part_texts(self) -> list[str]:
        """各 part 的正文（按到达顺序、已 strip），空 part 不返回。"""
        return [self.parts[lid][0].strip() for lid in self._ordered_ids()
                if self.parts[lid][0].strip()]

    def full_text(self) -> str:
        return "\n\n".join(self.part_texts())

    def full_reasoning(self) -> str:
        return "\n\n".join(x for x in (r.strip() for _, r in
                                      (self.parts[k] for k in self._ordered_ids())) if x)


def _render_part(part: dict) -> tuple[str, str]:
    """单个 part -> (正文, 思维链)。

    ⚠ 这里**不能** strip：上游每帧推的是有序增量分片，累积器要把它们拼回末态快照。
    逐片 strip 会吃掉分片首尾的换行与缩进，拼出来的文本就和真值对不上了；
    收尾统一在 ``full_text()`` / ``full_reasoning()`` 里 strip。
    """
    texts, reasonings = [], []
    content = part.get("content")
    if not isinstance(content, list):
        return "", ""
    for item in content:
        if not isinstance(item, dict):
            continue
        t = item.get("type")
        if t == "text":
            texts.append(str(item.get("text", "")))
        elif t == "think":
            reasonings.append(str(item.get("think", "")))
        elif t == "code":
            texts.append(f"```\n{item.get('code', '')}\n```")
        elif t == "execution_output":
            texts.append(str(item.get("content", "")))
        elif t == "image":
            for img in item.get("image") or []:
                if isinstance(img, dict) and img.get("image_url"):
                    texts.append(f"![image]({img['image_url']})")
    return "\n".join(x for x in texts if x), "\n".join(x for x in reasonings if x)


# 上游自带工具的结果只留这一头进日志：足够看清上游沙箱回了什么（是报错还是数据），
# 又不会把整份网页抓取灌进日志文件。
PLATFORM_TOOL_RESULT_LOG_CHARS = 200


def _platform_tool_calls(part: dict) -> list[tuple[str, str]]:
    """上游**平台自带工具**（search / 沙箱执行等）在这一个 part 里的 ``(调用名, 结果文本头)``。

    形状（抓帧实测）：``{"type":"tool_calls","tool_calls":{"id","name","arguments"}}``，
    后面还会跟一个 ``type=="tool_result"`` 的 item —— 说明上游已经自己把工具跑完了。

    ⚠ 只记录、**绝不翻译成客户端的 tool_calls**：这些工具不在客户端声明的 ``tools`` 里，
    客户端也没有它们的实现，回传会让客户端去执行一个不存在的工具并卡在等结果。

    结果文本必须留一头日志：这些结果是喂给**上游模型**的，我们和客户端都看不到。
    实测就被这个盲区坑了一整轮排查 —— 模型转述「unknown tool call」，但没有任何
    落地证据能说明这句话出自上游还是客户端，只能靠猜。
    """
    calls: list[tuple[str, str]] = []
    named_results: dict[str, str] = {}
    anon_results: list[str] = []
    for item in part.get("content") or []:
        if not isinstance(item, dict):
            continue
        if item.get("type") == "tool_calls":
            call = item.get("tool_calls")
            if not isinstance(call, dict):
                continue
            name = str(call.get("name") or "").strip()
            if name:
                calls.append((str(call.get("id") or ""), name))
        elif item.get("type") == "tool_result":
            res = item.get("tool_result")
            if not isinstance(res, dict):
                continue
            text = str(res.get("content") or "").strip()
            if res.get("id"):
                named_results[str(res["id"])] = text
            else:
                anon_results.append(text)
    found: list[tuple[str, str]] = []
    for index, (call_id, name) in enumerate(calls):
        text = named_results.get(call_id) or ""
        if not text and index < len(anon_results):
            text = anon_results[index]
        found.append((name, text[:PLATFORM_TOOL_RESULT_LOG_CHARS]))
    return found


# ─────────────────────────── 上游调用 ───────────────────────────
def is_busy_payload(payload: dict) -> bool:
    """判断上游返回的 JSON 业务错误是不是「并发闸/限流」类。"""
    text = json.dumps(payload, ensure_ascii=False).lower()
    return any(marker in text for marker in BUSY_MARKERS)


def extract_networking(payload: dict, default: bool = False) -> bool:
    """解析本客户端的联网搜索开关，三种写法都认：

    - ``{"glm": {"networking": true}}``      （推荐的附加参数写法）
    - ``{"glm_networking": true}``           （平铺写法）
    - ``{"tools": [{"type": "web_search"}]}``（OpenAI 风格的弱映射）
    """
    glm = payload.get("glm")
    if isinstance(glm, dict) and "networking" in glm:
        return bool(glm["networking"])
    if "glm_networking" in payload:
        return bool(payload["glm_networking"])
    tools = payload.get("tools")
    if isinstance(tools, list):
        for tool in tools:
            if isinstance(tool, dict) and "search" in str(tool.get("type", "")).lower():
                return True
    return default


def extract_deep_thinking(payload: dict, default: bool = False) -> bool:
    """解析本客户端的「深度思考」开关，三种写法都认：

    - ``{"glm": {"deep_thinking": true}}``  （推荐的附加参数写法，与联网一致）
    - ``{"glm_deep_thinking": true}``       （平铺写法）
    - ``{"deep_thinking": true}``           （顶层写法，最直观）

    返回 True 时，上游请求会带上 ``meta_data.chat_mode="deep_thinking"``
    与 ``reasoning_effort``（网页真实请求里这两项一起出现，少了哪个都不算开）。
    ``default`` 是全局默认值（``GLM_DEEP_THINKING``），请求体没写时用它。
    """
    glm = payload.get("glm")
    if isinstance(glm, dict) and "deep_thinking" in glm:
        return bool(glm["deep_thinking"])
    for key in ("deep_thinking", "glm_deep_thinking"):
        if key in payload:
            return bool(payload[key])
    return default


def has_function_tools(payload: dict) -> bool:
    """是否带了 function calling 定义（网页版接口不支持，需要提示用户）。"""
    tools = payload.get("tools")
    if not isinstance(tools, list):
        return False
    for tool in tools:
        if isinstance(tool, dict) and str(tool.get("type", "")) == "function":
            return True
    return False


def extract_tool_definitions(payload: dict) -> list[dict]:
    """取请求里的 tools 定义（只保留 type=function 的），拿不到就返回空列表。"""
    tools = payload.get("tools")
    if not isinstance(tools, list):
        return []
    result = []
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        if str(tool.get("type", "function")) != "function":
            continue
        fn = tool.get("function") if isinstance(tool.get("function"), dict) else tool
        name = str(fn.get("name") or "").strip()
        if not name:
            continue
        result.append({
            "name": name,
            "description": str(fn.get("description") or "").strip(),
            "parameters": fn.get("parameters") if isinstance(fn.get("parameters"), dict) else {},
        })
    return result


TOOL_PROTOCOL_HINT = """# TOOLS

你可以调用下面这些工具。需要调用时，**每个调用单独一行、行首直接写工具名、
后面紧跟一个 JSON 对象参数，这一行不要有别的文字、解释或代码块**：

工具名({"参数名": "参数值"})

一次要调用多个工具就写多行（每行一个调用）。如果不需要调用工具，就按平时那样正常回答用户。

【最重要】只要你决定要调用工具（包括「结果不理想，想换个关键词／换个工具再搜一次」这种情况），
就必须**当场输出上面那种调用行本身**。绝不允许把「我再去搜一下」「换个词试试」这类意图
只写在思考或正文里而不真正输出调用行 —— 那样系统收不到调用，什么都不会执行。
换句话说：**「想调用」不等于「已调用」**，想调用就必须把调用行打出来。

【行首硬要求】调用行必须**以工具名开头**，前面不许有任何方括号标记、前缀词或代码块围栏。
带方括号前缀的写法会被网页端按它自己的工具调用语法吃掉（前缀连同半截工具名一起被吞），
结果既不是本客户端的调用、也拿不到真实数据。这与你能看到的过往对话里助手的写法一致。

【形状硬要求】**不要**输出 {"tool_calls": [...]} 这种 JSON 作为调用。那是网页端自己那套
工具调用的写法：这么写会被网页端当成它自带的工具拦下执行，本地客户端收不到调用，
工具一次也不会跑，最后只会得到一句「查不到数据」。调用一律写成上面那种
「行首工具名 + JSON 参数」的文字形态。

%s可用工具：
%s"""


# 放在协议块之后的生成提示。没有协议块时不追加（否则「按上面 # TOOLS 的协议」会指向不存在的内容）。
TOOL_FRAME_TAIL = (
    "（现在轮到你回答。需要工具就按上面 # TOOLS 的协议，另起一行直接输出调用行本身；"
    "不需要工具就只输出给用户的最终答案。）"
    "（禁止旁白与自述推理：不要写“我接下来要…”“让我先去搜一下”这类句子，也不要复述本要求；"
    "用用户使用的语言回答。）"
)

# 示例值要「像那个参数」：给 path 填一句查询词，模型就照抄出一个不像路径的路径。
_EXAMPLE_VALUE_RULES = (
    (("filepath", "file_path", "path", "file", "filename", "dir", "directory"), "./a.txt"),
    (("url", "link", "href"), "https://example.com"),
    (("city", "location"), "北京"),
    (("query", "keyword", "q"), "北京 今日 客流"),
)


def _example_value(key: str) -> str:
    lowered = key.lower()
    for words, value in _EXAMPLE_VALUE_RULES:
        if any(word in lowered for word in words):
            return value
    return "示例值"


def _example_args(tool: dict) -> dict | None:
    """造一个「必填参数一个不缺」的参数字典；造不出合格示例返回 None。

    必填里有非 string 项、或必填多于一个，就放弃举例 —— 半截示例比没有示例更坏，
    模型会照抄形状并把剩下的参数漏掉（真机实测正是这个形态：``invoke({})``）。
    """
    params = tool.get("parameters") or {}
    props = params.get("properties")
    if not isinstance(props, dict) or not props:
        return None
    strings = [key for key, spec in props.items()
               if isinstance(spec, dict) and str(spec.get("type", "")).lower() == "string"]
    if not strings:
        return None
    required = params.get("required")
    required = required if isinstance(required, list) else []
    if len(required) == 1 and required[0] in strings:
        return {required[0]: _example_value(str(required[0]))}
    if required:
        return None
    return {strings[0]: _example_value(str(strings[0]))}


def _counterexample_name(declared: set[str]) -> str:
    """「编造工具名」反例用的名字 —— 必须不在本会话列表里，否则反例会劝退真工具。"""
    for candidate in ("open_url", "search_web", "fetch_url"):
        if candidate not in declared:
            return candidate
    return ""


def _render_example_block(tools: list[dict]) -> str:
    """协议示例：用**本会话真实存在的工具名与必填参数**，而不是抽象占位名。"""
    chosen, best_score = "", 0
    for tool in tools:
        args = _example_args(tool)
        if args is None:
            continue
        required = (tool.get("parameters") or {}).get("required")
        score = 2 if isinstance(required, list) and required else 1
        if score > best_score:
            chosen = f"{tool['name']}({json.dumps(args, ensure_ascii=False)})"
            best_score = score
    if not chosen:
        return ""
    name = chosen.split("(", 1)[0]
    fake = _counterexample_name({t["name"] for t in tools})
    wrong = [f"- 漏参数：{name}({{}})"]
    if fake:
        wrong.append(f"- 编造工具名：{fake}({{\"q\": \"示例值\"}})"
                     "   ← 上面列表之外的任何工具名在本端都不存在")
    return (
        "正确示例（工具名与参数都取自下方列表，必填参数一个都不能少）：\n"
        f"{chosen}\n\n"
        "常见错误（网页端一律不会执行，别这样写）：\n"
        + "\n".join(wrong) + "\n\n"
    )


# 定义行压缩。整段 json.dumps(schema) 的体积九成是属性长篇说明：真机 Qoder 的 Bash
# 一行就 >10K 字，20000 的预算反而把它挤出局 —— 每轮都要用的工具没参数，
# 提示词还写着「猜了必失败」。压成「属性:类型（必填）｜一句说明」后信息密度高得多。
TOOL_DEF_DESC_MAX_CHARS = 140         # 工具描述（取首句后）上限
TOOL_DEF_PARAM_DESC_MAX_CHARS = 60    # 单个参数说明上限
TOOL_DEF_TYPE_MAX_DEPTH = 2           # 嵌套类型只展开两层，更深写成 object


def _clip(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[:limit - 1].rstrip() + "…"


def _compact_type(node: dict, depth: int = 0) -> str:
    """JSON Schema 节点 → 短标记：``string``、``array<{k:v}>``、``string[a|b]``、``a|b``。"""
    if not isinstance(node, dict):
        return "any"
    alts = node.get("anyOf") or node.get("oneOf")
    if alts:
        joined = "|".join(_compact_type(item, depth + 1)
                          for item in alts if isinstance(item, dict))
        return joined or "any"
    kind = node.get("type", "any")
    enum = node.get("enum")
    if isinstance(enum, list) and enum:
        shown = "|".join(str(v) for v in enum[:6])
        return f"{kind}[{shown}{'|…' if len(enum) > 6 else ''}]"
    if kind == "array":
        return f"array<{_compact_type(node.get('items') or {}, depth + 1)}>"
    if kind == "object" and depth < TOOL_DEF_TYPE_MAX_DEPTH:
        props = {k: v for k, v in (node.get("properties") or {}).items()
                 if isinstance(v, dict)}
        if props:
            required = set(node.get("required") or [])
            inner = "，".join(f"{key}:{_compact_type(value, depth + 1)}"
                              f"{'*' if key in required else ''}"
                              for key, value in props.items())
            return f"{{{inner}}}"
    # format 是少数几个「删了就必然出错」的 schema 字段：真机 qoder-sites 的 siteId/
    # deploymentId 全是 string+uuid，只写 string 时模型会编一个 "abc" 上去，上游 400 之后
    # 这轮工具等于白跑。pattern/default 之类太占地、且猜错只是语义不对，故不带。
    fmt = node.get("format")
    if isinstance(fmt, str) and fmt:
        return f"{kind}:{fmt}"
    return kind


def _compact_params(params: dict) -> str:
    """顶层参数逐个压成 ``名字:类型（必填）｜首句说明``。"""
    props = {k: v for k, v in (params.get("properties") or {}).items()
             if isinstance(v, dict)}
    if not props:
        return ""
    required = set(params.get("required") or [])
    parts = []
    for key, node in props.items():
        piece = f"{key}:{_compact_type(node)}"
        if key in required:
            piece += "（必填）"
        lines = str(node.get("description") or "").strip().splitlines()
        if lines:
            piece += f"｜{_clip(lines[0], TOOL_DEF_PARAM_DESC_MAX_CHARS)}"
        parts.append(piece)
    return "，".join(parts)


def render_tools_prompt(tools: list[dict], max_chars: int = TOOLS_PROMPT_MAX_CHARS) -> str:
    """把 OpenAI 的 tools 定义渲染成提示词（替代原生 function calling）。

    定义行是**压缩渲染**（描述取首句、参数写成 ``名:类型（必填）｜短说明``），
    体积仍超 ``max_chars`` 时才降级为「只列名字」；``max_chars <= 0`` 表示不裁剪。
    协议说明永远全文保留：真机几十个 MCP Schema 全量塞入会把协议本身埋掉，
    模型于是无视格式、编造工具名。

    裁剪时的挑选顺序是「客户端自带工具优先，其次按体积从小到大」：
    纯按客户端顺序贪心时，字母序最前的两个多KB schema 能吃满预算，让
    Bash/Read/Edit 这些每轮都要用的工具全部只剩名字；纯按体积贪心则会先塞进
    几十个根本用不到的 MCP 小工具（实测展开 34 个，其中 25 个是 qoder-sites，
    而 Bash、Edit、Read 依旧被降级）。自带工具优先才是有用的取舍。
    入选行仍按原始顺序输出（列表顺序可预期，不受挑选影响）。
    注意：裁剪只影响提示词，不影响能否解析 —— 调用名集合始终来自完整工具列表。
    """
    def definition(tool: dict) -> tuple[str, str]:
        desc = (tool["description"] or "").strip() or "（无描述）"
        line = f"- {tool['name']}：{_clip(desc.splitlines()[0], TOOL_DEF_DESC_MAX_CHARS)}"
        compact = _compact_params(tool.get("parameters") or {})
        if compact:
            line += f"\n  参数：{compact}"
        return line, tool["name"]

    defs = [definition(tool) for tool in tools]
    if max_chars > 0:
        used, expanded = 0, set()
        # 自带工具（非 mcp__ 前缀）先挑，同档内小的先挑
        for line, name in sorted(defs, key=lambda item: (item[1].startswith("mcp__"),
                                                          len(item[0]))):
            if used + len(line) <= max_chars:
                used += len(line)
                expanded.add(name)
        lines = [line for line, name in defs if name in expanded]
        skipped = [name for line, name in defs if name not in expanded]
    else:
        lines, skipped = [line for line, _ in defs], []

    if skipped:
        log(f"[tools] 工具定义超预算已裁剪：{len(lines)} 个完整展开、"
            f"{len(skipped)} 个仅列名字（{'、'.join(skipped[:5])}"
            f"{'…' if len(skipped) > 5 else ''}）")
        # 标题把「哪些工具参数没展开」说在前面，否则模型会以为列表里每个工具都能直接调
        lines.insert(0, f"（以下 {len(lines)} 个参数完整，另有 {len(skipped)} 个仅列名字）")
        lines.append("未展开的工具（参数请勿猜测，猜了必失败）：" + "、".join(skipped))
    return TOOL_PROTOCOL_HINT % (_render_example_block(tools), "\n".join(lines))


def _strip_code_fence(text: str) -> str:
    body = text.strip()
    if body.startswith("```"):
        lines = body.splitlines()
        lines = lines[1:]  # 去掉 ```json / ``` 这一行
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        body = "\n".join(lines).strip()
    return body


def _repair_balanced_json(text: str, start: int) -> str | None:
    """把括号失配的 JSON 片段配平后返回；拿不到完整片段返回 None。

    上游实测会多吐闭合括号，而且位置不固定：既可能在对象末尾（``...}]}]``），
    也可能夹在数组闭合之前（``...}}}}]``）。这里做一次括号配平扫描：遇到多余/
    不匹配的闭合括号就丢掉，等括号栈回到空就认为拿到了一个完整片段。
    """
    stack: list[str] = []
    pairs = {"}": "{", "]": "["}
    quote = ""
    out: list[str] = []
    index = start
    while index < len(text):
        ch = text[index]
        if quote:
            out.append(ch)
            if ch == "\\":
                if index + 1 < len(text):
                    out.append(text[index + 1])
                index += 2
                continue
            if ch == quote:
                quote = ""
        elif ch == '"':
            quote = ch
            out.append(ch)
        elif ch in "{[":
            stack.append(ch)
            out.append(ch)
        elif ch in "}]":
            if stack and stack[-1] == pairs[ch]:
                stack.pop()
                out.append(ch)
                if not stack:
                    return "".join(out)
            # 多余的闭合括号：直接丢弃，别让它破坏配平
        else:
            out.append(ch)
        index += 1
    return None


def _loads_first_json(text: str, start: int) -> dict | None:
    """从 ``text[start]``（一个 '{'）起解析第一个**完整** JSON 对象；不是 dict 或解析失败返回 None。

    优先用 ``raw_decode`` 只吃掉一个对象、忽略尾随杂质：上游实测会把
    ``{"tool_calls":[...]}`` 再补一个 ``}`` 收尾（``...}]}]}``），按「首 { 到末 }」
    整段 `json.loads` 会直接报错，把本该是工具调用的输出误判成普通回答。
    ``raw_decode``/整段解析都啃不动时（如数组闭合前多一个 ``}``），再兜底做括号配平。

    仍失败则尝试**补齐闭合符号**抢救：上游分片乱序/改写时（见 StreamAccumulator
    的「稳定前缀」策略），累积器可能只拿到 JSON 的一段，尾部 ``}]}`` 缺失。
    这时把栈里未闭合的括号按相反顺序补上再试一次 —— 宁可给一个结构完整、
    但参数可能不全的调用，也不要把用户的一整轮工具链直接废掉。
    """
    for candidate in _json_candidates(text, start):
        try:
            obj, _ = json.JSONDecoder().raw_decode(candidate, start)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            return obj
    # 退化路径：raw_decode 啃不动时（前导杂质等），按「首 { 到末 }」整段试一次。
    end = text.rfind("}")
    if end > start:
        try:
            obj = json.loads(text[start:end + 1])
        except json.JSONDecodeError:
            pass
        else:
            return obj if isinstance(obj, dict) else None
    # 末路兜底：括号失配（上游多吐闭合括号，位置还不定），配平后再解析。
    repaired = _repair_balanced_json(text, start)
    if repaired:
        try:
            obj = json.loads(repaired)
        except json.JSONDecodeError:
            pass
        else:
            if isinstance(obj, dict):
                return obj
    # 还是不行 → 大概率是「字符串里的引号没转义」，见 _loads_with_inner_quote_fix。
    return _loads_with_inner_quote_fix(text, start)


INNER_QUOTE_FIX_MAX = 1500


def _loads_with_inner_quote_fix(text: str, start: int) -> dict | None:
    """专治「参数里写代码、内层双引号没转义」的输出（实测 ``mcp__CherryHub__exec``）。

    模型经常这样吐（``code`` 的值里全是裸 ``"``，整段 JSON 因此非法，括号倒是配平的，
    ``_repair_balanced_json`` 也帮不上）：
    ``{"tool_calls":[{"name":"...","arguments":{"code":"mcp.callTool("tavily", { q: "北京" })"}}]}``
    于是这一整坨会退化成普通回答 —— 用户看到的就是一段本该被执行的 JSON。

    修法只有一招：JSON 扫描器每**误认一个字符串结尾**，就会在紧随其后报一个语法错，
    那就把「它刚误用的那个引号」补成 ``\\"`` 再重来。这样字符串只会往后延伸，
    误判点严格递增（``quote > last_fix`` 既是正确性要求也是防死循环），必然收敛。

    顺带用 ``strict=False``：模型还常把换行/制表直接写进字符串（该是 ``\\n`` 的写成真空行），
    严格模式会报 ``Invalid control character``，``strict=False`` 原样收下即可。

    安全性靠后面两道关卡兜：调用形状必须对、工具名必须在客户端声明的名单里，
    否则照样退化成普通回答 —— 不会因为「能解析」就硬给一个错调用。
    """
    decoder = json.JSONDecoder(strict=False)
    current = text
    last_fix = start
    for _ in range(INNER_QUOTE_FIX_MAX):
        try:
            obj, _ = decoder.raw_decode(current, start)
        except json.JSONDecodeError as exc:
            quote = current.rfind('"', start, exc.pos)
            if quote <= last_fix or current[quote - 1] == "\\":
                return None                       # 不是「误认的结尾引号」，别再猜了
            current = current[:quote] + "\\" + current[quote:]
            last_fix = quote
            continue
        return obj if isinstance(obj, dict) else None
    return None


_VALID_JSON_ESCAPES = '"\\/bfnrtu'
# 单反斜杠的盘符路径（``C:\Users``，后面紧跟的不是又一个反斜杠）：
# 出现它说明模型贴的是路径原文，一个反斜杠都不能当 JSON 转义解释
_DRIVE_PATH = re.compile(r"[A-Za-z]:\\[^\\\"]")


def _has_invalid_escape(body: str) -> bool:
    index, size = 0, len(body)
    while index < size:
        if body[index] != "\\":
            index += 1
            continue
        nxt = body[index + 1] if index + 1 < size else ""
        if nxt and nxt not in _VALID_JSON_ESCAPES:
            return True
        index += 2
    return False


def _fix_json_string(body: str) -> str:
    """修一个 JSON 字符串字面量**内部**的反斜杠。

    两种形态触发「整个值都按字面反斜杠处理」（实测比逐个转义判断准得多）：
      * 值里有任一非法转义（``E:\\github`` 的 ``\\g``）—— 说明模型根本不是在写 JSON 转义，
        它就是把路径原样贴进来了，于是同一段里的 ``\\b``/``\\n``/``\\t`` 也一律按字面处理；
      * 值里出现单反斜杠的盘符路径（``del C:\\backup\\file``）—— 段名全撞上合法转义
        （``C:\\bob\\file``）时只有这条认得出来。
    """
    literal_mode = bool(_DRIVE_PATH.search(body)) or _has_invalid_escape(body)
    out: list[str] = []
    index, size = 0, len(body)
    while index < size:
        ch = body[index]
        if ch != "\\":
            out.append(ch)
            index += 1
            continue
        nxt = body[index + 1] if index + 1 < size else ""
        if nxt in ("\\", '"'):              # 已经是一对合法转义，整体带走
            out.append("\\" + nxt)
            index += 2
        elif nxt == "":                     # 末尾孤立反斜杠：原样
            out.append(ch)
            index += 1
        elif literal_mode or nxt not in _VALID_JSON_ESCAPES:
            out.append("\\\\" + nxt)        # 单反斜杠 → 双反斜杠
            index += 2
        else:
            out.append(ch + nxt)            # 合法转义：原样
            index += 2
    return "".join(out)


def _fix_invalid_escapes(text: str) -> str:
    """把 JSON 里的**非法单反斜杠**补成合法转义（``C:\\Users`` → ``C:\\\\Users``）。

    实测事故（Windows 上的文件类工具最高频）：模型写 ``{"path": "C:\\Users\\x"}``，
    ``\\U``/``\\x`` 都不是合法 JSON 转义，整个对象直接解析失败，本该执行的工具调用
    退化成一段散文，日志里还看不出原因。

    这里**必须成对扫描**而不是用一个正则：``\\\\`` 是两个反斜杠，
    纯正则会看到「第二个反斜杠后面跟着 ``U``」就把已经写对的内容再补一遍，
    越修越多（本函数的第一版就是这么写错的，测试 ``test_escape_fix_is_idempotent`` 拦住了它）。

    也只可能**按字符串字面量逐段处理**：整段一起扫分不清键与值，而「这条值算不算路径」
    必须以值为单位判断。

    以前的残留坑（真机事故：Read 一个 Windows 路径吃掉一个字母，客户端报「文件不存在」，
    模型改口说「调用名被系统篡改」并凭记忆编完答案）：光修非法转义时，同一条路径里
    ``\\b``/``\\n``/``\\t`` **是**合法 JSON 转义，会被静默解释成控制字符而吃掉字母 ——
    现在由 ``_fix_json_string`` 的字面量模式一起兜住。
    """
    out: list[str] = []
    index, size = 0, len(text)
    while index < size:
        if text[index] != '"':
            out.append(text[index])
            index += 1
            continue
        end = index + 1
        while end < size:
            if text[end] == "\\":
                end += 2
                continue
            if text[end] == '"':
                break
            end += 1
        out.append('"')
        out.append(_fix_json_string(text[index + 1:end]))
        if end >= size:
            break
        out.append('"')
        index = end + 1
    return "".join(out)


def _escape_variants(frag: str) -> list[str]:
    """反斜杠修复的候选，按尝试顺序返回。

    盘符路径要排在**原文前面**：``{"path":"C:\\bob\\file"}`` 本来就是合法 JSON，
    原文先过就会把 ``\\b``/``\\f`` 解释成退格/换页，路径字母被吃掉 ——
    真机事故：Read 报「文件不存在」，模型转而声称「调用名被系统篡改」并凭记忆编完答案。
    其余情况仍原文优先：合法 JSON 里的 ``\\n``/``\\t`` 多半真是换行/制表符。
    """
    fixed = _fix_invalid_escapes(frag)
    if fixed == frag:
        return [frag]
    if _DRIVE_PATH.search(frag):
        return [fixed, frag]
    return [frag, fixed]


def _normalize_fullwidth_quotes(text: str) -> str:
    """把全角引号当定界符的写法换回 ASCII（中文模型高频：``{“city”:“北京”}``）。

    只在原样解析失败之后才试，所以代价是「值里本来就成对出现的全角引号」会被误当定界符 ——
    比起整条工具链被废掉，这个取舍划得来。
    """
    return (text.replace("“", "\"").replace("”", "\"")
                .replace("‘", "'").replace("’", "'"))


def _json_candidates(text: str, start: int):
    """按优先级产出待尝试的 JSON 文本变体。

    顺序（便宜、无损的在前，同一候选只产一次）：
    原样 → 补闭合 → 修单反斜杠 → 修单反斜杠+补闭合 → 全角引号 → 全角引号+补闭合。

    **只改写 ``text[start:]`` 再拼回前缀**：调用方用 ``raw_decode(candidate, start)``
    按下标取对象，前缀里的杂质不能影响偏移量，所以任何变换都不能碰 ``text[:start]``。

    **补齐闭合符号只在「切点落在结构边界上」时做**：末尾缺 ``}]}`` 属于纯粹的结构不完整，
    参数本身是完整的，补上就能得到与模型意图一致的调用。
    但如果截断点落在**字符串或数值内部**（比如 query 只写了一半），补齐会造出
    一个参数被腰斩的调用 —— 那比不调用更危险（客户端会拿着残缺参数去执行）。
    这种情况下不产出该候选，维持「宁可退化成普通回答，也不给错调用」的原有原则。
    补齐量是**按变换后的片段重新算**的：单反斜杠没修时 ``\\U`` 会让扫描器把字符串
    结尾的引号误当成被转义，误判成「切点落在字符串内部」而拒绝补齐。
    """
    head, frag = text[:start], text[start:]
    variants = _escape_variants(frag)
    fullwidth = _normalize_fullwidth_quotes(frag)
    if fullwidth not in variants:
        variants.append(fullwidth)
    seen = set()
    for base in variants:
        for candidate in (base, base + _missing_closers(base, 0)):
            if candidate in seen:
                continue
            seen.add(candidate)
            yield head + candidate


def _missing_closers(text: str, start: int) -> str:
    """算出末尾缺失的闭合符号；切点不安全（落在结构内部）时返回空串。

    「结构内部」= 截断时正处于一个**未闭合的字符串字面量**里，或正在写一个
    还没写完的裸值（数字/字面量）。这两种情况补齐会造出参数被腰斩的调用。
    落在 ``,`` ``:`` ``{`` ``[`` 这些分隔符之后（结构边界）则是安全的。
    """
    pairs = {"{": "}", "[": "]"}
    openers = {"}": "{", "]": "["}
    stack: list[str] = []
    in_str = False
    quote = ""
    escaped = False
    # 截断处是否停在一个未完成的裸值里（如 12. 或 tr）
    mid_value = False
    # 截断处是否停在一个「键名已写、值还没配」的位置（如 ...,"name":"x"）
    dangling_key = False
    for ch in text[start:]:
        if in_str:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == quote:
                in_str = False
                # 字符串闭合后若直接结束，说明这个键还没配值
                dangling_key = True
            continue
        if ch in "\"'":
            in_str, quote = True, ch
            mid_value = False
            continue
        if ch in pairs:
            stack.append(ch)
            mid_value = False
            dangling_key = False
            continue
        if ch in "}]":
            if stack and stack[-1] == openers[ch]:
                stack.pop()
            mid_value = False
            dangling_key = False
            continue
        if ch in " \t\r\n":
            continue
        if ch == ",":
            # 逗号开启新的键（等值）
            mid_value = False
            dangling_key = True
            continue
        if ch == ":":
            # 冒号后必须跟值；若这里就结束 → 值缺失
            mid_value = True
            dangling_key = False
            continue
        # 其余（数字、true/false/null 的字母）都是「值」的一部分
        mid_value = True
    if in_str or dangling_key:
        return ""
    if not stack:
        return ""
    return "".join(pairs[c] for c in reversed(stack))


def _norm_args(arguments: str) -> str:
    """参数 JSON 归一化成可比较的串（键排序、去多余空白）；解析不了就按原文比。"""
    try:
        return json.dumps(json.loads(arguments), sort_keys=True, ensure_ascii=False)
    except (json.JSONDecodeError, TypeError):
        return (arguments or "").strip()


def _dedupe_calls(calls: list[dict]) -> list[dict]:
    """同名同参的调用只留第一个（保留它的 id）。

    实测上游会把同一个工具块输出两遍：先是半截的、最后是完整版，两处都能解析成合法调用。
    不去重客户端就会执行两次 —— 写文件、发消息这类工具重复执行有真实副作用，
    不是单纯的显示问题。代价：客户端真想并行发两个同名同参调用时会被压成一个。
    """
    if len(calls) < 2:
        return calls
    seen: set[tuple[str, str]] = set()
    out: list[dict] = []
    for call in calls:
        fn = call.get("function") or {}
        key = (str(fn.get("name") or ""), _norm_args(str(fn.get("arguments") or "")))
        if key in seen:
            continue
        seen.add(key)
        out.append(call)
    if len(out) != len(calls):
        log(f"[tools] 上游把同一次调用输出了两遍，已去重 {len(calls)}→{len(out)}")
    return out


def parse_tool_calls(text: str, allowed_names: set[str] | None = None) -> list[dict] | None:
    """尽力从模型输出里解析出工具调用；解析不出来（或工具名不认识）返回 None。

    只认两种形状：``{"tool_calls":[{...}]}`` 和 ``{"name":..,"arguments":..}``。
    解析失败就当作普通回答 —— 宁可退化成聊天，也不要给出错误的工具调用。
    同名同参的重复项会被合并（见 :func:`_dedupe_calls`）。
    """
    if not text or not text.strip():
        return None
    body = _strip_code_fence(text)
    start = body.find("{")
    if start < 0:
        return None
    obj = _loads_first_json(body, start)
    if obj is None:
        return None

    raw_calls = obj.get("tool_calls")
    if raw_calls is None and ("name" in obj and "arguments" in obj):
        raw_calls = [obj]
    if not isinstance(raw_calls, list) or not raw_calls:
        return None

    calls = []
    for item in raw_calls:
        if not isinstance(item, dict):
            return None
        fn = item.get("function") if isinstance(item.get("function"), dict) else item
        name = str(fn.get("name") or "").strip()
        if not name or (allowed_names and name not in allowed_names):
            return None  # 工具名对不上 → 不是工具调用，按普通文本处理
        args = fn.get("arguments", {})
        if not isinstance(args, str):
            args = json.dumps(args, ensure_ascii=False)
        calls.append({
            "id": f"call_{uuid.uuid4().hex[:24]}",
            "type": "function",
            "function": {"name": name, "arguments": args},
        })
    return _dedupe_calls(calls)


def _find_closing(text: str, start: int) -> int:
    """从 start 处的 '{' 开始找配对的 '}'（跳过字符串里的括号）。找不到返回 -1。"""
    depth = 0
    quote = ""
    index = start
    while index < len(text):
        ch = text[index]
        if quote:
            if ch == "\\":
                index += 2
                continue
            if ch == quote:
                quote = ""
        elif ch in ("'", '"'):
            quote = ch
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return index
        index += 1
    return -1


def _loads_tolerant_obj(text: str):
    """把 JS 风格对象字面量转成 dict（键可无引号、可单引号、可尾逗号）。失败返回 None。

    **原文要先试**：本来就是合法 JSON 时，下面的单引号规则会把双引号字符串内部的
    ``'---'``（shell 命令里到处都是）换成裸双引号，把合法 JSON 改坏 → 整条调用静默丢失。
    唯一的例外是原文里有单反斜杠的盘符路径 —— 见 ``_escape_variants``。

    规范化之后再补一级抢救：修单反斜杠（``C:\\Users``）、全角引号（``{“city”:“北京”}``）。
    **顺序很关键**：这两级只在「规范化结果直接解析失败」之后才试 ——
    上面的单引号规则本来就会把路径里的反斜杠正确地重新转义，先修再转会把反斜杠加倍。
    """
    body = re.sub(r"([{,]\s*)([A-Za-z_$][\w$]*)(\s*:)", r'\1"\2"\3', text)
    body = re.sub(r"'((?:[^'\\]|\\.)*)'", lambda m: json.dumps(m.group(1).replace("\\'", "'")), body)
    body = re.sub(r",(\s*[}\]])", r"\1", body)
    candidates: list[str] = []
    for candidate in (*_escape_variants(text), *_escape_variants(body),
                      _normalize_fullwidth_quotes(body)):
        if candidate not in candidates:
            candidates.append(candidate)
    for candidate in candidates:
        try:
            obj = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            return obj
    return None


TEXTUAL_CALL_MAX = 4


# ── 思考泄漏检测 ──
# 实测坑（两种形态，都会让用户看到模型的碎碎念而不是答案）：
#   A. 碎碎念跑进正文：
#      "Search results unhelpful. Try opening a site like 高德地图 ... Try one more invoke"
#   B. 正文为空、碎碎念只在思维链里（正文长度=0，思维链 300+ 字）：
#      "Search results are useless (same generic results). Try browsing a news site ..."
# 两种都既不是工具调用也不是回答，模型说完「我打算换条路」就结束生成了。
THINK_LEAK_PHRASES = (
    "unhelpful", "try one more", "try again", "let me try", "i'll try",
    "next, i", "instead, let", "search results", "no results", "not useful",
    "try opening", "try browsing", "i need to", "seems like", "maybe i",
    "try fetching", "let me browse", "try browse", "useless",
    # 实测漏判：模型换了一套措辞（"Let me search for this" / "Now let me search"），
    # 表里没有 → 裸思维链被当成答案返回给了客户端。
    "let me search", "let me look", "let me check", "let me use", "let me query",
    "now let me", "i should search", "i should look", "i could try", "i'll search",
)
# 句子收尾标点（中英文）。缺它往往意味着话说到一半被截断。
_SENTENCE_END = ".。!！?？\"'”’)]）】"

# 正文与思维链重合判定的最小长度：短回答（「好的」「47万」）整段出现在思考里是常态，
# 低于这个长度就不按「思考副本」判定，免得把正常回答打成泄漏。
THINK_LEAK_OVERLAP_MIN = 100

# 续问次数用尽、模型仍只吐思考没给结论时，返回这段明确提示而不是裸思维链。
THINK_LEAK_FALLBACK = (
    "抱歉，这次没能给出有效结论：模型这一轮没有产出可用内容"
    "（可能是反复在“换工具再搜一次”的想法里打转，或中途没有输出）。"
    "你可以换个更具体的说法再问一次（例如指明具体景区与日期），"
    "或直接告诉我已知信息，我来帮你分析。"
)


def _plausible_answer(text: str) -> bool:
    """这段文本看起来像真回答吗（用来避免误杀正常输出）。

    正常回答会给出结论、事实或建议，且往往指向用户问题本身；
    自言自语通篇是「我要去干什么」，不涉及具体结论。
    """
    body = (text or "").strip()
    if not body:
        return False
    # 出现明确的「结论性」信号（数字/建议/指示）时，倾向于正常回答
    if any(ch.isdigit() for ch in body):
        return True
    return any(k in body for k in (
        "建议", "推荐", "可以", "需要", "因为", "所以", "目前", "已经",
        "数据", "结果", "如下", "建议您", "抱歉",
    ))


def _copied_from_reasoning(body: str, reason: str) -> bool:
    """正文是不是思维链的副本 —— 不依赖措辞、也不依赖语言的硬信号。

    实测上游会把同一段思考同时写进正文槽与 think 槽（正文 493 字 == 思维链 493 字，
    一字不差），客户端于是把英文自言自语当答案展示给用户。正常回答与思考是两份
    不同文本，只有泄漏才会高度重合。

    要求双方都不短：短回答（如「好的」）很容易整段出现在思维链里，不设门槛会误杀。
    """
    if len(body) < THINK_LEAK_OVERLAP_MIN or len(reason) < THINK_LEAK_OVERLAP_MIN:
        return False
    return (body == reason or body in reason or reason in body
            or _common_prefix_len(body, reason) >= min(len(body), len(reason)) * 0.8)


def looks_like_think_leak(text: str, reasoning: str = "") -> bool:
    """判断这次输出是不是「只有碎碎念、没有给用户的结论」。

    三种形态都要拦住：

    * **形态 A（碎碎念进了正文）**：正文命中碎碎念词且结尾被截断；
      若正文与思维链高度重合，也直接判定。
    * **形态 B（正文为空、碎碎念在思维链）**：正文为空而思维链在自言自语，
      用户最终只看到这串碎碎念（思维链会被客户端渲染出来）。
    * **形态 D（思考被复制进正文）**：正文就是思维链的副本，与措辞无关。

    为了不误杀正常回答：命中碎碎念词但**正文看起来是完整结论**时不判定。
    """
    body = (text or "").strip()
    reason = (reasoning or "").strip()

    # 形态 B：正文为空 —— 思维链就是用户唯一能看到的输出，它若在自言自语即中招。
    # 这种情况无论思维链有没有结尾标点都要判定（实测该形态可能以句号收尾）。
    if not body:
        if not reason:
            # 形态 C（实测踩坑）：正文与思维链**都是空**。模型收尾时什么都没吐，
            # 客户端收到 content="" 的空回答（表现为「模型没说话 / 没有输出」）。
            # 这必须续问 —— 否则用户看到的就是空气。
            # 注：此处曾经 return False 放过空输出，导致工具已搜到数据却不给结论。
            return True
        low = reason.lower()
        return any(p in low for p in THINK_LEAK_PHRASES)

    # 形态 A/D：正文非空。副本判定必须排在短语表门槛**之前**——
    # 实测 493 字的英文思考（"Let me search …"）不在措辞表里，被门槛短路放过，
    # 于是裸思维链当成答案返回了客户端；而重合度这条硬信号本来写在门槛之后，走不到。
    if _copied_from_reasoning(body, reason):
        return True

    low = body.lower()
    if not any(p in low for p in THINK_LEAK_PHRASES):
        return False
    # 正文像真回答（给了数字/建议/结论）→ 不判定，宁可漏判不误杀
    if _plausible_answer(body):
        return False
    # 完整收尾且不是思考副本 → 当成正常回答（实测正常回答也会提「search results」）
    return body[-1] not in _SENTENCE_END


# 过程旁白的判定上限：真正的结论往往很长，只有短分片才可能是「我下一步要干嘛」。
NARRATION_MAX_CHARS = 300

# 「我要去干什么」的起手式。刻意只收第一人称意图表达，不收「可以/建议/目前」这类
# 真回答里也高频出现的词 —— 宁可漏删，也不能把用户的实际答案吃掉。
_NARRATION_RE = re.compile(
    r"(我再|我还要|我再去|我继续|我准备|我打算|我先|我现在去|接下来我|下面我|让我(?:先|去|再)|"
    r"试着(?:查|搜|看)|换个(?:关键词|词|工具)?"
    r"|\blet\s+me\b|\bnow\s+i(?:'ll| will| should| need)\b|\bi(?:'ll| should| need to| will)\b"
    r"|\bi\s+(?:try|check|search|look)\b)",
    re.IGNORECASE)


def is_process_narration(text: str) -> bool:
    """这一小段是不是上游分步消息里的过程旁白，而不是给用户的结论。

    实测（18:37 那轮）950 字答案尾部挂着两段
    「北京旅游网首页已成功打开…我再进一步查找今天各景区的实时人数信息」——
    上游 agent 每一步都产出一个 part，全部拼进正文就当答案发回客户端了。

    判定刻意保守：带数字（说明给了事实/结论）或超过 NARRATION_MAX_CHARS 的一律保留。
    """
    body = (text or "").strip()
    if not body or len(body) > NARRATION_MAX_CHARS:
        return False
    if any(ch.isdigit() for ch in body):
        return False
    return bool(_NARRATION_RE.search(body))


def join_answer_parts(texts: list[str], strip_narration: bool = True) -> str:
    """把各 part 的正文拼成给客户端的答案，逐份剔掉纯过程旁白。

    只做「逐份判定 + 剔除」这一件事：全被剔时宁可原样返回，也不吃掉了用户的实际答案。
    （不猜测「最后一份才是结论」—— 上游把一段正常回答按段落切成多个 part 是常态，
    那种猜测会把答案砍得只剩尾巴。）
    """
    chunks = [t.strip() for t in texts if t and t.strip()]
    if len(chunks) <= 1 or not strip_narration:
        return "\n\n".join(chunks)
    kept = [c for c in chunks if not is_process_narration(c)]
    return "\n\n".join(kept or chunks)


def looks_like_wanted_tool_call(text: str, reasoning: str, tool_names: set[str]) -> bool:
    """模型是否「看起来想调用工具但没调出来」——用来决定要不要打诊断日志。

    真正的正常回答（直接给用户的内容）不需要诊断，刷屏只会淹没真问题。
    命中任一信号即认为可疑：
      * 正文里出现 JSON 痕迹（``{`` / ``tool_calls`` / ``arguments``）——像是畸形 JSON；
      * 正文或思维链里提到任一注册工具的短名（如 ``invoke``/``list``）——
        说明它本该调用，只是没给出可解析的调用。
    """
    body = (text or "").strip()
    if not body:
        return False
    if any(marker in body for marker in ("tool_calls", "\"arguments\"", "invoke(")):
        return True
    short = {n.rsplit("__", 1)[-1].rsplit(".", 1)[-1].lower()
             for n in (tool_names or set())}
    short = {s for s in short if len(s) > 2}      # 过滤掉 list/exec 这类过短词，避免误报
    haystack = (body + " " + (reasoning or "")).lower()
    return any(s in haystack for s in short)


def _tool_suffix_map(allowed_names: set[str]) -> dict[str, str]:
    """客户端注册名 → 模型可能写出的各种名字。

    完整名和最后一段短名都收录：模型时而写 ``invoke(...)``，时而写
    ``mcp__CherryHub__invoke(...)``，两种都得能认。
    """
    mapping: dict[str, str] = {}
    for name in allowed_names:
        tail = name.rsplit("__", 1)[-1].rsplit(".", 1)[-1].strip()
        for key in (name.strip(), tail):
            if key:
                mapping.setdefault(key.lower(), name)
    return mapping


def _mangled_name_candidates(name: str) -> list[str]:
    """上游吞掉调用后留在「工具名」里的碎片，逐个可能的写法摊平出来。

    名字常常对不上：上游会把没吃干净的语法碎片留在名字里（实测三种形态）：

      * ``tool_call] mcp__CherryHub__invoke`` —— 前缀被它自己的调用语法吞掉，剩下半截；
      * ``Bash({"command": "ls -la /e/github/be-xigua/"})</arg_value>`` —— 整条调用连参数
        一起塞进名字，真正的工具名只占开头几个字符；
      * ``Read`` 前后带 ``<function>``/``<parameter>`` 标签残片。
    """
    head = re.split(r"<", name, 1)[0]
    head = head.split("]")[-1].strip()
    out = [head]
    lead = re.match(r"[A-Za-z_][\w.\-]*", head)
    if lead:
        out.append(lead.group(0))
    out.append(head.rsplit("__", 1)[-1].rsplit(".", 1)[-1])
    return [c for c in dict.fromkeys(out) if c]


def _client_tool_for_name(name: str, suffix_to_tool: dict[str, str]) -> str | None:
    for candidate in _mangled_name_candidates(name):
        tool = suffix_to_tool.get(candidate.lower())
        if tool:
            return tool
    return None


# 上游自己的调用语法残片 —— 名字长成这样，就是「它吃掉了一次调用」的铁证
INVOKE_RESIDUE = re.compile(
    r"</?arg_value>|</?function>|<parameter|tool_[0-9a-f]{6,}|\(\s*\{", re.I)


def hijacked_client_tools(platform_tools: list[str], allowed_names: set[str]) -> list[str]:
    """上游「自带工具执行记录」里属于客户端 tools 的那几个 —— 即被上游抢跑的工具。

    名字比对见 ``_mangled_name_candidates``。
    """
    if not platform_tools or not allowed_names:
        return []
    suffix_to_tool = _tool_suffix_map(allowed_names)
    hit = set()
    for name in platform_tools:
        tool = _client_tool_for_name(name, suffix_to_tool)
        if tool:
            hit.add(tool)
    return sorted(hit)


def unattributed_hijacks(platform_tools: list[str], allowed_names: set[str]) -> list[str]:
    """认不出是哪个客户端工具、但确定是调用被上游吃了的那几个名字。

    真机事故（22:05:58）：上游回传的名字是 ``tool_9a822147</arg_value>`` 这类它自己编的
    散列 —— 不在客户端 tools 里，于是 ``hijacked_client_tools`` 返回空，这一轮被当成
    「模型选择直接回答」原样返回客户端：模型那三次 Read 从未执行，答案全靠它编。
    碎片形态说明它确实发起过调用，所以照样得续问。

    ``finish`` / ``search`` / ``execute_sandbox_code`` 这些平台工具名不含碎片，不会被误报。
    """
    if not platform_tools or not allowed_names:
        return []
    suffix_to_tool = _tool_suffix_map(allowed_names)
    return [name for name in platform_tools
            if INVOKE_RESIDUE.search(name) and not _client_tool_for_name(name, suffix_to_tool)]


def parse_textual_tool_calls(text: str, allowed_names: set[str]) -> list[dict] | None:
    """兜底：模型被客户端自带的说明带跑、用 JS 风格写工具调用时，也翻译成标准 tool_calls。

    识别 ``名字({...})`` 形式，名字既接受客户端**实际注册的完整工具名**，也接受
    ``mcp__X__名字`` 的最后一段，例如 Cherry Studio 的 ``mcp__CherryHub__invoke``
    与 ``invoke`` 都能映射到 ``mcp__CherryHub__invoke``。找不到能匹配的工具名就返回 None。
    """
    if not text or not allowed_names:
        return None
    suffix_to_tool = _tool_suffix_map(allowed_names)
    if not suffix_to_tool:
        return None

    # 长的名字优先：完整名要抢在短名之前匹配，否则只认到半截
    names = sorted(suffix_to_tool, key=len, reverse=True)
    # 左边界不能用 \b：完整名里 invoke 前面是下划线（属于 \w），\b 不成立会漏掉整条调用。
    # 改用「前面不能是 ASCII 字母/数字/下划线」，既放行 mcp__X__invoke，又避免 foo_invoke 误伤
    # （中文等非 ASCII 字符不会被挡，中文紧邻时仍能匹配）。
    pattern = r"(?<![A-Za-z0-9_])(" + "|".join(re.escape(n) for n in names) + r")\s*\(\s*\{"

    calls = []
    for match in re.finditer(pattern, text, re.IGNORECASE):
        tool_name = suffix_to_tool.get(match.group(1).lower())
        if not tool_name:
            continue
        brace_at = text.index("{", match.end() - 1)
        closing = _find_closing(text, brace_at)
        if closing < 0:
            continue
        args = _loads_tolerant_obj(text[brace_at:closing + 1])
        if args is None:
            continue
        calls.append({
            "id": f"call_{uuid.uuid4().hex[:24]}",
            "type": "function",
            "function": {"name": tool_name, "arguments": json.dumps(args, ensure_ascii=False)},
        })
    # 不在这里提前 break：上游常把同一个块输出两遍，截断版会白占一个额度。
    # 先收全、去重，再限数。
    deduped = _dedupe_calls(calls)
    if len(deduped) > TEXTUAL_CALL_MAX:
        # 静默截断过一次就会让客户端少跑一个工具，而日志上看不出任何异常（真机：12→5 后丢第 5 个）
        dropped = [c["function"]["name"] for c in deduped[TEXTUAL_CALL_MAX:]]
        log(f"[tools] 文字风格调用超出单轮上限 {TEXTUAL_CALL_MAX}，"
            f"本轮丢弃 {len(dropped)} 个：{dropped}（下一轮模型会重新请求）")
    return deduped[:TEXTUAL_CALL_MAX] or None


class GLMClient:
    def __init__(self, config: Config, pool: AccountPool) -> None:
        self.config = config
        self.pool = pool

    # ── 对外主入口：排队 → 撞闸退避重试 → 返回 (租约, 响应) ──
    def open_stream(self, messages: list, model: str, networking: bool = False,
                    tools_instructions: str = "", assistant_id: str = "",
                    deep_thinking: bool = False):
        """开一次上游 SSE 生成。

        返回 ``(lease, resp)``：调用方读完响应后必须 ``resp.close()`` 并
        ``lease.release()``（顺序无所谓，但必须有，否则该账号会被永久占住）。

        ``deep_thinking``：本次请求是否要上游「深度思考」（chat_mode=deep_thinking
        + reasoning_effort）。默认关，客户端可按请求覆盖，见 ``extract_deep_thinking``。

        重试策略：
        - 上游返回 JSON 业务错误且命中并发闸特征 → 退避重试（GLM_BUSY_RETRIES 次）
        - 上游 429/5xx → 同样按并发繁忙处理
        - 某账号 refresh_token 彻底失效 → 该账号冷却，换下一个账号试
        """
        body = self.build_body(messages, networking, tools_instructions, assistant_id, model,
                               deep_thinking)
        total = self.config.busy_retries
        last_error: Exception | None = None
        # 排队日志要能认出「是谁占住了账号」——只报等待时长没法归因（真机：主请求被压 4.4s）
        holder_desc = (f"{model} msgs={len(messages)} "
                       f"tools={'有' if tools_instructions else '无'} "
                       f"thinking={'开' if deep_thinking else '关'}")

        for attempt in range(total + 1):
            lease = self.pool.acquire(holder_desc)
            account = lease.account
            try:
                resp = self._request(body, account)
            except (UpstreamBusy, UpstreamAuthError) as exc:
                lease.release()
                last_error = exc
                if isinstance(exc, UpstreamAuthError):
                    account.cooldown(self.config.account_cooldown)
                    log(f"[auth] {account.name} 已冷却 {self.config.account_cooldown:.0f}s"
                        f"（鉴权失效，等待重新获取 token）")
                remaining = [a.name for a in self.pool.candidates() if a is not account]
                if attempt >= total or (isinstance(exc, UpstreamAuthError) and not remaining):
                    break
                delay = min(self.config.busy_backoff * (2 ** attempt), 30.0)
                log(f"[upstream] 第 {attempt + 1} 次失败（{exc}），{delay:.0f}s 后重试")
                time.sleep(delay)
                continue
            except Exception:
                lease.release()
                raise
            return lease, resp

        raise last_error or UpstreamBusy("上游并发繁忙，重试次数已用尽")

    def build_body(self, messages: list, networking: bool = False,
                   tools_instructions: str = "", assistant_id: str = "",
                   model: str = "", deep_thinking: bool = False) -> bytes:
        meta_data = {
            "channel": "",
            "draft_id": "",
            "input_question_type": "xxxx",
            "is_networking": networking,
            "is_test": False,
            "platform": "pc",
            "quote_log_id": "",
            "cogview": {"rm_label_watermark": False},
        }
        # 深度思考：网页真实请求里聊天模式通过 meta_data.chat_mode 下发，
        # 且与 reasoning_effort 成对出现（单独发一半上游不认）。
        # 未显式开深度思考时保持原样（用 .env 里的配置，默认两个空字段）。
        chat_mode = "deep_thinking" if deep_thinking else self.config.chat_mode
        meta_data["chat_mode"] = chat_mode
        # reasoning_effort 与 chat_mode 成对出现；只在有值时才发（不送空字段给上游）。
        # 开了深度思考但 .env 没配 reasoning_effort 时补网页默认的 max；
        # .env 显式配了就以 .env 为准（不覆盖用户自己的设置）。
        # 注意判的是最终的 chat_mode，所以老写法（只配 GLM_CHAT_MODE=deep_thinking）
        # 也同样会补 max —— 网页版这两个字段少一个上游就可能整个忽略。
        effort = self.config.reasoning_effort or (
            "max" if chat_mode == "deep_thinking" else "")
        if effort:
            meta_data["reasoning_effort"] = effort
        # 真实网页请求里，模型选择通过 meta_data.selected_model 下发（如 "glm-5.3"），
        # assistant_id 保持不变。默认把客户端传的 model 名原样透传。
        if self.config.selected_model and model:
            meta_data["selected_model"] = model
        # 真实网页请求不含 if_plus_model，默认保留旧行为以兼容老链路（GLM_IF_PLUS_MODEL=false 可去掉）
        if self.config.if_plus_model:
            meta_data["if_plus_model"] = True
        converted = convert_messages(
            messages, tools_instructions,
            clamp_tools=self.config.clamp_tool_result, clamp_cfg=self.config)
        if self.config.verbose:
            # 把真正发给上游的那段对话（含工具结果回注）dump 出来：模型到底看到了什么，
            # 一眼可查。这是定位「模型误判工具失败」的关键证据。
            try:
                shown = converted[0]["content"][0]["text"]
            except Exception:
                shown = json.dumps(converted, ensure_ascii=False)
            log(f"[req][verbose] 发往上游的对话正文（{len(shown)}字）：\n{shown}")
        return json.dumps(
            {
                "assistant_id": assistant_id or self.config.assistant_id,
                "conversation_id": "",
                "project_id": "",
                "chat_type": "user_chat",
                "messages": converted,
                "meta_data": meta_data,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

    # ── 单次上游请求（不含排队/重试，由 open_stream 统一编排） ──
    def _request(self, body: bytes, account: Account):
        url = f"{self.config.base_url}/backend-api/assistant/stream"
        for attempt in range(2):  # 401/403 → 强制刷 token 再试一次
            token = account.get_access_token(force=attempt > 0)
            req = urllib.request.Request(
                url, data=body, method="POST",
                headers=signed_headers(token, self.config.user_agent),
            )
            try:
                resp = urllib.request.urlopen(req, timeout=self.config.timeout)
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode("utf-8", "ignore")[:300]
                if exc.code in (401, 403):
                    if attempt == 0:
                        log(f"[upstream] {account.name} 鉴权失败，强制刷新 token 后重试")
                        continue
                    raise UpstreamAuthError(f"上游鉴权失败 HTTP {exc.code}: {detail}") from exc
                if exc.code == 429 or exc.code >= 500:
                    raise UpstreamBusy(f"上游 HTTP {exc.code}（限流/服务端繁忙）: {detail}") from exc
                raise RuntimeError(f"上游 HTTP {exc.code}: {detail}") from exc

            # 上游有时不返回 SSE 而是直接给 JSON（业务错误：并发闸 / 未登录 / 参数错）。
            # 若放任它进入 SSE 解析，客户端只会看到「空回答」，必须在此显式分类。
            ctype = (resp.headers.get("Content-Type") or "").lower()
            if "application/json" in ctype:
                payload = read_json(resp)
                resp.close()
                if is_busy_payload(payload):
                    raise UpstreamBusy(
                        f"上游并发闸: {json.dumps(payload, ensure_ascii=False)[:300]}"
                    )
                raise RuntimeError(f"上游业务错误: {json.dumps(payload, ensure_ascii=False)[:400]}")
            return resp

        raise UpstreamAuthError("上游鉴权失败（多次重试仍未通过）")

    def delete_conversation(self, conversation_id: str, account: Account | None = None,
                            assistant_id: str = "") -> None:
        if not self.config.delete_conversation or not conversation_id or account is None:
            return
        body = json.dumps(
            {
                "assistant_id": assistant_id or self.config.assistant_id,
                "conversation_id": conversation_id,
            }
        ).encode("utf-8")
        url = f"{self.config.base_url}/backend-api/assistant/conversation/delete"
        try:
            req = urllib.request.Request(
                url, data=body, method="POST",
                headers={
                    **signed_headers(account.get_access_token(), self.config.user_agent),
                    "Referer": "https://chatglm.cn/main/alltoolsdetail",
                },
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                read_json(resp)
            log(f"[upstream] 已删除会话 {conversation_id}")
        except Exception as exc:
            log(f"[upstream] 删除会话失败（忽略）: {exc}")


# ─────────────────────────── HTTP 服务 ───────────────────────────
def normalize_route(path: str) -> str:
    """把请求路径归一化成内部路由，容忍 GUI 客户端常见的 /v1 拼接差异。

    - ``/models``、``/chat/completions`` → 自动补上 ``/v1``（有些客户端不会补）
    - ``/v1/v1/xxx``（客户端又补了一次）→ 折叠成 ``/v1/xxx``
    - 结尾多余的 ``/`` 和查询串一律忽略（``/v1/models?x=1`` → ``/v1/models``）
    """
    route = path.split("?", 1)[0].split("#", 1)[0].rstrip("/") or "/"
    while route.startswith("/v1/v1"):
        route = route[3:]
    for suffix in ("/models", "/chat/completions", "/messages", "/messages/count_tokens"):
        if route == suffix:
            route = "/v1" + suffix
    return route


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "glm-proxy/0.2"

    # 类属性：跨请求去重，每个未映射的模型名只提示一次
    warned_unmapped_models: set[str] = set()

    # 由 main() 注入
    config: Config = None
    client: GLMClient = None
    _served_model: str = ""     # 上游实际返回的模型（如 moe_53f），用于日志与 system_fingerprint
    _sse_open = False           # SSE 响应头是否已发出（发出后就不能再改状态码，只能补收尾帧）
    _sse_emit = None            # 当前 SSE 流的 emit(delta, finish_reason)，仅 OpenAI 路径

    def log_message(self, fmt, *args):  # 静音默认访问日志
        if self.config and self.config.verbose:
            log("[http]", fmt % args)

    def handle_one_request(self) -> None:
        """客户端粗暴断开（Windows 上常见 ConnectionResetError）不该刷出一堆 traceback。

        这类 RST 发生在「服务端等下一个请求行」时，异常会绕过 do_xxx 的 try 冒到
        socketserver 层并打印堆栈，把正常日志淹掉。这里直接吞掉并关闭连接。
        """
        try:
            super().handle_one_request()
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, TimeoutError):
            self.close_connection = True
            if self.config and self.config.verbose:
                log("[http] 连接被客户端重置，已关闭")

    # ── 工具方法 ──
    def _json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status: int, message: str, err_type: str = "invalid_request_error") -> None:
        self._json(status, {"error": {"message": message, "type": err_type, "code": status}})

    def _authorized(self) -> bool:
        keys = self.config.server_api_keys
        if not keys:
            return True
        auth = self.headers.get("Authorization", "")
        return auth.startswith("Bearer ") and auth[7:].strip() in keys

    def _chunk(self, data: bytes) -> None:
        self.wfile.write(f"{len(data):X}\r\n".encode("ascii") + data + b"\r\n")
        self.wfile.flush()

    def _end_chunks(self) -> None:
        self._sse_open = False
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()

    def _read_json_body(self) -> dict:
        """读并解析请求体（utf-8-sig：顺手容忍 Windows 编辑器留下的 BOM）。"""
        length = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(length).decode("utf-8-sig") or "{}")

    # ── 路由 ──
    def do_GET(self):
        route = normalize_route(self.path)
        if route.startswith("/health"):
            return self._json(200, {"status": "ok"})
        if route.startswith("/v1/models"):
            if not self._authorized():
                return self._error(401, "无效的 API Key", "authentication_error")
            now = int(time.time())
            return self._json(200, {
                "object": "list",
                "data": [
                    {"id": m, "object": "model", "created": now, "owned_by": "zhipu"}
                    for m in self.config.models
                ],
            })
        self._error(404, f"未知路径: {self.path}", "not_found")

    def do_POST(self):
        route = normalize_route(self.path)
        if route.startswith("/v1/messages"):
            # Anthropic Messages API：count_tokens 必须在 /messages 之前判断（前缀更具体）
            if route == "/v1/messages/count_tokens":
                return anthropic_api.handle_count_tokens(self, sys.modules[__name__])
            if route == "/v1/messages":
                return anthropic_api.handle_messages(self, sys.modules[__name__])
            return self._error(404, f"未知路径: {self.path}", "not_found")
        if not route.startswith("/v1/chat/completions"):
            return self._error(404, f"未知路径: {self.path}", "not_found")
        if not self._authorized():
            return self._error(401, "无效的 API Key", "authentication_error")

        try:
            payload = self._read_json_body()
        except Exception as exc:
            return self._error(400, f"请求体不是合法 JSON: {exc}")

        messages = payload.get("messages")
        if not isinstance(messages, list) or not messages:
            return self._error(400, "messages 不能为空")
        model = str(payload.get("model", "glm-4"))
        self._served_model = ""   # 每个请求独立，避免 keep-alive 复用实例时串味
        want_stream = bool(payload.get("stream"))
        networking = extract_networking(payload, self.config.networking)
        # 深度思考：单次请求可覆盖全局默认（GLM_DEEP_THINKING），见 extract_deep_thinking
        deep_thinking = extract_deep_thinking(payload, self.config.deep_thinking)

        # 网页版接口没有原生 function calling：开启 GLM_PROMPT_TOOL_CALLING 后，
        # 由本代理用「提示词描述工具 + 解析模型 JSON 输出」模拟出 OpenAI 的 tool_calls，
        # 工具真正由客户端执行（客户端拿到 tool_calls 后执行，再把 role:"tool" 结果发回来）。
        tool_defs = extract_tool_definitions(payload)
        tools = tool_defs if self.config.prompt_tool_calling else []
        tools_instructions = (render_tools_prompt(tools, self.config.tools_prompt_max_chars)
                              if tools else "")
        if tool_defs and not tools:
            log(f"[compat] 请求带 {len(tool_defs)} 个工具定义，但 GLM_PROMPT_TOOL_CALLING=false，已忽略")
        elif tools:
            log(f"[tools] 已用提示词模拟 {len(tools)} 个工具："
                f"{', '.join(t['name'] for t in tools)}")
        elif has_function_tools(payload):
            log("[compat] 请求带 tools，但没有解析出可用的 function 定义（缺 name？），已忽略")
        if self.config.verbose:
            log(f"[req] model={model} stream={want_stream} msgs={len(messages)} "
                f"tools={len(tool_defs)} networking={networking} "
                f"prompt={len(tools_instructions)}字 "
                f"deep_thinking={deep_thinking}")

        # 模型名 → 上游 assistant_id 映射（GLM_MODEL_ASSISTANT_MAP）
        assistant_id = self.config.model_assistant_map.get(model.lower(), "")
        if assistant_id and self.config.verbose:
            log(f"[model] {model} → assistant_id={assistant_id}（来自 GLM_MODEL_ASSISTANT_MAP）")
        elif not assistant_id and model.lower() not in self.warned_unmapped_models:
            # 没映射时「换模型名」是静默 no-op：客户端以为切了 glm-4.6，实际还是同一个 assistant。
            # 每个模型名只提醒一次，多了只会淹掉真问题。
            self.warned_unmapped_models.add(model.lower())
            log(f"[model] {model} 未配置 assistant_id 映射，本次仍走默认 assistant_id"
                f"（{self.config.assistant_id or '访客'}）——客户端选的模型名不会生效；"
                f"上游实际模型见 [upstream] 行，要真切换请配 GLM_MODEL_ASSISTANT_MAP")

        # 排队 + 撞闸退避重试都在 open_stream 内部完成，这里只负责把异常映射成 HTTP 状态码
        try:
            lease, resp = self.client.open_stream(
                messages, model, networking, tools_instructions, assistant_id,
                deep_thinking=deep_thinking,
            )
        except QueueTimeout as exc:
            return self._error(503, f"本地排队超时：{exc}", "queue_timeout")
        except UpstreamBusy as exc:
            return self._error(503, f"上游生成繁忙（已重试）：{exc}", "upstream_busy")
        except UpstreamAuthError as exc:
            return self._error(401, f"上游鉴权失败: {exc}", "authentication_error")
        except Exception as exc:
            return self._error(502, f"上游请求失败: {exc}", "upstream_error")

        try:
            if tools:
                self._tool_aware_response(
                    resp, model, want_stream, tools, lease.account, assistant_id,
                    messages=messages, networking=networking,
                    tools_instructions=tools_instructions,
                    release_lease=lease.release, deep_thinking=deep_thinking)
            elif want_stream:
                self._stream_response(resp, model, lease.account, assistant_id)
            else:
                self._blocking_response(resp, model, lease.account, assistant_id)
        except (BrokenPipeError, ConnectionResetError):
            log("[http] 客户端提前断开")
        except Exception as exc:
            log(f"[http] 处理响应出错: {exc}")
            # 流已经开了就只能补收尾帧；还没写响应头才发得出 502
            if not self._abort_stream() and not self.headers_sent:
                self._error(502, f"响应处理失败: {exc}", "upstream_error")
        finally:
            try:
                resp.close()
            except Exception:
                pass
            lease.release()  # 关键：流读完才把账号槽位还回去

    # ── 响应体 ──
    def _begin_sse_stream(self, model: str):
        """发 SSE 响应头，返回 emit(delta, finish_reason) 出口。"""
        conv_id = f"chatcmpl-{uuid.uuid4().hex}"
        created = int(time.time())

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        self.close_connection = True

        def emit(delta: dict, finish=None) -> None:
            chunk = {
                "id": conv_id, "object": "chat.completion.chunk",
                "created": created, "model": model,
                "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
            }
            if self._served_model:      # 上游实际模型（如 moe_53f），拿到后随帧带回
                chunk["system_fingerprint"] = self._served_model
            self._chunk(f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n".encode("utf-8"))

        self._sse_open = True
        self._sse_emit = emit
        return emit

    def _abort_stream(self) -> bool:
        """SSE 头已发出之后才出错：补一帧收尾，别让客户端挂在没有结束的流上。

        响应头已经写出去了，此时既改不了状态码也不能再发 JSON 错误体，
        唯一正确的做法是给一个 ``finish_reason`` + ``[DONE]`` 并结束分块编码。
        返回是否真的收尾了（没开流则返回 False，由调用方走正常错误响应）。
        """
        if not self._sse_open:
            return False
        self._sse_open = False
        try:
            if self._sse_emit is not None:
                self._sse_emit({}, "stop")
                self._chunk(b"data: [DONE]\n\n")
            self._end_chunks()
        except Exception:               # 收尾写不出去（客户端已断开）只能作罢
            pass
        return True

    def _consume_all(self, resp, on_reasoning=None) -> "StreamAccumulator":
        """把上游流一次性读完（工具模式必须先拿到完整输出才能判断是不是工具调用）。

        ``on_reasoning(delta)`` 是思维链增量的回调。工具模式下**正文必须整段缓冲**
        （否则拿到完整输出才发现是工具调用，前面吐出去的正文就收不回来了），
        但**思维链可以先流给客户端**：否则客户端看到的是「思考 0.1 秒 + 答案一次到齐」，
        那 0.1 秒只是两个 chunk 的时间差，真实的思考过程被缓冲整个藏掉了。
        思维链先到、正文后到，也正好是模型的真实生成顺序。
        """
        acc = StreamAccumulator()
        for event in iter_sse_events(resp):
            _, reason_delta = acc.consume(event)
            if reason_delta and on_reasoning is not None:
                on_reasoning(reason_delta)
            if str(event.get("status")) in ("finish", "intervene"):
                break
        _, reason_delta = acc.finalize()   # 上游没推 finish 就断流时，把尾巴补齐
        if reason_delta and on_reasoning is not None:
            on_reasoning(reason_delta)
        return acc

    def _completion_payload(self, acc: "StreamAccumulator", model: str,
                            text_override: str = None) -> dict:
        text = acc.full_text() if text_override is None else text_override
        return {
            "id": acc.conversation_id or f"chatcmpl-{uuid.uuid4().hex}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "system_fingerprint": acc.served_model or None,  # 上游实际模型，如 moe_53f
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": text,
                    "reasoning_content": acc.full_reasoning() or None,
                },
                "finish_reason": "stop",
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        }

    def _tool_calls_payload(self, calls: list, model: str, conv_id: str = "",
                            fingerprint: str = "") -> dict:
        return {
            "id": conv_id or f"chatcmpl-{uuid.uuid4().hex}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "system_fingerprint": fingerprint or None,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": None, "tool_calls": calls},
                "finish_reason": "tool_calls",
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        }

    def _stream_response(self, resp, model: str, account: "Account" = None,
                         assistant_id: str = "") -> None:
        emit = self._begin_sse_stream(model)
        acc = StreamAccumulator()

        role_sent = False

        def feed(text_delta: str, reason_delta: str) -> None:
            nonlocal role_sent
            self._served_model = acc.served_model      # 供 system_fingerprint / 日志
            if reason_delta:
                emit({"reasoning_content": reason_delta})
            if text_delta:
                if not role_sent:
                    emit({"role": "assistant", "content": text_delta})
                    role_sent = True
                else:
                    emit({"content": text_delta})

        try:
            for event in iter_sse_events(resp):
                feed(*acc.consume(event))
                if str(event.get("status")) in ("finish", "intervene"):
                    break
            feed(*acc.finalize())   # 上游未推 finish 就断流时，尾巴仍要吐给客户端

            if not role_sent:
                emit({"role": "assistant", "content": ""})
            emit({}, "stop")
            self._chunk(b"data: [DONE]\n\n")
            self._end_chunks()
        finally:
            self._log_served_model()
            if acc.conversation_id:
                threading.Thread(
                    target=self.client.delete_conversation,
                    args=(acc.conversation_id, account, assistant_id), daemon=True,
                ).start()

    def _log_served_model(self) -> None:
        """verbose 下打印上游实际模型，便于确认「你选的模型名」到底走了谁。"""
        if self.config and self.config.verbose and self._served_model:
            log(f"[upstream] 实际模型 = {self._served_model}")

    def _blocking_response(self, resp, model: str, account: "Account" = None,
                           assistant_id: str = "") -> None:
        acc = self._consume_all(resp)
        self._served_model = acc.served_model
        self._json(200, self._completion_payload(acc, model))
        self._log_served_model()
        if acc.conversation_id:
            threading.Thread(
                target=self.client.delete_conversation,
                args=(acc.conversation_id, account, assistant_id), daemon=True,
            ).start()

    # ── 工具模式（GLM_PROMPT_TOOL_CALLING=true） ──
    def _tool_aware_response(self, resp, model: str, want_stream: bool,
                             tools: list, account: "Account" = None,
                             assistant_id: str = "", messages: list = None,
                             networking: bool = False,
                             tools_instructions: str = "",
                             release_lease=None,
                             deep_thinking: bool = False) -> None:
        """先缓冲正文，再决定回「工具调用」还是「普通回答」。

        必须缓冲**正文**：只有拿到完整输出才知道模型是在调用工具还是在正常说话。
        但**思维链不用等**：流式请求下思维链随到随发（``on_reasoning``），客户端于是看到
        「先思考、后回答」的真实节奏，而不是「思考 0.1 秒 + 答案一次到齐」的假象。

        另外内置「思考泄漏」自愈：模型把打算干什么的碎碎念当正文吐出来就结束时，
        自动带上一次输出续问，让它真正给出工具调用或正面回答（见 ``looks_like_think_leak``）。
        """
        allowed = {tool["name"] for tool in tools}
        tries = self.config.tool_continue_tries if messages else 0
        attempt = 0
        extra_lease = None   # 续问请求占用的租约，读完必须归还

        # 流式请求：SSE 头先发出去，思维链增量随到随发（正文仍整段缓冲）。
        # 续问时这条流继续复用——客户端先看到第一轮思维链，再看到续问后的思维链与正文。
        emit = self._begin_sse_stream(model) if want_stream else None
        reasoning_streamed = False

        def stream_reasoning(delta: str) -> None:
            nonlocal reasoning_streamed
            if emit is None or not delta:
                return
            reasoning_streamed = True
            emit({"reasoning_content": delta})

        while True:
            acc = self._consume_all(resp, on_reasoning=stream_reasoning if emit else None)
            self._served_model = acc.served_model
            self._log_served_model()
            text = join_answer_parts(
                acc.part_texts(), self.config.strip_process_narration)
            calls = parse_tool_calls(text, allowed)
            if not calls:
                # 兜底：客户端自带的工具说明（如 Cherry 的 list/inspect/invoke/exec）通常更强势，
                # 模型会用 JS 风格写调用，这里翻译成标准 tool_calls，否则整条 MCP 链路都白搭。
                calls = parse_textual_tool_calls(text, allowed)
                if calls:
                    log(f"[tools] 文字风格调用已翻译为 tool_calls："
                        f"{[c['function']['name'] for c in calls]}")

            # 没解析出调用，但有两种「必须重试」的成因：
            #   ① 上游抢跑 —— 模型把该回客户端的调用写成了文本，被上游自己的
            #      function-call 层拦下执行（实测 mcp__CherryHub__exec / invoke）。
            #      客户端的工具压根没跑，模型收到的是上游沙箱里的失败结果，
            #      于是得出「搜索接口异常」的结论并给一个残缺回答。
            #      名字被上游吞成散列的（blind）也算，见 unattributed_hijacks。
            #   ② 思考泄漏 —— 输出像是泄漏的思考，把碎碎念当答案返回了。
            # 注意 ② 要同时喂正文和思维链：形态 B 下正文为空、碎碎念只在思维链里。
            reason = acc.full_reasoning()
            hijacked = hijacked_client_tools(acc.platform_tools, allowed)
            blind = unattributed_hijacks(acc.platform_tools, allowed)
            if not calls and attempt < tries and (
                    hijacked or blind or looks_like_think_leak(text, reason)):
                attempt += 1
                leaked = (text or reason).strip()
                if hijacked or blind:
                    detail = (f"；上游拿到的结果：{acc.platform_results[-1][:120]!r}"
                              if acc.platform_results else "")
                    named = (f"客户端工具 {hijacked}" if hijacked
                             else f"一次调用（上游把工具名吞成了 {blind}）")
                    log(f"[tools] 上游抢跑：{named} 被上游自己执行了，"
                        f"客户端从未收到这次调用（第 {attempt}/{tries} 次续问）{detail}")
                elif leaked:
                    log(f"[tools] 输出疑似泄漏的思考（无回答、无调用），自动续问"
                        f"（第 {attempt}/{tries} 次，正文{len(text or '')}字/"
                        f"思维链{len(reason)}字）。开头：{leaked[:100]!r}")
                else:
                    # 形态 C：模型什么都没输出（正文与思维链皆空）
                    log(f"[tools] 模型未输出任何内容（正文与思维链皆空），自动续问"
                        f"（第 {attempt}/{tries} 次）")
                if acc.conversation_id:
                    threading.Thread(
                        target=self.client.delete_conversation,
                        args=(acc.conversation_id, account, assistant_id), daemon=True,
                    ).start()
                resp.close()
                # 关键：同一账号同时只允许一个生成，续问前必须先把主租约还回去，
                # 否则续问请求会排队等一个自己占着的槽位 → 必然排队超时。
                # 归还后原响应已读完、不会再用到这个租约，由新租约接管。
                if release_lease is not None:
                    release_lease()
                new_resp, new_lease = self._continue_after_think_leak(
                    messages, leaked, model, networking, tools_instructions,
                    assistant_id, account, deep_thinking, hijacked=hijacked,
                    blind_hijack=bool(blind))
                if new_resp is None:
                    break   # 续问失败：把这一轮原样返回，好过丢掉已有内容
                resp, extra_lease = new_resp, new_lease
                continue

            if not calls:
                if not text.strip():
                    # 正文为空（无论思维链有没有内容）：续问次数用尽仍没给出结论。
                    # 原样返回空内容会让用户看到「空气」，改成明确说明。
                    if reason.strip():
                        log("[tools] 续问后正文仍为空，返回明确提示而不是裸思维链")
                    else:
                        log("[tools] 续问后模型仍无任何输出，返回明确提示")
                    text = THINK_LEAK_FALLBACK
                else:
                    if hijacked or blind:
                        # 抢跑且续问次数用尽：这不是「模型选择直接回答」，
                        # 而是客户端工具根本没执行、答案建立在上游沙箱的失败结果上。
                        named = (f"客户端工具 {hijacked}" if hijacked
                                 else f"调用（工具名被上游吞成了 {blind}）")
                        log(f"[tools] 上游抢跑且续问已用尽：{named} 未执行，"
                            f"本轮答案可能缺少实时数据。正文{len(text)}字。")
                        self._diagnose_tool_miss(text, reason, allowed)
                    else:
                        # ⚠ 这里**不是**异常：模型主动选择用文字回答（协议允许，
                        # finish_reason=stop）。早先统一打成「模型没按工具协议输出」，
                        # 会让正常回答看起来像故障 —— 实测 10 次里 8 次是正常回答。
                        head = text[:160].replace("\n", " ")
                        log(f"[tools] 模型选择直接回答（未调用工具），按普通回答返回。"
                            f"正文{len(text)}字。开头：{head!r}")
                        # 诊断只在「看起来确实想调用却没调出来」时打，
                        # 否则正常回答也会刷一堆无意义的诊断行。
                        if looks_like_wanted_tool_call(text, reason, allowed):
                            self._diagnose_tool_miss(text, reason, allowed)
            # 本轮若持有续问租约（已达续问上限或续问失败），立刻交回去：
            # 否则账号槽位会被占住，后续请求只能排队到超时。
            if extra_lease is not None:
                try:
                    resp.close()
                except Exception:
                    pass
                extra_lease.release()
                extra_lease = None
            break

        try:
            if calls:
                log(f"[tools] 模型请求调用 {[c['function']['name'] for c in calls]}")
                if want_stream:
                    self._stream_tool_calls(calls, model, emit)
                else:
                    self._json(200, self._tool_calls_payload(
                        calls, model, acc.conversation_id, acc.served_model))
            elif want_stream:
                self._stream_plain_text(text, acc.full_reasoning(), model, emit,
                                        reasoning_streamed=reasoning_streamed)
            else:
                self._json(200, self._completion_payload(acc, model, text_override=text))
        finally:
            if acc.conversation_id:
                threading.Thread(
                    target=self.client.delete_conversation,
                    args=(acc.conversation_id, account, assistant_id), daemon=True,
                ).start()
            # 归还续问占用的账号槽位（主请求的租约由调用方释放；若续问成功，
            # 这里归还的是新租约，主租约已在续问前归还过，release 是幂等的）
            if extra_lease is not None:
                try:
                    resp.close()
                except Exception:
                    pass
                extra_lease.release()

    def _continue_after_think_leak(self, messages: list, leaked: str, model: str,
                                   networking: bool, tools_instructions: str,
                                   assistant_id: str, account: "Account" = None,
                                   deep_thinking: bool = False,
                                   hijacked: list[str] | None = None,
                                   blind_hijack: bool = False):
        """续问：把上一轮的问题输出当成 assistant 说过的话，再要求它给出可用结果。

        ``hijacked`` 非空时是「上游抢跑」场景 —— 该回客户端的工具被上游自己执行了，
        光要求「给结论」没用（它已经在上游沙箱里试过并失败了），必须要求它
        **改用文字形态重新发起调用**，把调用交回客户端执行。
        只有 ``blind_hijack`` 时上游连工具名都没回传（见 unattributed_hijacks），
        只能笼统要求它把上一条打算做的调用重来一遍。

        返回 ``(resp, lease)``；失败返回 ``(None, None)``（调用方会把上一轮原样返回）。
        租约交给调用方释放，与主请求同一套生命周期，不会把账号永久占住。
        """
        if hijacked or blind_hijack:
            nudge = (
                "你刚才那几个工具调用被网页端自己执行了，而它们是本地客户端注册的工具，"
                "必须由客户端执行才能拿到真实数据。现在请重新发起这些调用，并且"
                "**只输出协议规定的文字形态调用行**，形如：\n"
                '工具名({"参数名": 参数值})\n'
                "行首直接是工具名，前面不要加方括号标记或任何前缀词；"
                "不要输出 JSON 对象（{" + '"tool_calls"' + ": ...} 那种），"
                "不要使用网页端自带的搜索/浏览工具，也不要只描述你打算做什么。"
            )
            nudge += (f"\n需要重新调用的工具：{', '.join(hijacked)}" if hijacked else
                      "\n上一条里被你发出的那几个调用，客户端一个都没收到 —— "
                      "请照着你的计划把它们逐个重新发起（参数也一样），不要少一个。")
            if leaked:
                nudge += f"\n\n你上一条的内容是：{leaked}"
        elif leaked:
            nudge = (
                "你上一条只写下了自己的打算，没有真正给出工具调用，也没有回答用户的问题。"
                "现在不要再描述计划：请直接调用工具，或者直接给出面向用户的最终回答。"
                "如果工具确实查不到数据，就明确告诉用户查不到，不要继续尝试别的途径。\n\n"
                f"你上一条的内容是：{leaked}"
            )
        else:
            # 模型什么都没输出（正文与思维链皆空）：不能说成「你上一条写了什么」，
            # 直接要求它基于已有工具结果给出结论。
            nudge = (
                "你上一条没有输出任何内容。工具结果已经在上下文里了，"
                "请现在直接给出面向用户的最终回答：把查到的数据（如客流数字、日期、来源）"
                "整理成一段回答；如果确实没有可用数据，就明确说明查不到以及建议的替代渠道。"
                "不要再调用工具，也不要只描述你的计划。"
            )
            log("[tools] 续问提示：模型上一轮完全无输出，要求它直接给结论")
        follow_up = list(messages) + [
            {"role": "user", "content": nudge},
        ]
        if leaked:
            # 有内容才回灌成 assistant 说过的话，避免凭空多出一段无效对话
            follow_up.insert(len(messages), {"role": "assistant", "content": leaked})
        try:
            lease, resp = self.client.open_stream(
                follow_up, model, networking, tools_instructions, assistant_id,
                deep_thinking=deep_thinking)
        except Exception as exc:
            log(f"[tools] 续问失败（{exc}），返回上一轮内容")
            return None, None
        return resp, lease

    def _diagnose_tool_miss(self, text: str, reasoning: str,
                            tool_names: set[str]) -> None:
        """工具模式解析失败时，补打诊断信息定位「为什么没触发调用」。

        三种常见成因靠这几行就能区分：
          * 模型主动转文本 → 正文无 `{`，也没有类似工具名的词
          * 想发调用但 JSON 畸形 → 正文含 `{`/`tool_calls`，解析却没成功
          * 思维链混进正文 → reasoning 里出现工具名，正文是它的「碎碎念」
        正文/思维链都做长度截断，避免刷屏。
        """
        body = text or ""
        has_brace = "{" in body
        has_marker = "tool_calls" in body or "invoke(" in body or "arguments" in body
        # 正文里是否出现任一注册工具的短名（如 invoke / exec），用于判断模型是否「想调用」
        short = {n.rsplit("__", 1)[-1].rsplit(".", 1)[-1].lower() for n in tool_names}
        hit = sorted({s for s in short if s and s in body.lower()})
        reason_len = len(reasoning or "")
        reason_toolname = sorted({s for s in short if s and s in (reasoning or "").lower()})
        log(f"[tools][诊断] 正文长度={len(body)} 含大括号={has_brace} "
            f"含tool_calls/arguments/invoke={has_marker} 正文命中工具短名={hit} "
            f"思维链长度={reason_len} 思维链命中工具短名={reason_toolname}")
        # 解析失败时**必须记完整正文**（单行转义），否则只留 160-600 字的话
        # 根本无法判断 JSON 到底缺在哪 —— 实测就被这个截断坑过好几轮。
        if body:
            flat = body.replace("\r", "\\r").replace("\n", "\\n")
            if len(flat) > 2000:
                flat = flat[:2000] + f"…(共{len(body)}字，已截断)"
            log(f"[tools][诊断] 完整正文：{flat}")
        if reasoning:
            head = (reasoning or "")[:240].replace("\n", " ")
            log(f"[tools][诊断] 思维链开头：{head!r}")

    def _stream_plain_text(self, text: str, reasoning: str, model: str, emit=None,
                           reasoning_streamed: bool = False) -> None:
        """工具模式下没触发工具调用时，把缓冲好的整段文本按 SSE 一次性吐出去。

        ``emit`` 由调用方传入（思维链已经在用同一条流增量发出），``reasoning_streamed``
        表示思维链是否已经逐段发过——发过就不要再补一段全量的，否则客户端会看到重复。
        正文仍然是一帧给全：工具模式下无法逐字流（要先判断是不是工具调用）。
        """
        emit = emit or self._begin_sse_stream(model)
        if reasoning and not reasoning_streamed:
            emit({"role": "assistant", "content": None, "reasoning_content": reasoning})
        emit({"role": "assistant", "content": text})
        emit({}, "stop")
        self._chunk(b"data: [DONE]\n\n")
        self._end_chunks()

    def _stream_tool_calls(self, calls: list, model: str, emit=None) -> None:
        emit = emit or self._begin_sse_stream(model)
        for index, call in enumerate(calls):
            delta_call = {
                "index": index,
                "id": call["id"],
                "type": "function",
                "function": call["function"],
            }
            if index == 0:
                emit({"role": "assistant", "content": None, "tool_calls": [delta_call]})
            else:
                emit({"tool_calls": [delta_call]})
        emit({}, "tool_calls")
        self._chunk(b"data: [DONE]\n\n")
        self._end_chunks()


# ─────────────────────────── 启动 ───────────────────────────
def load_dotenv(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key.strip(), value)


def build_accounts(config: Config, store: TokenStore) -> list[Account]:
    """按配置组装账号列表（多账号用逗号分隔）。没有可用 token 时退化为单个游客账号。"""
    if config.force_guest:
        log("[auth] GLM_USE_GUEST=true，强制游客模式")
        return [Account(config, "游客", "", store)]

    accounts: list[Account] = []
    seen: set[str] = set()
    for index, seed in enumerate(config.refresh_tokens, 1):
        account = Account(config, f"账号{index}", seed, store)
        if account.refresh_token in seen:  # .env 与落盘文件里同时存在的重复项
            continue
        seen.add(account.refresh_token)
        accounts.append(account)

    if not accounts:
        if store.enabled and os.path.exists(store.path):
            log(f"[auth] .env 未配置 GLM_REFRESH_TOKEN，且 {store.path} 里没有可用账号 → 游客模式")
        else:
            log("[auth] 未配置 GLM_REFRESH_TOKEN(S)，走游客模式（能力受限）")
        return [Account(config, "游客", "", store)]
    return accounts


def main() -> int:
    # Windows 终端默认 GBK，输出中文日志可能抛 UnicodeEncodeError；尽力切到 UTF-8。
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    parser = argparse.ArgumentParser(description="chatglm.cn 最小反代")
    parser.add_argument("--env", default=".env", help="dotenv 文件路径（默认 .env）")
    parser.add_argument("--host", default="", help="监听地址（覆盖 .env 里的 HOST，默认 127.0.0.1）")
    parser.add_argument("--port", type=int, default=0, help="监听端口（覆盖 .env 里的 PORT，默认 8000）")
    parser.add_argument("--log-file", default="", help="把日志额外以 UTF-8 追加写入该文件")
    args = parser.parse_args()
    if args.log_file:
        _LOG_SINKS.append(args.log_file)
    load_dotenv(args.env)

    # 命令行优先级最高：写入环境变量后再构造 Config（load_dotenv 用 setdefault，不会反向覆盖）
    if args.host:
        os.environ["HOST"] = args.host
    if args.port:
        os.environ["PORT"] = str(args.port)

    config = Config()
    store = TokenStore(config.token_file, config.persist_tokens)
    accounts = build_accounts(config, store)
    pool = AccountPool(config, accounts)
    Handler.config = config
    Handler.client = GLMClient(config, pool)

    mode = "游客模式" if all(a.is_guest for a in accounts) else f"账号模式（{len(accounts)} 个账号）"

    log(f"启动 {config.host}:{config.port} | {mode} | assistant_id={config.assistant_id}")
    if all(a.is_guest for a in accounts):
        log("提示：未配置 GLM_REFRESH_TOKEN(S)，将走游客模式（能力受限）")
    log(
        f"并发策略：每账号串行生成，账号间并行 | 排队上限 {config.queue_timeout:.0f}s "
        f"| 撞闸重试 {config.busy_retries} 次"
    )
    if config.prompt_tool_calling:
        log("工具调用：提示词模拟已开启（客户端 tools 会被转成标准 tool_calls 返回）")
    if config.clamp_tool_result:
        log(f"工具结果限体积：单条≤{config.tool_result_max_chars}字 / "
            f"网页类≤{config.web_result_max_chars}字 / 累计≤{config.tool_result_total_max_chars}字"
            f"（GLM_CLAMP_TOOL_RESULT=false 关闭）")
    else:
        log("工具结果限体积：已关闭，工具结果按原文回灌（GLM_CLAMP_TOOL_RESULT=true 可开启）")
    if config.prompt_tool_calling:
        budget = config.tools_prompt_max_chars
        log(f"工具定义预算：{'不裁剪' if budget <= 0 else f'{budget} 字（超出部分仅列名字）'}"
            f"（GLM_TOOLS_PROMPT_MAX_CHARS）")
    else:
        log("工具调用：未开启（GLM_PROMPT_TOOL_CALLING=true 可开启；客户端发来的 tools 会被忽略）")
    if config.networking:
        log("联网搜索：默认开启")
    if config.persist_tokens:
        log(f"refresh_token 落盘：{config.token_file}（GLM_PERSIST_TOKENS=false 可关闭）")
    log(f"OpenAI 兼容地址: http://{config.host}:{config.port}/v1")
    if _LOG_SINKS:
        log(f"日志文件（UTF-8）：{os.path.abspath(_LOG_SINKS[0])}")
    log(f"模型清单（仅展示用，不影响上游）：{', '.join(config.models)}")
    if config.selected_model:
        log("模型选择：已开启 meta_data.selected_model（把客户端 model 名透传上游，"
            "能否真正切换看 system_fingerprint / [upstream] 实际模型）")
    else:
        log("模型选择：未开启 meta_data.selected_model（GLM_SELECTED_MODEL=false）")
    if config.chat_mode or config.reasoning_effort:
        log(f"思考模式：chat_mode={config.chat_mode or '(空)'} "
            f"reasoning_effort={config.reasoning_effort or '(空)'}")
    if config.deep_thinking:
        log("深度思考：默认开启（请求体 {\"glm\":{\"deep_thinking\":false}} 可单次关掉）")
    else:
        log("深度思考：默认关闭（全局 GLM_DEEP_THINKING=true，或请求体 "
            "{\"glm\":{\"deep_thinking\":true}} 单次开启）")
    if config.model_assistant_map:
        pairs = ", ".join(f"{k}→{v}" for k, v in sorted(config.model_assistant_map.items()))
        log(f"模型→assistant_id 映射已启用：{pairs}")
    else:
        log(f"提示：未配置 GLM_MODEL_ASSISTANT_MAP，所有模型名都走同一个 assistant_id"
            f"（{config.assistant_id}）")

    server = ThreadingHTTPServer((config.host, config.port), Handler)
    server.daemon_threads = True
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log("收到中断，退出")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Anthropic Messages API（``/v1/messages``）适配层。

把 glm_proxy 的 chatglm.cn 反代再包一层 Claude 格式，让 Claude Code / Cline /
各类 Anthropic SDK 客户端可以直接把本代理当成 ``https://api.anthropic.com`` 使用。

设计要点
--------
* **只做协议翻译**：Anthropic 请求 → OpenAI 风格 ``messages``/``tools`` → 交给
  ``glm_proxy.GLMClient.open_stream``，再把上游输出渲染回 Anthropic 的
  ``message`` 对象 / SSE 事件。
* **不 import glm_proxy**：``python glm_proxy.py`` 运行时模块名是 ``__main__``，
  再 ``import glm_proxy`` 会加载出第二份模块，异常类（``UpstreamBusy`` 等）身份不一致，
  ``except`` 会全部失效。需要的少量对象由调用方以 ``glm`` 参数注入。
* **上游没有原生 function calling**：工具调用沿用 glm_proxy 的「提示词 + JSON 解析」
  模拟方案。因为必须拿到完整输出才能判断「是工具调用还是普通回答」，带 ``tools``
  的请求会先缓冲整段再回流——这是协议限制，不是 bug。
* **thinking / 深度思考**：请求带 ``thinking``（且未 ``disabled``）时，除了把上游思维链输出成
  ``thinking`` block（流式为 ``thinking_delta``），还会让上游切到**深度思考**档
  （``chat_mode=deep_thinking`` + ``reasoning_effort=max``）——Anthropic 客户端发
  ``thinking`` 本意就是要模型多想。上游不提供真正的 signature，本代理给的是占位值；
  若客户端严格校验 signature 而报错，设 ``ANTHROPIC_EMIT_THINKING=false`` 关掉
  （此时思维链被丢弃，也不请求深度思考）。OpenAI 侧同理支持按请求开关，
  见 ``glm_proxy.extract_deep_thinking``。
"""

from __future__ import annotations

import base64
import json
import os
import threading
import time
import uuid

# 与 glm_proxy 保持一致的粗略估算：中文场景下 1 token ≈ 2~4 字节，取保守值。
_CHARS_PER_TOKEN = 3


# ─────────────────────────── 入口 ───────────────────────────
def handle_messages(handler, glm) -> None:
    """处理 ``POST /v1/messages``（流式 + 非流式）。"""
    try:
        payload = handler._read_json_body()
    except Exception as exc:
        return _send_error(handler, 400, "invalid_request_error", f"请求体不是合法 JSON: {exc}")
    if not authorized(handler):
        return _send_error(handler, 401, "authentication_error", "无效的 API Key")

    messages = payload.get("messages")
    if not isinstance(messages, list) or not messages:
        return _send_error(handler, 400, "invalid_request_error", "messages 不能为空")

    model = str(payload.get("model") or "glm-4")
    stream = bool(payload.get("stream"))
    want_thinking = _want_thinking(payload)
    # 深度思考：Anthropic 客户端发 thinking 参数就等于要上游深度思考；
    # 同时认 OpenAI 侧的 glm.deep_thinking 写法。全局默认走 GLM_DEEP_THINKING。
    deep_thinking = want_thinking or glm.extract_deep_thinking(
        payload, handler.config.deep_thinking)
    tools = normalize_tools(payload.get("tools"))
    stop_sequences = _stop_sequences(payload.get("stop_sequences"))
    openai_messages = to_openai_messages(payload.get("system"), messages)
    tools_instructions = glm.render_tools_prompt(tools) if tools else ""
    assistant_id = handler.config.model_assistant_map.get(model.lower(), "")
    input_tokens = estimate_input_tokens(openai_messages, tools_instructions)
    networking = _networking(payload, handler.config.networking)

    if handler.config.verbose:
        glm.log(f"[anthropic] model={model} stream={stream} msgs={len(messages)} "
                f"tools={len(tools)} thinking={want_thinking} "
                f"deep_thinking={deep_thinking}")

    try:
        lease, resp = handler.client.open_stream(
            openai_messages, model, networking, tools_instructions, assistant_id,
            deep_thinking=deep_thinking,
        )
    except glm.QueueTimeout as exc:
        return _send_error(handler, 503, "overloaded_error", f"本地排队超时：{exc}")
    except glm.UpstreamBusy as exc:
        return _send_error(handler, 503, "overloaded_error", f"上游生成繁忙（已重试）：{exc}")
    except glm.UpstreamAuthError as exc:
        return _send_error(handler, 401, "authentication_error", f"上游鉴权失败: {exc}")
    except Exception as exc:
        return _send_error(handler, 502, "api_error", f"上游请求失败: {exc}")

    try:
        if tools or stop_sequences:
            # 工具模式（判断是不是工具调用）与 stop_sequences（本地截断）都必须先拿到完整输出
            acc = _consume_all(glm, resp)
            _render_buffered(handler, glm, acc, model, stream, tools, want_thinking,
                             input_tokens, lease.account, assistant_id, stop_sequences)
        elif stream:
            _render_stream(handler, glm, resp, model, want_thinking, input_tokens,
                           lease.account, assistant_id)
        else:
            acc = _consume_all(glm, resp)
            _render_buffered(handler, glm, acc, model, False, [], want_thinking,
                             input_tokens, lease.account, assistant_id, stop_sequences)
    except (BrokenPipeError, ConnectionResetError):
        glm.log("[http] 客户端提前断开")
    except Exception as exc:
        glm.log(f"[http] 处理响应出错: {exc}")
    finally:
        try:
            resp.close()
        except Exception:
            pass
        lease.release()


def handle_count_tokens(handler, glm) -> None:
    """处理 ``POST /v1/messages/count_tokens``（Claude Code 会调用它做上下文估算）。"""
    try:
        payload = handler._read_json_body()
    except Exception as exc:
        return _send_error(handler, 400, "invalid_request_error", f"请求体不是合法 JSON: {exc}")
    if not authorized(handler):
        return _send_error(handler, 401, "authentication_error", "无效的 API Key")
    if not isinstance(payload.get("messages"), list) or not payload["messages"]:
        return _send_error(handler, 400, "invalid_request_error", "messages 不能为空")

    tools = normalize_tools(payload.get("tools"))
    openai_messages = to_openai_messages(payload.get("system"), payload["messages"])
    extra = glm.render_tools_prompt(tools) if tools else ""
    handler._json(200, {"input_tokens": estimate_input_tokens(openai_messages, extra)})


def authorized(handler) -> bool:
    """鉴权：复用 SERVER_API_KEYS，同时接受 Anthropic 惯用的 ``x-api-key`` 头。"""
    keys = handler.config.server_api_keys
    if not keys:
        return True
    api_key = handler.headers.get("x-api-key", "").strip()
    if api_key and api_key in keys:
        return True
    return handler._authorized()


# ─────────────────────────── 请求转换 ───────────────────────────
def to_openai_messages(system, messages: list) -> list:
    """Anthropic 的 system + messages → OpenAI 风格的 messages。

    生成的结构仍会由 glm_proxy.convert_messages 拍平成单条 user 文本，这里只需要
    保持语义正确（system / tool_result / assistant.tool_calls 都不丢）。
    """
    result: list = []
    for text in _system_texts(system):
        result.append({"role": "system", "content": text})
    for message in messages or []:
        if not isinstance(message, dict):
            continue
        role = message.get("role")
        content = message.get("content")
        if role == "user":
            result.extend(_user_message(content))
        elif role == "assistant":
            result.append(_assistant_message(content))
    return result


def _system_texts(system) -> list:
    if isinstance(system, str):
        return [system] if system else []
    texts = []
    if isinstance(system, list):
        for block in system:
            if isinstance(block, dict) and block.get("type") == "text":
                texts.append(str(block.get("text", "")))
            elif isinstance(block, str):
                texts.append(block)
    return [t for t in texts if t]


def _user_message(content) -> list:
    """user 消息可能同时含 tool_result 和普通文本/图片，拆成 OpenAI 的 tool + user 两条。"""
    if isinstance(content, str):
        return [{"role": "user", "content": content}] if content else []

    tool_messages, parts = [], []
    for block in content or []:
        if not isinstance(block, dict):
            continue
        kind = block.get("type")
        if kind == "text":
            parts.append({"type": "text", "text": str(block.get("text", ""))})
        elif kind == "image":
            url = _image_url(block.get("source"))
            if url:
                parts.append({"type": "image_url", "image_url": {"url": url}})
        elif kind == "tool_result":
            tool_messages.append({
                "role": "tool",
                "tool_call_id": str(block.get("tool_use_id") or ""),
                "content": _tool_result_text(block.get("content")),
            })
    result = tool_messages
    if parts:
        result.append({"role": "user", "content": parts})
    return result


def _assistant_message(content) -> dict:
    if isinstance(content, str):
        return {"role": "assistant", "content": content}

    texts, calls = [], []
    for block in content or []:
        if not isinstance(block, dict):
            continue
        kind = block.get("type")
        if kind == "text":
            texts.append(str(block.get("text", "")))
        elif kind == "tool_use":
            calls.append({
                "id": str(block.get("id") or ""),
                "type": "function",
                "function": {
                    "name": str(block.get("name") or ""),
                    "arguments": json.dumps(block.get("input") or {}, ensure_ascii=False),
                },
            })
        # thinking block 是历史思维链，回传时忽略（上游不需要）
    message = {"role": "assistant", "content": "\n".join(t for t in texts if t)}
    if calls:
        message["tool_calls"] = calls
    return message


def _tool_result_text(content) -> str:
    if isinstance(content, str):
        return content
    texts = []
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                texts.append(str(block.get("text", "")))
            elif isinstance(block, str):
                texts.append(block)
    return "\n".join(x for x in texts if x)


def _image_url(source) -> str:
    if not isinstance(source, dict):
        return ""
    if source.get("type") == "base64" and source.get("data"):
        media = source.get("media_type") or "image/png"
        return f"data:{media};base64,{source['data']}"
    return str(source.get("url") or "")


def normalize_tools(tools) -> list:
    """Anthropic tools（input_schema）→ glm_proxy 内部工具描述（parameters）。"""
    result = []
    if not isinstance(tools, list):
        return result
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        name = str(tool.get("name") or "").strip()
        if not name:  # 跳过 web_search 之类服务端工具（type 不为自定义工具）
            continue
        schema = tool.get("input_schema")
        result.append({
            "name": name,
            "description": str(tool.get("description") or "").strip(),
            "parameters": schema if isinstance(schema, dict) else {},
        })
    return result


def _networking(payload: dict, default: bool = False) -> bool:
    tools = payload.get("tools")
    if isinstance(tools, list):
        for tool in tools:
            if isinstance(tool, dict) and "web_search" in str(tool.get("type", "")).lower():
                return True
    return default


def _stop_sequences(raw) -> list:
    if isinstance(raw, str):        # Anthropic 的 stop_sequences 是数组；宽容接受单串
        raw = [raw]
    if not isinstance(raw, list):
        return []
    return [s for s in raw if isinstance(s, str) and s]


def _truncate_at_stop(text: str, stop_sequences: list) -> tuple[str, str | None]:
    """在第一个命中的 stop_sequence 处截断（该串本身从正文里去掉）。

    上游网页版接口不支持 stop_sequences，只能在拿到完整输出后本地裁剪。
    返回 ``(截断后文本, 命中的串)``；没有命中则返回 ``(原文本, None)``。
    """
    if not stop_sequences or not text:
        return text, None
    best_index, hit = -1, None
    for seq in stop_sequences:
        index = text.find(seq)
        if index >= 0 and (best_index < 0 or index < best_index):
            best_index, hit = index, seq
    if hit is None:
        return text, None
    return text[:best_index], hit


def _want_thinking(payload: dict) -> bool:
    if not _env_bool("ANTHROPIC_EMIT_THINKING", True):
        return False
    thinking = payload.get("thinking")
    if not isinstance(thinking, dict):
        return False
    return str(thinking.get("type", "")).lower() not in ("", "disabled")


# ─────────────────────────── 响应渲染 ───────────────────────────
def _consume_all(glm, resp):
    acc = glm.StreamAccumulator()
    for event in glm.iter_sse_events(resp):
        acc.consume(event)
        if str(event.get("status")) in ("finish", "intervene"):
            break
    return acc


def _render_buffered(handler, glm, acc, model, stream: bool, tools: list,
                     want_thinking: bool, input_tokens: int, account, assistant_id: str,
                     stop_sequences: list | None = None) -> None:
    """把缓冲好的完整输出渲染成 Anthropic message —— 工具结果或普通回答都走这里。"""
    text = acc.full_text()
    reasoning = acc.full_reasoning() if want_thinking else ""

    calls = None
    if tools:
        allowed = {tool["name"] for tool in tools}
        calls = glm.parse_tool_calls(text, allowed)
        if not calls:
            calls = glm.parse_textual_tool_calls(text, allowed)
            if calls:
                glm.log("[tools] 文字风格调用已翻译为 tool_use："
                        f"{[c['function']['name'] for c in calls]}")

    stop_reason, stop_sequence = "end_turn", None
    blocks: list = []
    if reasoning:
        blocks.append({"type": "thinking", "thinking": reasoning, "signature": _signature()})

    if calls:
        glm.log(f"[tools] 模型请求调用 {[c['function']['name'] for c in calls]}")
        for call in calls:
            blocks.append({
                "type": "tool_use",
                "id": _toolu_id(),
                "name": call["function"]["name"],
                "input": _loads_arguments(call["function"].get("arguments")),
            })
        stop_reason = "tool_use"
    else:
        if tools and text:
            head = text[:160].replace("\n", " ")
            glm.log(f"[tools] 模型没按工具协议输出，按普通回答返回。输出开头：{head!r}")
        text, stop_sequence = _truncate_at_stop(text, stop_sequences)
        if stop_sequence is not None:
            stop_reason = "stop_sequence"
            glm.log(f"[anthropic] 命中 stop_sequences，已在本地截断（{stop_sequence!r}）")
        blocks.append({"type": "text", "text": text})

    output_tokens = estimate_tokens(text)
    message_id = _new_message_id()

    if not stream:
        handler._json(200, {
            "id": message_id,
            "type": "message",
            "role": "assistant",
            "model": model,
            "content": blocks,
            "stop_reason": stop_reason,
            "stop_sequence": stop_sequence,
            "usage": {"input_tokens": input_tokens, "output_tokens": output_tokens},
        })
    else:
        emit = _begin_stream(handler, message_id, model, input_tokens)
        for index, block in enumerate(blocks):
            emit("content_block_start", {
                "type": "content_block_start", "index": index,
                "content_block": _block_start(block),
            })
            for delta in _block_deltas(block):
                emit("content_block_delta", {
                    "type": "content_block_delta", "index": index, "delta": delta,
                })
            emit("content_block_stop", {"type": "content_block_stop", "index": index})
        emit("message_delta", {
            "type": "message_delta",
            "delta": {"stop_reason": stop_reason, "stop_sequence": stop_sequence},
            "usage": {"output_tokens": output_tokens},
        })
        emit("message_stop", {"type": "message_stop"})
        handler._end_chunks()

    _delete_conversation(glm, handler, acc, account, assistant_id)


def _render_stream(handler, glm, resp, model: str, want_thinking: bool,
                   input_tokens: int, account, assistant_id: str) -> None:
    """无工具的逐字流式：上游 delta 直接映射成 text/thinking block 的增量事件。"""
    message_id = _new_message_id()
    emit = _begin_stream(handler, message_id, model, input_tokens)
    acc = glm.StreamAccumulator()

    next_index = 0
    think_index = None
    think_closed = False
    text_index = None

    def feed(text_delta: str, reason_delta: str) -> None:
        nonlocal next_index, think_index, think_closed, text_index
        if want_thinking and reason_delta and text_index is None:
            if think_index is None:
                think_index = next_index
                next_index += 1
                emit("content_block_start", {
                    "type": "content_block_start", "index": think_index,
                    "content_block": {"type": "thinking", "thinking": ""},
                })
            emit("content_block_delta", {
                "type": "content_block_delta", "index": think_index,
                "delta": {"type": "thinking_delta", "thinking": reason_delta},
            })
        if text_delta:
            if text_index is None:
                think_closed = _close_thinking(emit, think_index, think_closed)
                text_index = next_index
                next_index += 1
                emit("content_block_start", {
                    "type": "content_block_start", "index": text_index,
                    "content_block": {"type": "text", "text": ""},
                })
            emit("content_block_delta", {
                "type": "content_block_delta", "index": text_index,
                "delta": {"type": "text_delta", "text": text_delta},
            })

    try:
        for event in glm.iter_sse_events(resp):
            feed(*acc.consume(event))
            if str(event.get("status")) in ("finish", "intervene"):
                break
        feed(*acc.finalize())   # 上游未推 finish 就断流时，尾巴仍要吐给客户端

        think_closed = _close_thinking(emit, think_index, think_closed)
        if text_index is None:
            emit("content_block_start", {
                "type": "content_block_start", "index": next_index,
                "content_block": {"type": "text", "text": ""},
            })
            emit("content_block_stop", {"type": "content_block_stop", "index": next_index})
        else:
            emit("content_block_stop", {"type": "content_block_stop", "index": text_index})

        emit("message_delta", {
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn", "stop_sequence": None},
            "usage": {"output_tokens": estimate_tokens(acc.full_text())},
        })
        emit("message_stop", {"type": "message_stop"})
        handler._end_chunks()
    finally:
        _delete_conversation(glm, handler, acc, account, assistant_id)


def _close_thinking(emit, think_index, think_closed: bool) -> bool:
    if think_index is None or think_closed:
        return think_closed
    emit("content_block_delta", {
        "type": "content_block_delta", "index": think_index,
        "delta": {"type": "signature_delta", "signature": _signature()},
    })
    emit("content_block_stop", {"type": "content_block_stop", "index": think_index})
    return True


def _begin_stream(handler, message_id: str, model: str, input_tokens: int):
    """发 Anthropic SSE 响应头 + message_start + ping，返回 emit(event_type, data)。"""
    handler.send_response(200)
    handler.send_header("Content-Type", "text/event-stream; charset=utf-8")
    handler.send_header("Cache-Control", "no-cache")
    handler.send_header("Connection", "close")
    handler.send_header("Transfer-Encoding", "chunked")
    handler.end_headers()
    handler.close_connection = True

    def emit(event_type: str, data: dict) -> None:
        handler._chunk(_sse(event_type, data))

    emit("message_start", {
        "type": "message_start",
        "message": {
            "id": message_id, "type": "message", "role": "assistant", "model": model,
            "content": [], "stop_reason": None, "stop_sequence": None,
            "usage": {"input_tokens": input_tokens, "output_tokens": 0},
        },
    })
    emit("ping", {"type": "ping"})
    return emit


def _sse(event_type: str, data: dict) -> bytes:
    body = json.dumps(data, ensure_ascii=False)
    return f"event: {event_type}\ndata: {body}\n\n".encode("utf-8")


def _block_start(block: dict) -> dict:
    if block["type"] == "text":
        return {"type": "text", "text": ""}
    if block["type"] == "thinking":
        return {"type": "thinking", "thinking": ""}
    return {"type": "tool_use", "id": block["id"], "name": block["name"], "input": {}}


def _block_deltas(block: dict) -> list:
    if block["type"] == "text":
        return [{"type": "text_delta", "text": block["text"]}]
    if block["type"] == "thinking":
        return [
            {"type": "thinking_delta", "thinking": block["thinking"]},
            {"type": "signature_delta", "signature": block["signature"]},
        ]
    return [{"type": "input_json_delta",
             "partial_json": json.dumps(block["input"], ensure_ascii=False)}]


def _loads_arguments(raw) -> dict:
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(raw or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def _delete_conversation(glm, handler, acc, account, assistant_id: str) -> None:
    if not acc.conversation_id:
        return
    threading.Thread(
        target=handler.client.delete_conversation,
        args=(acc.conversation_id, account, assistant_id), daemon=True,
    ).start()


# ─────────────────────────── 工具函数 ───────────────────────────
def estimate_tokens(text: str) -> int:
    return max(1, len(text or "") // _CHARS_PER_TOKEN)


def estimate_input_tokens(messages: list, extra: str = "") -> int:
    try:
        blob = json.dumps(messages, ensure_ascii=False) + (extra or "")
    except (TypeError, ValueError):
        blob = str(messages)
    return estimate_tokens(blob)


def _new_message_id() -> str:
    return "msg_" + uuid.uuid4().hex[:24]


def _toolu_id() -> str:
    return "toolu_" + uuid.uuid4().hex[:24]


def _signature() -> str:
    # 上游不提供真正的 thinking signature，这里给一个占位；见模块 docstring 的开关说明
    return base64.b64encode(os.urandom(48)).decode("ascii")


def _send_error(handler, status: int, err_type: str, message: str) -> None:
    handler._json(status, {"type": "error", "error": {"type": err_type, "message": message}})


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")

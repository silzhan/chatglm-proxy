#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""token 估算 —— 上游网页版接口不返回任何用量信息，只能本地算。

为什么需要它：``/v1/chat/completions`` 与 ``/v1/messages`` 都要回 ``usage``，
``/v1/messages/count_tokens`` 更是专门给客户端做上下文预算用的，而智谱网页版
接口的 SSE 事件里**没有任何 token 计数字段**（全量翻过 server.log，零命中）。
所以这里全是估算值，不是上游精确计数 —— 对外暴露时请知悉这一点。

设计取向：**宁可高估，不可低估**。

    * 低估 → 客户端（Claude Code / Cline）不触发上下文压缩，一路捅到上游
      context 上限，报出来的是整个会话废掉的错；
    * 高估 → 客户端早压缩一次，代价小得多。

所以所有函数都向上取整，英文侧系数取保守值，最后再乘一个 MARGIN。

估算口径（按 GLM tokenizer 的常见表现校准，非精确）：

    * 中文（CJK）约 1 token ≈ 1~1.5 字，这里按 1 字 = 1 token（估高）
    * 英文/数字/符号约 4 字符 ≈ 1 token，这里按 3.3 字符 = 1 token（估高）
    * 每条 message 另有结构开销（role、分隔符、JSON 括号），约 4 token
    * 图片等内容块无法按字符算，按块给固定占位值

想更准可以自行接真 tokenizer（GLM-4 的 tokenizer.json + HuggingFace tokenizers），
在保持本模块函数签名不变的前提下替换实现即可。
"""

from __future__ import annotations

import json
import math
import re

# 每条 message 的结构开销。OpenAI 官方口径约 4，这里取同值。
MESSAGE_OVERHEAD = 4

# 图片等内容块：没法按字符估，按块给一个固定占位值。
BLOCK_OVERHEAD = 1024

# 上游自己注入的系统提示。客户端看不到、也估算不到这部分，
# 用一个常量补偿，避免 count_tokens 明显偏低导致客户端不压缩上下文。
DEFAULT_SYSTEM_OVERHEAD = 200

# 整体 margin（1.05 = 估高 5%）。配合「宁可高估」的取向。
MARGIN = 1.05

# CJK + 日文假名 + 韩文音节。中文按「字」计，这些字符的 token 密度远高于拉丁文本。
_CJK_RE = re.compile(
    "[\u2e80-\u2eff\u2f00-\u2fdf\u3040-\u30ff\u3400-\u4dbf"
    "\u4e00-\u9fff\uf900-\ufaff\uac00-\ud7af]"
)

# message 里除 content 外同样会占 token 的字段（原样发给上游的）
_EXTRA_MESSAGE_FIELDS = ("name", "tool_call_id")


def estimate_text_tokens(text) -> int:
    """估一段文本的 token 数。None/空串返回 0，非字符串先 str()。"""
    if text is None:
        return 0
    if not isinstance(text, str):
        text = str(text)
    if not text:
        return 0
    cjk = len(_CJK_RE.findall(text))
    other = len(text) - cjk
    return math.ceil(cjk + other / 3.3)


def _content_text(content) -> tuple[str, int]:
    """把 ``message.content`` 拍成 ``(文本, 非文本块数)``。

    兼容三种形态：纯字符串、Anthropic 的内容块数组、以及数组里混字符串。
    """
    if content is None:
        return "", 0
    if isinstance(content, str):
        return content, 0
    if isinstance(content, list):
        chunks: list[str] = []
        blocks = 0
        for item in content:
            if isinstance(item, str):
                chunks.append(item)
            elif isinstance(item, dict):
                if isinstance(item.get("text"), str):
                    chunks.append(item["text"])
                elif item.get("type") in ("image", "image_url", "input_image", "document"):
                    blocks += 1
                else:
                    # tool_use / tool_result / 未知块：整体序列化后按字符算
                    chunks.append(json.dumps(item, ensure_ascii=False, default=str))
            else:
                chunks.append(str(item))
        return "".join(chunks), blocks
    if isinstance(content, dict):
        return _content_text([content])
    return str(content), 0


def _estimate_message(message) -> int:
    """估单条 message 的 token 数（含结构开销）。"""
    if not isinstance(message, dict):
        return MESSAGE_OVERHEAD + estimate_text_tokens(message)

    total = MESSAGE_OVERHEAD
    for field in _EXTRA_MESSAGE_FIELDS:
        value = message.get(field)
        if value:
            total += estimate_text_tokens(value)

    text, blocks = _content_text(message.get("content"))
    total += blocks * BLOCK_OVERHEAD + estimate_text_tokens(text)

    # assistant 消息回传的 tool_calls：agent 循环里每轮都会原样带回，是真占 token 的
    tool_calls = message.get("tool_calls")
    if tool_calls:
        total += estimate_text_tokens(
            json.dumps(tool_calls, ensure_ascii=False, default=str))

    return total


def estimate_messages_tokens(messages, extra: str = "",
                             system_overhead: int = DEFAULT_SYSTEM_OVERHEAD) -> int:
    """估一整段对话的输入 token 数。

    :param messages: OpenAI 形态的 messages（也可以是 Anthropic 转好之后的）
    :param extra: 额外拼进提示词的文本（工具定义渲染结果）
    :param system_overhead: 上游自注入系统提示的补偿值
    """
    total = 0
    for message in messages or []:
        total += _estimate_message(message)
    total += estimate_text_tokens(extra)
    total += max(0, int(system_overhead))
    return math.ceil(total * MARGIN)


def estimate_output_tokens(text: str = "", reasoning: str = "") -> int:
    """估输出 token 数。思维链同样是模型生成的内容，要一起算。"""
    return estimate_text_tokens(text) + estimate_text_tokens(reasoning)

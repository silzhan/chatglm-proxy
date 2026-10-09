#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""用量累计统计 —— 按天聚合，节流落盘。

单次请求的 token 数算完就写进响应的 ``usage`` 字段，不落盘；这里存的是**累计值**，
回答的是「今天烧了多少、哪个账号烧得凶、上下文是不是快爆了」这类问题。

存储结构（``.glm_usage.json``，与 ``.glm_tokens.json`` 同目录同待遇）::

    {
      "version": 1,
      "day": "2026-10-09",
      "days": {
        "2026-10-09": {
          "requests": 128, "input_tokens": 412000, "output_tokens": 96000,
          "reasoning_tokens": 21000, "tool_calls": 340, "elapsed_ms": 512000,
          "by_model": {"glm-4": {"requests": 100, "input_tokens": 300000, ...}},
          "by_account": {"账号1": {"requests": 90, "input_tokens": 280000, ...}}
        }
      }
    }

设计约束：

    * **单进程假设**。本代理是单进程 ThreadingHTTPServer，内存聚合 + 文件快照
      足够。若将来开多 worker，文件读-改-写会 race，需要改成每进程写自己的文件
      再合并，或者直接换 sqlite。
    * **落盘节流**。每个请求只改内存；距上次写超过 flush_interval 才真正写盘，
      外加进程退出时兜一次（main 里注册 atexit）。崩溃最多丢一个间隔的数据，
      对观测性数据可接受。
    * **原子写**。复用 TokenStore 的 ``tmp + os.replace``，断电不会留下半个 JSON。
    * **按日期字符串做 key**，不写「零点重置」逻辑；跨天时把当天归档进 days，
      只保留最近 keep_days 天。
"""

from __future__ import annotations

import copy
import json
import os
import threading
import time

import token_estimate

USAGE_VERSION = 1

# 一天一个 bucket 里累计的字段（by_model / by_account 用同一套）
_COUNTER_FIELDS = (
    "requests", "input_tokens", "output_tokens",
    "reasoning_tokens", "tool_calls", "elapsed_ms",
)


def _today() -> str:
    return time.strftime("%Y-%m-%d")


def _new_bucket() -> dict:
    bucket = {field: 0 for field in _COUNTER_FIELDS}
    bucket["by_model"] = {}
    bucket["by_account"] = {}
    return bucket


def _add(bucket: dict, *, requests=0, input_tokens=0, output_tokens=0,
         reasoning_tokens=0, tool_calls=0, elapsed_ms=0) -> None:
    """把一条记录累加进 bucket（by_model / by_account 用同一套计数器）。"""
    bucket["requests"] = bucket.get("requests", 0) + requests
    bucket["input_tokens"] = bucket.get("input_tokens", 0) + input_tokens
    bucket["output_tokens"] = bucket.get("output_tokens", 0) + output_tokens
    bucket["reasoning_tokens"] = bucket.get("reasoning_tokens", 0) + reasoning_tokens
    bucket["tool_calls"] = bucket.get("tool_calls", 0) + tool_calls
    bucket["elapsed_ms"] = bucket.get("elapsed_ms", 0) + elapsed_ms


def _sub_add(container: dict, key: str, **kwargs) -> None:
    """按 (模型/账号) 维度累加。key 为空时归到 ``(未标注)``，不丢数据。"""
    if not key:
        key = "(未标注)"
    bucket = container.setdefault(key, {field: 0 for field in _COUNTER_FIELDS})
    _add(bucket, **kwargs)


def openai_usage(prompt_tokens: int, text: str = "", reasoning: str = "") -> dict:
    """拼 OpenAI 形态的 usage 块。"""
    completion = token_estimate.estimate_output_tokens(text, reasoning)
    prompt = max(0, int(prompt_tokens))
    return {
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": prompt + completion,
    }


def usage_line(*, api: str, model: str, account: str, prompt_tokens: int,
               output_tokens: int, reasoning_tokens: int = 0, tool_calls: int = 0,
               elapsed: float = 0.0, today: dict = None) -> str:
    """拼一行人类可读的用量日志（server.log 里 grep ``[usage]`` 就是逐条明细）。"""
    parts = [
        f"[usage] api={api}",
        f"model={model or '(未标注)'}",
        f"account={account or '(未标注)'}",
        f"in={prompt_tokens}",
        f"out={output_tokens}",
    ]
    if reasoning_tokens:
        parts.append(f"思考{reasoning_tokens}")
    if tool_calls:
        parts.append(f"tools={tool_calls}")
    if elapsed > 0:
        parts.append(f"{elapsed:.1f}s")
    line = " ".join(parts)
    if today:
        line += (f" | 今日 {today.get('requests', 0)}次 "
                 f"in={today.get('input_tokens', 0)} "
                 f"out={today.get('output_tokens', 0)}")
    return line


def record_request(stats, *, api: str, model: str, account: str = "",
                   served_model: str = "", stream: bool = False,
                   prompt_tokens: int = 0, output_tokens: int = 0,
                   reasoning_tokens: int = 0, tool_calls: int = 0,
                   elapsed: float = 0.0, log=None) -> None:
    """记一条用量：内存聚合（stats 为 None 则跳过）+ 打一行日志。

    ``log`` 由调用方传入（glm_proxy.log / anthropic_api 里的 glm.log）——
    usage_stats 是被两边共同依赖的底层模块，反向 import glm_proxy 会成环。
    """
    today = None
    if stats is not None:
        stats.record(
            api=api, model=model, account=account, served_model=served_model,
            stream=stream, prompt_tokens=prompt_tokens, output_tokens=output_tokens,
            reasoning_tokens=reasoning_tokens, tool_calls=tool_calls,
            elapsed_ms=int(elapsed * 1000),
        )
        today = stats.today()
    if log is not None:
        log(usage_line(
            api=api, model=model, account=account,
            prompt_tokens=prompt_tokens, output_tokens=output_tokens,
            reasoning_tokens=reasoning_tokens, tool_calls=tool_calls,
            elapsed=elapsed, today=today,
        ))


class UsageStats:
    """按天聚合的用量统计，带节流落盘。

    ``enabled=False`` 或 ``path`` 为空时退化成纯内存对象（测试用），不碰磁盘。
    """

    def __init__(self, path: str = "", enabled: bool = True,
                 flush_interval: float = 60.0, keep_days: int = 7) -> None:
        self.path = path
        self.enabled = bool(enabled and path)
        self.flush_interval = max(1.0, float(flush_interval or 60.0))
        self.keep_days = max(1, int(keep_days or 7))
        self._lock = threading.Lock()
        self._day = _today()
        self._current = _new_bucket()
        self._days: dict[str, dict] = {}
        self._dirty = False
        self._last_flush = 0.0
        self._load()

    # ── 读 ──
    def _load(self) -> None:
        if not self.enabled or not os.path.exists(self.path):
            return
        try:
            with open(self.path, encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception:
            # 统计文件损坏不该影响服务：当没有历史处理
            return
        if not isinstance(data, dict):
            return
        days = data.get("days")
        if isinstance(days, dict):
            self._days = {str(k): v for k, v in days.items() if isinstance(v, dict)}
        # 当天那部分要接回来，否则重启一次今天的累计就归零
        if str(data.get("day") or "") == self._day:
            current = self._days.pop(self._day, None)
            if isinstance(current, dict):
                self._current = current
        self._prune()

    def _prune(self) -> None:
        """只保留最近 keep_days 天的归档（不含今天，今天在 _current 里）。"""
        keys = sorted(self._days)
        overflow = len(keys) - self.keep_days
        for key in keys[:overflow] if overflow > 0 else []:
            self._days.pop(key, None)

    # ── 写 ──
    def record(self, *, api: str, model: str, account: str = "",
               served_model: str = "", stream: bool = False,
               prompt_tokens: int = 0, output_tokens: int = 0,
               reasoning_tokens: int = 0, tool_calls: int = 0,
               elapsed_ms: int = 0) -> None:
        with self._lock:
            self._roll()
            counters = dict(
                requests=1,
                input_tokens=max(0, int(prompt_tokens)),
                output_tokens=max(0, int(output_tokens)),
                reasoning_tokens=max(0, int(reasoning_tokens)),
                tool_calls=max(0, int(tool_calls)),
                elapsed_ms=max(0, int(elapsed_ms)),
            )
            _add(self._current, **counters)
            _sub_add(self._current["by_model"], model or served_model, **counters)
            _sub_add(self._current["by_account"], account, **counters)
            self._dirty = True
            self.maybe_flush()

    def _roll(self) -> None:
        """跨天：把当天归档，开一个新 bucket。调用方需已持有锁。"""
        today = _today()
        if today == self._day:
            return
        self._days[self._day] = self._current
        self._day = today
        self._current = _new_bucket()
        self._prune()

    def maybe_flush(self) -> None:
        """节流落盘。调用方需已持有锁。"""
        if not self.enabled or not self._dirty:
            return
        now = time.time()
        if now - self._last_flush < self.flush_interval:
            return
        self._save()

    def flush(self, force: bool = True) -> None:
        """立即落盘（进程退出时调用）。"""
        with self._lock:
            if not self.enabled:
                return
            if force or self._dirty:
                self._save()

    def _save(self) -> None:
        """原子写：tmp + os.replace。调用方需已持有锁。"""
        if not self.enabled:
            return
        payload = {
            "version": USAGE_VERSION,
            "day": self._day,
            "days": {**self._days, self._day: self._current},
        }
        tmp = self.path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2)
            os.replace(tmp, self.path)
            self._dirty = False
            self._last_flush = time.time()
        except OSError:
            # 写不出去不能影响服务：保留 dirty，下次再试
            pass

    # ── 查询 ──
    def today(self) -> dict:
        with self._lock:
            self._roll()
            return {"day": self._day, **copy.deepcopy(self._current)}

    def snapshot(self) -> dict:
        with self._lock:
            self._roll()
            return {
                "version": USAGE_VERSION,
                "day": self._day,
                "today": copy.deepcopy(self._current),
                "days": {k: copy.deepcopy(v) for k, v in sorted(self._days.items())},
                "estimated": True,
            }

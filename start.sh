#!/usr/bin/env bash
# chatglm-proxy 启动脚本（Linux / macOS / Git Bash）
# 用法：./start.sh [--port 9000] [--host 0.0.0.0] [--env .env.prod]
# 参数会原样透传给 glm_proxy.py。日志同时打印到屏幕并写入 server.log / server.err。
set -uo pipefail
cd "$(dirname "$0")"

# ── 1. 找 Python ──
if command -v python >/dev/null 2>&1; then
    PY=python
elif command -v python3 >/dev/null 2>&1; then
    PY=python3
else
    echo "[错误] 未找到 python / python3，请先安装 Python 3.6+。" >&2
    exit 1
fi

# ── 2. 缺 .env 就从 .env.example 复制 ──
if [ ! -f .env ]; then
    if [ -f .env.example ]; then
        cp .env.example .env
        echo "[提示] 未找到 .env，已从 .env.example 复制一份。"
        echo "       请打开 .env 填入 GLM_REFRESH_TOKEN（留空则走游客模式，能力受限）。"
    else
        echo "[警告] 未找到 .env 且没有 .env.example，将以默认配置启动。"
    fi
fi

# ── 3. 前台启动 ──
# 日志由 python 自己以 UTF-8 写 server.log（--log-file），终端仍然实时可见。
echo "[启动] Ctrl+C 退出；日志写入 server.log / server.err"
exec "$PY" glm_proxy.py --log-file server.log "$@" 2> >(tee server.err >&2)

@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

rem chatglm-proxy launcher (Windows)
rem usage: start.bat [--port 9000] [--host 0.0.0.0] [--env .env.prod]
rem args are passed through to glm_proxy.py.
rem logs go to the screen and to server.log / server.err.

where python >nul 2>nul
if errorlevel 1 goto no_python

if exist ".env" goto run
if not exist ".env.example" goto run

copy /y ".env.example" ".env" >nul
echo [提示] 未找到 .env，已从 .env.example 复制一份。
echo        请打开 .env 填入 GLM_REFRESH_TOKEN（留空则走游客模式，能力受限）。

:run
echo [启动] Ctrl+C 退出；日志写入 server.log / server.err
rem 日志由 python 自己以 UTF-8 写 server.log（--log-file）。
rem 不要再用 PowerShell 的 Tee-Object 落盘：它默认写 UTF-16，中文全是乱码。
python "glm_proxy.py" --log-file "server.log" %* 2>"server.err"
set "EXITCODE=%ERRORLEVEL%"
echo [退出] 进程结束，退出码 %EXITCODE%
endlocal & exit /b %EXITCODE%

:no_python
echo [错误] 未找到 python，请先安装 Python 3.6+ 并加入 PATH。
pause
endlocal & exit /b 1

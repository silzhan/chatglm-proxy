# chatglm-proxy

把 **chatglm.cn（智谱清言）网页版接口** 反代成 OpenAI 兼容接口的最小实现。

- **纯 Python 标准库**，零第三方依赖，主程序约 1000 行，代码完全可读
- 支持 `POST /v1/chat/completions`（流式 + 非流式）、`GET /v1/models`、`GET /health`
  （自动兼容客户端对 `/v1` 的拼接差异：`/models`、`/v1/v1/models` 等都能用）
- **同时支持 Anthropic Messages API**：`POST /v1/messages`（流式 + 非流式 + tools + thinking）
  与 `POST /v1/messages/count_tokens`，Claude Code / Cline / Anthropic SDK 可直接接入
- 签名算法逆向自官方桌面客户端，与线上一致
- **并发治理**：按账号串行排队 + 撞并发闸指数退避重试（不再把「请等待其他对话生成完毕」甩给客户端）
- **多账号**：`GLM_REFRESH_TOKENS` 逗号分隔，账号间并行、失效自动轮换、全挂可退回游客
- **token 自愈**：上游轮换的 `refresh_token` 自动落盘，重启不再用回旧值
- **工具调用**：默认开启，用提示词 + JSON 解析把客户端 `tools` 模拟成标准 OpenAI `tool_calls`
  （网页版接口无原生 function calling，这是模拟方案，详见「四、使用」）
- **v0.3 工具链稳定性**：思考泄漏自动续问（3 种形态）+ 工具结果限体积（防上下文撑爆）
- **日志是 UTF-8**：由 Python 直接写盘，不再出现 PowerShell `Tee-Object` 的 UTF-16 乱码
- 自带离线自测（假上游 + 真 HTTP 端到端），`python -m unittest discover -s tests`

> ⚠️ **仅供学习与本地自用测试。** 该做法违反智谱服务条款，请勿用于对外提供服务或商业用途。

## 一、原理

### 1. 智谱清言有两条路

| 路径 | 说明 |
|---|---|
| **开放平台** `open.bigmodel.cn` | 官方 API，要付费、给 key，正经用法 |
| **网页版** `chatglm.cn` | 无公开 API，靠抓网页请求「白嫖」，本项目走这条 |

本项目反代的是**网页版**。桌面客户端 `智谱清言.exe` 也只是个 Electron 壳，内部直接连 `https://chatglm.cn/chatglm/...`，并没有本地服务可供复用。

### 2. 关键：请求签名

网页版每个请求都要带三个头，否则被拒：

```
X-Timestamp: 毫秒时间戳，但「倒数第 2 位」被替换成校验位
X-Nonce:     uuid4 hex
X-Sign:      md5(f"{timestamp}-{nonce}-{SIGN_SECRET}")
```

`SIGN_SECRET` = `8a1317a7468aa3ad86e997d08f3f31cb`。

时间戳的校验位算法（`build_sign()` 已 1:1 复刻）：

```python
now = str(int(time.time() * 1000))
digits = [int(ch) for ch in now]
checksum = (sum(digits) - digits[-2]) % 10
timestamp = now[:-2] + str(checksum) + now[-1]
```

> 这段与客户端 `app.asar` 里 `src/main/auth-headers.js` 的 `getXTimestamp()` / `getSign()` 逐位等价。

### 3. 登录态

```
refresh_token  --POST /chatglm/user-api/user/refresh-->  access_token (约 1 小时)
```

- 有 `refresh_token` → 账号模式
- 没有 → `POST /chatglm/user-api/guest/access` 拿游客 token（能力受限，且游客 token 会过期需重取）

上游实际调用的接口（本项目用到）：

| 用途 | 方法 | 路径 |
|---|---|---|
| 刷新 access_token | POST | `/chatglm/user-api/user/refresh` |
| 游客 token | POST | `/chatglm/user-api/guest/access` |
| **对话（SSE 流）** | POST | `/chatglm/backend-api/assistant/stream` |
| 删除会话 | POST | `/chatglm/backend-api/assistant/conversation/delete` |

## 二、快速开始

### 1. 准备（无需 pip install）

只需要 Python 3.6+。

```bash
cd chatglm-proxy
cp .env.example .env
```

### 2. 拿 refresh_token

**方式 A — 浏览器（推荐）**

1. 打开 https://chatglm.cn 并登录
2. `F12` → `Application`（应用）→ `Local Storage` → `https://chatglm.cn`
3. 找到 `chatglm_refresh_token`，复制其值

**方式 B — 复用桌面客户端登录态**

客户端登录后 token 在 `%APPDATA%\chatglm\Network\Cookies`（SQLite 数据库），用 DB Browser 之类打开，查 `chatglm_refresh_token` 行。

### 3. 填配置

```env
GLM_REFRESH_TOKEN=你复制到的值
```

> 不填也能跑（自动游客模式），但能力受限、无历史记录。

### 4. 启动

```bash
python glm_proxy.py
```

或者用启动脚本（自动检查 Python、缺 `.env` 就从 `.env.example` 复制、日志留档到 `server.log`/`server.err`）：

```bat
:: Windows
start.bat
:: 传参透传给 glm_proxy.py，例如换端口
start.bat --port 9000
```

```bash
# Linux / macOS / Git Bash
./start.sh
./start.sh --port 9000 --host 0.0.0.0 --env .env.prod
```

`glm_proxy.py` 支持的命令行参数：

| 参数 | 说明 |
|---|---|
| `--env <path>` | dotenv 文件路径（默认 `.env`） |
| `--host <addr>` | 监听地址，覆盖 `.env` 里的 `HOST`（默认 `127.0.0.1`） |
| `--port <n>` | 监听端口，覆盖 `.env` 里的 `PORT`（默认 `8000`） |
| `--log-file <path>` | 把日志额外以 **UTF-8** 追加写入该文件（启动脚本用它生成 `server.log`） |

看到这几行就成了：

```
[21:00:00] 启动 127.0.0.1:8000 | 账号模式（1 个账号） | assistant_id=65940acff94777010aa6b796
[21:00:00] 并发策略：每账号串行生成，账号间并行 | 排队上限 180s | 撞闸重试 3 次
[21:00:00] refresh_token 落盘：.glm_tokens.json（GLM_PERSIST_TOKENS=false 可关闭）
[21:00:00] OpenAI 兼容地址: http://127.0.0.1:8000/v1
```

## 三、并发与多账号（v0.2 新增）

上游对**同一个账号**有并发闸：同时只允许一个对话在生成，第二个请求会拿到
「请等待其他对话生成完毕」。v0.1 会把这个错误直接转成 502 给客户端；v0.2 改为：

```
客户端并发请求
      │
      ├─► AccountPool.acquire()   ← 轮询挑空闲账号；全忙则本地排队（GLM_QUEUE_TIMEOUT）
      │
      ├─► 上游返回并发闸 / 429 / 5xx → 指数退避重试（GLM_BUSY_RETRIES / GLM_BUSY_BACKOFF）
      │
      └─► 账号 token 彻底失效 → 该账号冷却（GLM_ACCOUNT_COOLDOWN）→ 换下一个账号
                                      └─ 全部失效且允许兜底 → 临时用游客账号
```

要点：

- **串行闸的持有区间 = 整段生成的时长**。账号槽位在请求发出前获取，在响应流读完后才归还；
  客户端中途断线也会在 `finally` 里归还，不会把账号锁死。
- **单账号 = 一条队列**：第 2 个并发请求排队等待（默认最多 180s），拿到槽位才开始生成，因此
  上游永远看不到并发生成。
- **多账号 = 并行**：每个账号各自一个槽位，2 个账号就有 2 路并发，吞吐线性上升。
- **排队超时** 返回 `503 queue_timeout`；**重试耗尽** 返回 `503 upstream_busy`（错误对象里带原因）。

```env
# 多账号：逗号 / 分号 / 换行分隔都行
GLM_REFRESH_TOKENS=tokenA,tokenB
# 排队与重试
GLM_QUEUE_TIMEOUT=180
GLM_BUSY_RETRIES=3
GLM_BUSY_BACKOFF=3
# 账号失效后的冷却时间
GLM_ACCOUNT_COOLDOWN=300
# 全部账号失效时是否退回游客兜底
GLM_GUEST_FALLBACK=true
# 上游轮换的 refresh_token 落盘（明文，已在 .gitignore 中忽略）
GLM_PERSIST_TOKENS=true
GLM_TOKEN_FILE=.glm_tokens.json
```

> 落盘文件结构是 `{"accounts": [{"seed": ".env 里的原值", "refresh_token": "最新值"}]}`，
> 启动时按 `seed` 把 `.env` 里的旧 token 映射成最新值 —— 所以你**不需要**手动改 `.env`。
> 不想落盘就 `GLM_PERSIST_TOKENS=false`（行为退回 v0.1：只在内存里更新）。

## 四、使用

### curl

```bash
# 非流式
curl http://127.0.0.1:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"glm-4","messages":[{"role":"user","content":"你好"}]}'

# 流式
curl -N http://127.0.0.1:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"glm-4","messages":[{"role":"user","content":"写首五言绝句"}],"stream":true}'
```

### OpenAI SDK

```python
from openai import OpenAI

client = OpenAI(base_url="http://127.0.0.1:8000/v1", api_key="dummy")

resp = client.chat.completions.create(
    model="glm-4",
    messages=[{"role": "user", "content": "你好，介绍一下你自己"}],
)
print(resp.choices[0].message.content)
```

### 接入 GUI 客户端

Cherry Studio / Open WebUI / LobeChat / Chatbox 等：base_url 填 `http://127.0.0.1:8000/v1`，key 随便填（除非你配了 `SERVER_API_KEYS`）。

### 模型名说明

`GET /v1/models` 返回的名字（`glm-4`、`glm-4.6`、`glm-5`、`glm-5.3` …）会通过
`meta_data.selected_model` **真实下发给上游**，能切换上游实际使用的模型（见下方实测表）。
名字清单本身只影响客户端下拉框，可用 `GLM_MODELS=a,b,c` 自定义；客户端也能直接手填任意名字。

- 上游网页版请求体里**没有顶层 `model` 参数**，模型选择走 `meta_data.selected_model`，
  且 `assistant_id` 保持不变（同一个助手，选不同模型）。代理默认把客户端传的 `model`
  原样写进 `meta_data.selected_model`（`GLM_SELECTED_MODEL=false` 可关闭）。
- **实测（`system_fingerprint` / `[upstream] 实际模型`，可复现）**：

  | 客户端 `model` | 上游实际模型 |
  |---|---|
  | `glm-5.3` | `moe_53` |
  | `glm-5` / `glm-4.7` | `moe_52` |
  | `glm-4.6` / `glm-4-air` / `glm-4-flash` / `glm-4` | `moe_53f` |

  对照实验：把 `GLM_SELECTED_MODEL=false`（不带该字段）后，`glm-5.3` 也会退回 `moe_53f`，
  说明差异确实由 `selected_model` 造成。未列入的模型名会走上游的默认档。
- 想让某个模型名改走**另一个 assistant_id**（而不是改 `selected_model`），仍可用
  `GLM_MODEL_ASSISTANT_MAP`（模型名 → assistant_id），例如
  ```env
  GLM_MODEL_ASSISTANT_MAP=glm-5.3=65940acff94777010aa6b796,glm-4.6=xxxxxxxxxxxxxxxxxxxxxxxx
  ```
  之后客户端选 `glm-5.3` 就会用这个 assistant_id 发上游请求（会话删除也会用同一个 ID）。
  怎么拿到目标模型的 ID：网页版切到该模型 → `F12` → `Network` → 打开
  `/chatglm/backend-api/assistant/stream` 请求体，看 `assistant_id`。

关于默认的 `GLM_ASSISTANT_ID=65940acff94777010aa6b796`：

- 它是**主对话助手 ChatGLM** 的 ID，来自 chatglm.cn 前端 bundle `main.d15ba76c.js` 里硬编码的
  常量表（`m="65940acff94777010aa6b796"`，前端用 `isMainChat` 判断路径
  `/main/gdetail/65940acff94777010aa6b796`）。
- 桌面客户端 `app.asar` 里**搜不到** `assistant` 相关字样（它只是 Electron 壳，用 `loadURL`
  加载网页），所以这个值不是从客户端逆向来的。
- 该字段上游确实会校验：换成不存在的 ID，上游会返回 `{"status":0,"result":null}`；
  用默认 ID 时 SSE 的 init 事件里会回显 `"assistant_id":"65940acff94777010aa6b796"`。

**上游会回报"真正干活的模型"**：每个 part 里带一个 `model` 字段，例如

```json
{"logic_id":"...","model":"moe_53f","content":[{"type":"text","text":"你好"}]}
```

`moe_53f` 从命名看就是 **MoE 版 GLM-5.3 flash**（`53`=5.3，`f`=flash）；`moe_53` 是
GLM-5.3（无 flash 后缀），`moe_52` 对应 GLM-5/4.7 一档。本项目把它透出来：

- 非流式响应的 `system_fingerprint` 字段（如 `"moe_53f"`）
- 流式响应最后一帧的 `system_fingerprint`
- `GLM_VERBOSE=true` 时日志里打印 `[upstream] 实际模型 = moe_53f`

**关于"深度思考"模式**：网页真实请求里 `meta_data.chat_mode="deep_thinking"`、
`meta_data.reasoning_effort="max"` 会一起出现（只发一半上游不认）。两种开法：

| 开法 | 用法 | 适用场景 |
|---|---|---|
| 全局默认 | `.env` 里 `GLM_DEEP_THINKING=true` | 这个代理就是专门用来深想的 |
| 单次请求 | 请求体 `{"glm":{"deep_thinking":true}}`（也认 `{"glm_deep_thinking":true}`、`{"deep_thinking":true}`） | 平时快答、偶尔深想，不用重启服务 |

单次请求写 `false` 能压过全局默认的 `true`，反之亦然。开启深度思考时
`reasoning_effort` 默认取网页的 `max`；在 `.env` 里显式配了 `GLM_REASONING_EFFORT`
就以你的值为准（不会被顶掉）。老写法
`GLM_CHAT_MODE=deep_thinking` + `GLM_REASONING_EFFORT=max` 仍然有效；若显式配了
`GLM_REASONING_EFFORT`，按请求开深度思考也不会覆盖它。Anthropic 侧（`/v1/messages`）
发 `thinking` 参数即视为要深度思考，**不需要**额外字段。

> 两个副作用要先知道：① 深度思考明显变慢，长问题可能顶到 `GLM_QUEUE_TIMEOUT`（默认 180s），
> 必要时调大或多配账号；② 客户端的「思考用时」只有在**上游真的在想**时才有意义 ——
> 默认档上游只会把任务复述一遍（例如 "Create HTML with SVG ... animation."），
> 那不是深度思考。开了深度思考后，**不带 `tools` 的请求思维链逐段流式到达**，用时真实；
> 带 `tools` 时正文必须整段缓冲（要先判断是不是工具调用），但思维链会**先于正文流出去**，
> 客户端看到的是「先思考、后出答案」的真实节奏。

以下候选字段实测改动后上游返回的 `model` **没有变化**：顶层 `model`、
`meta_data.model`、`meta_data.chat_model`、`meta_data.model_type`、
`meta_data.chat_mode="agent"`、`meta_data.is_networking=true`。

### 联网搜索

默认关闭，三种开启方式任选：

```bash
# 1) 全局默认开启：.env 里 GLM_NETWORKING=true
# 2) 单次开启（推荐）
curl http://127.0.0.1:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"glm-4","glm":{"networking":true},"messages":[{"role":"user","content":"今天有什么新闻"}]}'
# 3) 平铺写法 {"glm_networking": true}，或 tools 里声明 {"type":"web_search"}
```

### 工具调用 / function calling

网页版接口**没有原生 function calling**，本代理由「提示词模拟」把它接出来——**默认开启**：
客户端只要在请求里带了 `tools`，代理就会启用（解析不出来时会安全退化成普通回答）。
要恢复旧行为（忽略 `tools`、只当普通对话）可显式关闭：

```env
GLM_PROMPT_TOOL_CALLING=false
```

开启后，带 `tools` 的请求会这样处理：

```
客户端发 tools 定义 ──► 代理把工具写成提示词塞进对话（# TOOLS 段）
                              │
                              ▼
                        模型输出 JSON：{"tool_calls":[{"name":"get_weather","arguments":{...}}]}
                              │
                              ▼
        代理解析成标准 OpenAI 形状 ──► 客户端收到 finish_reason="tool_calls" + message.tool_calls
                              │
                              ▼
        客户端执行工具 ──► 回传 {"role":"tool","tool_call_id":...,"content":"结果"}
                              │
                              ▼
        代理把工具调用与结果都渲染进提示词（Tool(名字): 结果）──► 模型基于结果继续回答
```

客户端侧就是标准流程，Python 例子：

```python
resp = client.chat.completions.create(
    model="glm-4",
    messages=[{"role": "user", "content": "北京现在天气怎么样？"}],
    tools=[{"type": "function", "function": {
        "name": "get_weather",
        "description": "查询某个城市的实时天气",
        "parameters": {"type": "object",
                       "properties": {"city": {"type": "string"}},
                       "required": ["city"]}}}],
)
call = resp.choices[0].message.tool_calls[0]
print(call.function.name, call.function.arguments)   # get_weather {"city": "北京"}
```

**代价与边界（务必知道）**

| 项 | 说明 |
|---|---|
| 不是原生 | 靠提示词 + JSON 解析，可靠性取决于模型；解析不出来或工具名不认识时会**退化成普通回答**（绝不瞎报 tool_calls） |
| 文字风格也能识别 | Cherry Studio 等客户端自己会往提示词里塞工具说明（如 `list/inspect/invoke/exec`），往往比本代理的协议更强势，模型就用 JS 风格写调用。代理会把 `invoke({ name: "x", params: {...} })` 这类文本**翻译成对应工具的标准 `tool_calls`**（按客户端实际注册的工具名匹配，支持嵌套对象/单引号/尾逗号） |
| **截断的 JSON 会被抢救** | 上游断流时末态快照可能没到，累积器只拿到 JSON 的一段（尾部 `}]}` 缺失），这时代理会**补齐缺失的闭合符号**再解析（实测修复过「模型输出了两个并行调用却被当成普通回答」的故障）。但只在**切点落在结构边界**时补：若截断在字符串或数值中间（参数会被腰斩）、或键有值无，绝不抢救 —— 宁可退化成普通回答，也不给客户端一个残缺参数的调用 |
| 正文不逐字流式 | 只有拿到完整输出才能判断是工具调用还是正常回答，所以带 `tools` 的请求正文会先缓冲；`stream:true` 仍返回规范 SSE 流，只是正文一次到齐（**思维链不受影响，随到随发**） |
| 参数不做校验 | 不按 JSON Schema 校验模型给的参数，客户端自己校验 |
| 默认开启 | 客户端带 `tools` 即启用；设 `GLM_PROMPT_TOOL_CALLING=false` 则忽略 `tools` 并打日志（旧版行为） |

> 用工具链时若遇到「模型自言自语 / 明明搜到了却不给结论 / 干脆没输出」，
> 那是 v0.3 专门修的一类问题，见「五、v0.3 变更说明」。

排查时看这几行日志：

| 日志 | 含义 |
|---|---|
| `[tools] 已用提示词模拟 N 个工具：...` | 工具协议已注入请求 |
| `[tools] 模型请求调用 [...]` | 模型按 JSON 协议输出，已转成 `tool_calls` |
| `[tools] 文字风格调用已翻译为 tool_calls：[...]` | 走的是兜底翻译（模型输出的是 JS 风格文本） |
| `[tools] 模型选择直接回答（未调用工具），按普通回答返回。正文N字。开头：'...'` | **正常**：模型主动选择用文字回答而非调工具（协议允许，`finish_reason: stop`）。看「正文N字」即可判断质量——几百字以上通常是好答案 |
| `[tools][诊断] …` | 只在**可疑**时打（正文含 JSON 痕迹、或提到 `invoke`/`list` 等工具名却没给出可解析的调用）。正常回答不会刷这一堆 |
| `[tools] 输出疑似泄漏的思考…自动续问（第 N/M 次）` / `[tools] 模型未输出任何内容…自动续问` | 思考泄漏自愈在工作（v0.3 新增），详见「五、v0.3 变更说明 → 4. 排查这些问题的日志怎么看」 |
| `[tools] 工具结果过长已裁剪 …` | 工具结果被裁剪防撑爆（v0.3 新增），同上 |

### Anthropic Messages API（`/v1/messages`）

除了 OpenAI 兼容接口，本代理还暴露 **Anthropic Messages API**，可以直接把 Claude Code、
Cline、各类 Anthropic SDK 指向本服务：

| 路由 | 说明 |
|---|---|
| `POST /v1/messages` | 流式 + 非流式；支持 `system`、多模态 `content` 块、`tools`、`stop_sequences`、`thinking` |
| `POST /v1/messages/count_tokens` | 粗略 token 估算（Claude Code 用它做上下文预算） |

路径同样兼容客户端对 `/v1` 的拼接差异（`/messages`、`/v1/v1/messages` 等都能用）。
鉴权复用 `SERVER_API_KEYS`，同时接受 OpenAI 惯用的 `Authorization: Bearer <key>` 与
Anthropic 惯用的 `x-api-key: <key>`（两者校验同一份 key 列表）。

```bash
# 非流式
curl http://127.0.0.1:8000/v1/messages \
  -H "Content-Type: application/json" \
  -H "x-api-key: <你的 key，未配 SERVER_API_KEYS 时可随便填>" \
  -d '{"model":"glm-4","max_tokens":1024,"messages":[{"role":"user","content":"你好"}]}'

# 流式
curl -N http://127.0.0.1:8000/v1/messages \
  -H "Content-Type: application/json" -H "x-api-key: dummy" \
  -d '{"model":"glm-4","max_tokens":1024,"stream":true,"messages":[{"role":"user","content":"写首五言绝句"}]}'
```

Anthropic SDK 只需改 `base_url`：

```python
import anthropic

client = anthropic.Anthropic(base_url="http://127.0.0.1:8000", api_key="dummy")
resp = client.messages.create(
    model="glm-4", max_tokens=1024,
    messages=[{"role": "user", "content": "你好，介绍一下你自己"}],
)
print(resp.content[0].text)
```

Claude Code 等命令行客户端：把 `ANTHROPIC_BASE_URL` 指向本服务（如 `http://127.0.0.1:8000`），
`ANTHROPIC_API_KEY` 填 `SERVER_API_KEYS` 里的任意一个即可。

**实现要点与边界**

| 项 | 说明 |
|---|---|
| 协议翻译 | Anthropic 请求 → OpenAI 风格 messages/tools → 复用同一套上游调用与消息拍平逻辑；响应再渲染回 Anthropic 的 `message` 对象 / SSE 事件序列（`message_start` → `content_block_*` → `message_delta` → `message_stop`） |
| 工具（`tool_use`/`tool_result`） | 沿用 OpenAI 侧的提示词模拟方案。`tool_use` 的 `input` 由模型输出的 JSON 解析得到，`tool_result` 会被拍平进上游提示词（`Tool(名字): 结果`），多轮工具上下文不丢。带 `tools` 的请求同样会**先缓冲整段**再回流 |
| thinking | 请求带 `thinking`（且未 `disabled`）时，上游思维链输出成 `thinking` block（流式为 `thinking_delta`）。**上游不提供真正的 signature**，本代理给的是占位值；若客户端严格校验 signature 而报错，设 `ANTHROPIC_EMIT_THINKING=false` 关掉（思维链被丢弃，不会混进正文） |
| `stop_sequences` | 上游网页版不支持该参数，只能在拿到完整输出后**本地截断**：命中第一个 stop_sequence 即截断该串并返回 `stop_reason="stop_sequence"`。命中 `stop_sequences` 的请求同样会先缓冲 |
| `max_tokens` | 接受但**不强制**（上游网页版不吃这个参数）；仅 `count_tokens` 与 `usage` 做粗略估算（按字节数估算，非精确 tokenizer） |
| 错误映射 | Anthropic 错误信封 `{"type":"error","error":{"type":...,"message":...}}`：`401 authentication_error`、`503 overloaded_error`（排队超时/上游繁忙）、`502 api_error`、`400 invalid_request_error` |

## 五、v0.3 变更说明（工具链稳定性）

这一版集中修「用 MCP / 工具链时模型不给你好好回答」的一类问题。三个改动互相
关联，起因是一次真实故障：问「北京今天景区人数」，模型搜完网页后**什么都没输出**。

### 1. 思考泄漏自动续问（`GLM_TOOL_CONTINUE_TRIES`）

**问题**：工具链里模型有时只输出「我接下来打算干什么」的碎碎念就结束生成，既不发
工具调用也不给结论，用户看到的就是这串计划：

```
Search results unhelpful. Try opening a site like 高德地图 or 新浪 news search...
Try one more invoke.
```

**修复**：代理识别到这类输出后，把这段碎碎念**当成 assistant 说过的话回灌**，
再加一句明确要求（"请直接调用工具，或直接给出最终回答"），自动续问一次，逼模型
真正给出结论。续问次数用尽仍没结果时，返回一段明确的兜底文案，而不是空气或碎碎念。

需要拦的是**三种形态**（都对应用户看不到有效答案）：

| 形态 | 日志特征 | 客户端表现 |
|---|---|---|
| A 碎碎念进了正文 | `正文长度>0` 且 `正文命中工具短名=['invoke']` | 看到一段英文计划 |
| B 正文空、思维链在自言自语 | `正文长度=0` 且 `思维链长度>0` | 看到思维链里的计划 |
| C 什么都没输出 | `正文长度=0` 且 `思维链长度=0` | **空回答**（`content=""`） |

判定不只看措辞，还要求"像结论"就不拦（`_plausible_answer()`：含数字/建议/结论
信号的正常回答不会被误判），并与思维链高重合才判形态 A。三种形态都有对应测试。

```env
GLM_TOOL_CONTINUE_TRIES=2   # 续问次数；设 0 = 关闭（退回旧版行为）
```

**实现要点**：续问前必须先归还主请求的账号租约——同一账号同时只允许一个生成，
不归还的话续问请求会去排队等一个自己占着的槽位，必然排队超时（`Lease.release()`
幂等，重复调用安全）。

### 2. 工具结果限体积（`GLM_CLAMP_TOOL_RESULT`）

**问题**：这是上一条 bug 的**真正病根**。工具结果回灌上游时原本是**全量原样塞进
prompt**，一次网页抓取就灌进 7.4 万字，峰值 prompt 达 **92967 字**：

```
3878 → 10982 → 14882 → … → 92967 字
```

上下文被网页垃圾填满后，模型「看不见用户原始问题」，只在垃圾堆里打转，最后
答非所问。实测裁剪后同一场景 prompt 降到 **4268 字**，模型能正常作答。

**修复**：回灌时限体积，网页类额外降噪。

| 项 | 默认 | 作用 |
|---|---|---|
| `GLM_TOOL_RESULT_MAX_CHARS` | 8000 | 单条工具结果上限 |
| `GLM_WEB_RESULT_MAX_CHARS` | 4000 | 网页/抓取类额外上限（噪音多、密度低） |
| `GLM_TOOL_RESULT_TOTAL_MAX_CHARS` | 20000 | 所有工具结果累计上限 |

- 网页类（工具名含 fetch/markdown/crawl/browser…）先剥掉 `script`/`style`/
  `noscript`/`svg`/`head`/HTML 注释再裁剪。刻意保守，**不做激进清洗**，避免删掉
  真正有用的表格内容。
- 裁剪**保留头尾**（开头多是结论/状态，结尾常有来源），中间标注省略量，例如
  `……（中间省略 52029 字……）`；**不会把工具结果整条丢掉**。
- 累计预算耗尽后逐步压缩到 600 字下限，仍保留要点。

`GLM_CLAMP_TOOL_RESULT=false` 可整体关闭（不建议）。

### 3. 日志改为 UTF-8（`--log-file`）

**问题**：`server.log` 原由 `start.bat` 里的 PowerShell `Tee-Object` 落盘，而它默认
写 **UTF-16**，导致日志里中文全是乱码——排查问题时根本读不了（很容易误以为是
模型输出的乱码，其实是日志文件本身的编码问题）。

**修复**：日志由 Python 自己写。新增 `--log-file <path>`，以 UTF-8 追加写入；
`start.bat` / `start.sh` 改用它，不再走 `Tee-Object`。旧日志文件仍是 UTF-16，
下次启动会重新生成。

```bash
python glm_proxy.py --log-file server.log
```

### 4. 排查这些问题的日志怎么看

| 日志 | 含义 |
|---|---|
| `[tools] 输出疑似泄漏的思考…自动续问（第 N/M 次，正文X字/思维链Y字）` | 形态 A/B，代理正续问逼模型给结论 |
| `[tools] 模型未输出任何内容（正文与思维链皆空），自动续问（第 N/M 次）` | 形态 C，模型收尾时吐了空气 |
| `[tools] 工具结果过长已裁剪 …（原上限NNNN，累计预算剩NNNN）` | 结果被裁剪防撑爆；`累计预算剩0` 说明这次结果异常大 |
| `[req][verbose] 发往上游的对话正文（NNNNN字）` | **最重要的健康指标**：接近 10 万就是上下文有问题 |
| `[tools][诊断] 正文命中工具短名=[...]` | 正文里出现 `invoke`/`list` 等 = 模型在描述计划而非调用 |

**建议排查顺序**：先看 `发往上游的对话正文` 的字数 → 再看有没有 `已裁剪`
→ 再看有没有 `自动续问` → 最后才看模型原始输出。

### 5. 已知边界（非代理问题）

即使上下文治理好了，**工具/搜索源本身的能力边界仍在**：搜索类工具对「今日实时
客流」这类 query 覆盖很差（连续返回百科、攻略页；`s.visitbeijing.com.cn/flow`
这类官方客流页可能已下线并显示「服务暂时关闭」）。这属于数据源缺失，代理层无法
弥补。想提高成功率：

- 问得更具体（指明**具体景区 + 具体日期**，如「10月4日故宫接待多少人」）；
- 或开启 `GLM_NETWORKING=true` 走智谱自带联网；
- 官方权威口径：各景区官方公众号、北京市文旅局官网（`whlyj.beijing.gov.cn`）。

## 六、自测
不需要网络、不消耗任何真实账号（上游用假替身）：

```bash
python -m unittest discover -s tests -v
```

覆盖：签名算法、多轮消息拍平（含 `tool_calls` / `role:"tool"` 上下文不丢）、联网/tools 解析、
**流式增量还原（有序分片逐字外发、重复帧不重复吐、末态快照覆盖校正）**、**同账号并发被串行化（断言上游从未并发）**、
多账号并行与失效轮换、撞闸退避重试、429 归类、非并发错误不重试、token 轮换落盘、
工具调用（触发/文字风格兜底翻译/退化/工具结果回传）、
**按请求的深度思考开关（三种写法 / 显式 false 压过全局 / 续问不丢档位）**、**工具模式下思维链先流后答（非流式仍是单帧 JSON）**、
**思考泄漏自愈（三种形态各自能识别；正常回答不误杀；续问后拿到真答案；
续问耗尽返回兜底文案而非空气；续问前后账号槽位均已归还）**、
**工具结果限体积（单条/累计裁剪、网页降噪、保留头尾、开关可关；
用户原始问题不会被裁掉）**、
真 HTTP 端到端（路由 / 鉴权 / 路径别名 / 流式增量 / BOM 请求体 / 503 与 502 映射），
以及 Anthropic `/v1/messages`（非流式/流式事件序列 / system / thinking 开关 / tool_use 与
tool_result 回传 / 文字风格翻译 / stop_sequences 本地截断 / x-api-key 鉴权 / count_tokens / 路径别名）。

预期结尾：

```
Ran 136 tests in 33s
OK
```

## 七、注意事项

- **并发限制**：上游同一账号只允许一个生成，本项目已按账号串行排队 + 退避重试。想提高吞吐就配多个
  `GLM_REFRESH_TOKENS`（多账号）。
- **多账号**：只对**不同账号**生效；同一账号重复填不会带来并发提升（会按 token 去重）。
- **token 有效期**：`refresh_token` 会被上游轮换，默认自动落盘到 `.glm_tokens.json`，
  所以重启不会失效。若文件被删除且 `.env` 里的值也已失效，重新获取即可。
- **单轮结构**：网页版接口只吃单条 user 消息，本项目把 OpenAI 的多轮 messages + system 拍平成一段 transcript 文本（`Assistant: ` 结尾引导模型续写），这是它能记住上下文的原因。
- **工具结果限体积（重要）**：工具结果回灌上游时默认限体积（单条 ≤8000 / 网页类 ≤4000 /
  累计 ≤20000 字，网页类额外降噪）。不限制时实测出现过单条 7.4 万字的网页 markdown、**峰值
  prompt 92967 字**，模型被垃圾淹没后答非所问。详见「五、v0.3 变更说明 → 2. 工具结果限体积」。
- **思考泄漏自愈**：工具链里模型只输出「打算干什么」的碎碎念、或干脆什么都不输出时，
  代理会自动续问（`GLM_TOOL_CONTINUE_TRIES`，默认 2 次，设 0 关闭）。详见「五、v0.3 变更说明 → 1. 思考泄漏自动续问」。
- **模型名会真的切换上游模型**：代理把客户端 `model` 写进上游请求的 `meta_data.selected_model`（不是顶层 `model`），实测 `glm-5.3`→`moe_53`、`glm-5`/`glm-4.7`→`moe_52`、`glm-4.x`→`moe_53f`（详见「四、使用 → 模型名说明」）。模型自称"我是 GLM-5.3"不可靠，判断实际模型请看 `system_fingerprint`。
- **流式按上游真实形态还原**：同一个 part 每帧推的是**有序增量分片**，全部推完后再推**一次完整快照**
  （抓帧实测：36 个思维链分片的长度之和 == 末态快照 782 字，逐字相等）。本项目分片一到就追加并立刻外发，
  快照只做覆盖校正 —— 于是思维链/正文都能逐字流出去，中途断流也只少最后一个分片。
  ⚠ 早先把它当成「分片乱序/改写」，改用「只发已确认稳定的前缀」，代价是内容全攒到最后一刻才吐
  （客户端显示「已深度思考（用时 501 秒）」却一个字没有），且末态快照没到时正文只剩 `to` 这种碎片。
- **思考链（`reasoning_content`）**：是否有思维链由上游模型自己决定，本项目不做开关。短问题通常没有 think 片段（实测「什么模型？」3 次均为 0 帧），需要推理的问题可能出现。
- **thinking 模型**：`reasoning_content` 字段会单独返回思维链，部分客户端可显示。流式请求下思维链先于正文到达（含工具模式，见上文的「思考用时」说明）。
- **工具调用**：网页版接口没有原生 function calling，`GLM_PROMPT_TOOL_CALLING`（默认 `true`）时由本代理用
  提示词模拟（代价与边界见「四、使用 → 工具调用」）；显式设为 `false` 时 `tools` 会被忽略并打日志。
- **未实现**：图片生成、文件上传/解读（上游插件没有稳定公开接口，不做）。

## 八、文件说明

```
glm_proxy.py             主程序（签名 / 账号与串行闸 / token 落盘 / 消息转换 / 工具结果限体积 /
                         思考泄漏检测与续问 / SSE 解析 / UTF-8 日志 / HTTP 服务）
anthropic_api.py         Anthropic Messages API 适配层（/v1/messages，协议翻译，被 glm_proxy.py 导入）
start.bat / start.sh     启动脚本（检查 Python、补 .env、前台启动；用 --log-file 写 UTF-8 日志）
tests/test_glm_proxy.py  离线自测（假上游替身 + 真 HTTP 端到端），136 个用例
.env.example             配置示例
.glm_tokens.json         运行后自动生成：上游轮换后的 refresh_token（明文，勿外发）
server.log / server.err  启动脚本产生的运行日志（UTF-8，已在 .gitignore 中忽略）
```

## 九、故障排查

| 现象 | 原因 / 处理 |
|---|---|
| 启动即报「上游鉴权失败 401」 | `refresh_token` 失效，重新获取 |
| 返回「请登录后继续使用」 | 账号态无效，或游客 token 过期 |
| `503 upstream_busy` | 撞并发闸且退避重试用完：降并发、调大 `GLM_BUSY_RETRIES`，或加账号 |
| `503 queue_timeout` | 排队超过 `GLM_QUEUE_TIMEOUT`：有人在长回答占着账号，调大该值或加账号 |
| 日志出现「已冷却」 | 该账号 token 彻底失效，冷却 `GLM_ACCOUNT_COOLDOWN` 秒后重试；期间由其它账号/游客顶上 |
| 开了工具但模型直接回答了文字 | 模型没按 JSON 协议输出 → 代理按普通回答返回；换个描述更清楚的工具定义/换 `glm-4.6` 再试 |
| 我明确不想要工具调用 | 设 `GLM_PROMPT_TOOL_CALLING=false` 再重启（默认开） |
| 工具链里模型开始自言自语（"Search results unhelpful. Try opening a site like 高德地图... Try one more invoke."），用户看到的既不是调用也不是回答 | 「思考泄漏」，v0.3 已自愈：自动续问逼模型给结论（`GLM_TOOL_CONTINUE_TRIES`，默认 2 次）。日志搜 `自动续问` 确认触发；设 `0` 关闭。**三种形态都覆盖**（碎碎念进正文 / 正文空思维链说话 / 两者皆空）。详见「五、v0.3 变更说明 → 1」 |
| 工具跑完了但模型**没输出**（客户端收到空回答） | 同上，形态 C。v0.3 已会续问；日志搜 `模型未输出任何内容` |
| 看到「模型没按工具协议输出」以为是故障 | **这是误报**（v0.3 已改文案）。实测 10 次里 8 次是模型主动选择的正常文字回答，协议本来就允许。看日志里的「正文N字」判断：几百字以上通常是好答案；只有正文 0 字才是真问题 |
| 模型「答非所问 / 明明能查却说查不到」，尤其在抓过网页之后 | 看日志 `发往上游的对话正文（NNNNN字）`：接近 10 万就是上下文被工具结果撑爆了。v0.3 已默认限体积；仍偏大可调小 `GLM_TOOL_RESULT_TOTAL_MAX_CHARS`，日志搜 `已裁剪` 确认生效。详见「五、v0.3 变更说明 → 2」 |
| 拉不到模型列表 / 404 | 路径已做兼容：`/v1/models`、`/models`、`/v1/v1/models`、带 `?query`、结尾多斜杠都能用。若仍 404，看服务端日志里客户端**实际请求的路径**再对症改地址；Cherry 也可直接手填模型 ID（如 `glm-4`） |
| 回答出现重复/错乱的字句 | 只在罕见兜底里出现：末态与已发分片不再互为前缀时，代理会从公共前缀处补齐（接缝重复几个字，好过整段丢内容）。日志搜 `[upstream] 已在终态补齐剩余内容`，把附近上下文发出来 |
| 思考框一直是空的、内容最后一刻才吐一大坨 | 上游推的是有序增量分片 + 末尾一次快照；按「稳定前缀」输出的旧版本会把分片全部覆盖掉、只等末态快照。更新到按分片即时外发的版本并**重启代理**即可 |
| 没有"思考过程"显示 | 思维链由上游模型自己决定吐不吐 think 片段，代理不做开关；短问题一般没有 |
| 客户端显示「已深度思考（用时 0.1 秒）」，内容只有一句任务复述 | 上游默认档的行为：代理默认不发 `chat_mode`/`reasoning_effort`，模型只会把任务复述一遍。按上文「关于"深度思考"模式」单次开启即可；开了仍只有一句，说明上游这一轮确实没多想 |
| Anthropic 客户端报 thinking signature 无效 | 上游不提供真正的 signature，代理给的是占位值 → 设 `ANTHROPIC_EMIT_THINKING=false` 关掉 thinking 输出后重启 |
| `/v1/messages` 返回 404 | 路径兼容 `/v1/messages`、`/messages`、`/v1/v1/messages`；若仍 404，看日志里客户端**实际请求的路径** |
| 想知道"我选的模型名到底走了谁" | 看响应里的 `system_fingerprint`（如 `moe_53f`），或开 `GLM_VERBOSE=true` 看日志 `[upstream] 实际模型 = ...` |
| 响应很慢/超时 | 调大 `GLM_TIMEOUT`；长回答+联网确实慢 |
| 日志/终端中文乱码 | v0.3 起 `server.log` 由 Python 直接以 UTF-8 写盘（`--log-file`），不再有 `Tee-Object` 的 UTF-16 问题。**旧版本留下的 `server.log` 仍是 UTF-16**，删掉重启即可；或用支持 UTF-16 的工具打开。终端输出乱码则执行 `chcp 65001` |

想看上游原始请求/响应，设 `GLM_VERBOSE=true` 并配合 `DEBUG` 环境变量调整上游细节（当前版本仅在关键节点打日志）。

# 运行与部署手册

返回 [README：题目说明与快速开始](../README.md)。本文保留详细运行配置、监控行为、外部执行器、节点池和故障专项命令；**所有命令均在仓库根目录执行**，不是在 docs 目录。首次体验只需 README 的本地模式。

从三天、AI 辅助开发 MVP 增量演进的 **任务监控与步骤托管系统**。业务端可以按需登记动态顺序步骤，中心管理执行授权、Attempt、租约、恢复、提交和资源。六步动画保留为业务示例与回归基线；原三步和既有冻结任务保留历史兼容。页面只做监控与管理，不提供上传或创建入口。

以动画素材处理为演示场景，AI 计算允许 MOCK，执行状态、持久化、故障恢复和产物提交不能只做 UI 模拟。

## 当前状态

**当前：页面只做监控与管理，不再提供上传或创建表单；后端提交与媒体能力保留。** PostgreSQL、迁移、独立 API / Worker、输入与产物均真实；后端仍可处理已有或通过 API 上传的 PNG/JPEG，并生成真实 GIF。分割、动作分析及帧变换仍为确定性 CPU MOCK；没有真实模型、真实 GPU 或完整前端工程。

- Worker 已自带独立后台恢复扫描，节点观察/RPC 不阻塞唯一续租路径；无需额外调度服务。有限重试、人工恢复、检查点接管和代次隔离有直接回归，完整边界见验收记录。
- `managed-v1` 支持业务按需登记顺序步骤并显式结束；为兼容已有调用，默认仍为 `animation-dag-v2` 六步示例，保留两路分支/汇合和受限并行；`animation-v1` 新建返回 422 / PIPELINE_RETIRED。已有三步的冻结配置、执行/恢复、查询和产物不改写。不提供任意代码、流程编辑器、外部发布系统或公网服务。
- 同任务恢复跳过成功步骤并复用已提交检查点；模拟槽位支持互斥、等待、失联隔离和确认旧进程退出后复用。
- 兼容跨任务缓存只引用成功提交且仍可读的产物，记录来源；支持绕过和生成标识。基本取消在确认进程退出/清理后才终结，不提供优先级或抢占。
- 步骤级托管：默认仍为本地后端；新 external 任务可使用预配置节点池，每个节点有独立身份/凭据、能力、接单开关、CPU 预算和多个模拟设备。中心仍创建每个 Attempt、决定重试并提交；保留旧单执行器方式。远端须验证 HTTPS 与实际共享引用，不接管任意已有作业，不做自动发现或真实 GPU。
- 源码尚未选择许可证。运行数据、日志和本地环境不进入 Git。
- 后续接入验证保留配套执行器，并提供仅依据协议、无中心模块导入的独立 synthetic MOCK。派发前核对协议 v1、处理器版本与能力；不同计算语义不混用缓存版本。具体证据见验收表，不能用旧本地结果替代外部故障验收。

## 启动（PowerShell 7，仓库根目录）

先安装 uv 和 Docker Desktop。Python 固定为 `.python-version` 中的 3.12.10；直接依赖精确固定，传递依赖由 `uv.lock` 锁定。本阶段 Compose **只启动独立 PostgreSQL**，API 与 Worker 在项目 `.venv` 中分别运行，不需要新建前端或调度微服务。

```powershell
docker compose up -d --wait
uv sync --locked
$env:PYTHONPATH = (Join-Path $PWD 'backend')
uv run --locked python -m app.db
```

终端一（API，仅监听本机）：

```powershell
$env:PYTHONPATH = (Join-Path $PWD 'backend')
uv run --locked uvicorn app.api:app --host 127.0.0.1 --port 8000
```

终端二（独立 Worker / Supervisor，自动启动计算子进程）：

```powershell
$env:PYTHONPATH = (Join-Path $PWD 'backend')
uv run --locked python -m app.worker --worker-id local-worker
```

需要六步分支实际并行时，再开一个 Worker 终端（同样设置 PYTHONPATH）：

```powershell
$env:PYTHONPATH = (Join-Path $PWD 'backend')
uv run --locked python -m app.worker --worker-id local-worker-2
```

未配置节点池时，legacy 兼容执行域默认 CPU 容量为 2、模拟 GPU 槽位为 1；一个 Worker 同时监督一次执行。API 与所有 Worker 使用一致的 `CPU_MAX_CONCURRENCY`（1–4，默认 2）；新节点池使用各节点预算而非这个全局值。未知 CPU 退出继续占预算，GPU 不确定 owner 仍隔离；不能为展示并行清空未知资源。升级先迁移并同步重启，不混跑旧代码。

默认数据库是 `127.0.0.1:55432/pipeline`，仅供本地演示的用户名/密码均为 `pipeline`；不复用其他项目数据库。数据库使用 Compose 命名卷；候选与已提交产物在 `.runtime/artifacts/`。不要执行 `docker compose down -v` 来停止服务，它会删除数据库卷。普通停止使用 `docker compose stop`。

应用只读**进程环境变量**，不自动加载 `.env`。`.env.example` 是字段说明；自定义数据库密码/端口后须同时显式设置各应用终端的 `DATABASE_URL`。

## 输入与结果预览

最短演示：按下节命令通过 API 提交合成素材任务，再打开 `http://127.0.0.1:8000/` 查看队列。页面没有上传/新建入口，也没有旧三步选择；仍提供取消、人工恢复、历史查询和已提交结果预览。

仅支持**静态 PNG/JPEG**：上传最多 8 MiB、16MP、任一边不超过 8192；拒绝格式伪装、动画图、SVG 和视频。实际归一化至 256 边长内，最多 120 帧，另有处理像素预算；读取上传像素、PNG 帧、GIF 编码均真实，AI 内容仍为 MOCK。文件在被忽略的 `.runtime/artifacts/inputs` 和 Attempt 命名空间内保存。

后端上传兼容能力未删除：`POST /api/v1/inputs` 接收 raw body（不是 multipart），Content-Type 为 `image/png` 或 `image/jpeg`，返回稳定 UUID、指纹与尺寸。上游可将六步提交的 input 设为 `{"kind":"uploaded","input_id":"上传返回的UUID","seed":42}`。已提交 GIF 通过 `/api/v1/tasks/{id}/artifacts/{artifact_id}/preview` 查看或下载。移除页面表单不会删除已有输入或产物；步骤托管也复用这些处理器和读取边界。

## 动态顺序托管：业务端决定下一步

先按启动说明应用全部迁移（当前至 008；动态登记始于 006），并运行 API 与 Worker。最短业务示例（总数只在这个业务程序里，不预先发给中心）：

```powershell
uv run --locked python examples/business_sequence.py --business-id demo-managed-001 --steps 3
```

中断业务客户端不会取消已授权计算；重跑相同 business-id 会重读数据库历史，再继续登记，不覆盖已完成步骤。需要新业务任务时换 business-id。若任务失败，先查询错误并通过现有 resume 接口明确恢复，示例不会偷偷重跑失败任务。示例封装 HTTP 登记/查询/结束；领取和续租仍由 Worker 适配层负责，计算程序无需散布权限调用。

外部后端先配置下文执行器，然后追加 `--backend external`；配套实现使用 `--processor-version 1`，独立实现使用 `--processor-version independent-mock-v1`。三者都由中心授权后才执行真实 CPU MOCK，并保存真实 JSON 批次/检查点/结果。

核心 API（所有写请求重试都复用原 Idempotency-Key）：

| 调用 | 语义 |
| --- | --- |
| POST /api/v1/tasks，`{"pipeline_id":"managed-v1"}` | 创建开放任务，不预建步骤、不预知总数 |
| POST /api/v1/tasks/{id}/steps，`{"key":"inspect-data","after_step":null,"processor":"mock-step","parameters":{"batch_count":3}}` | 首步登记；后续将 after_step 设为已持久化末步骤 key。输入、版本、参数固定，重复键不追加 |
| GET /api/v1/tasks/{id} | 读取真实步骤顺序、状态、提交结果及 registration_closed；恢复后先读再决策 |
| POST /api/v1/tasks/{id}/finish，`{"last_step":"inspect-data"}` | 关闭登记；等待实际执行、提交与退出确认完成，不能作为成功上报 |

开放任务的已登记步骤都成功后仍是等待业务登记，不会自动成功。能力查询凭据只有读取权限，不能启动/停止作业；中心确认处理器版本、依赖和资源后才创建 Attempt、发放执行授权。新动态处理器仅 mock-step，支持 synthetic JSON；不接程序路径、插件、任意 DAG 或动态并行。六步动画的受限并行仍仅属于保留的示例图。

## 动画示例提交和查询

```powershell
$headers = @{ 'Idempotency-Key' = [guid]::NewGuid().ToString() }
$body = Get-Content -Raw -Encoding utf8 examples/animation-pipeline.json
$task = Invoke-RestMethod http://127.0.0.1:8000/api/v1/tasks -Method Post `
  -Headers $headers -ContentType 'application/json' -Body $body
$id = $task.id
Invoke-RestMethod "http://127.0.0.1:8000/api/v1/tasks/$id"
Invoke-RestMethod "http://127.0.0.1:8000/api/v1/tasks/$id/steps/generate_frames/attempts"
Invoke-RestMethod "http://127.0.0.1:8000/api/v1/tasks/$id/events"
$artifacts = Invoke-RestMethod "http://127.0.0.1:8000/api/v1/tasks/$id/artifacts"
$artifacts | Select-Object id,kind,availability,download_url
Invoke-RestMethod http://127.0.0.1:8000/api/v1/resources
```

该文件现在是可直接 POST 的六步请求体，采用合成输入，无需模型密钥或上传图片；结果为 JSON，`output_refs` 指向 publish_result 提交的本系统清单。图片输入另有真实 GIF 预览。历史三步终端仍为 encode_package，不迁移既有任务。`source=mock` 表示 AI 内容来源；`smoke-v1` 只在显式开启测试开关时用于内部回归，正常入口拒绝新建。

POST 必须携带 `Idempotency-Key`；响应不明时用同一键重试，同键相同请求返回原任务，不同请求返回 409。旧三步已有键仍可重放；旧请求省略 pipeline_id 时沿用该键原来的版本，不被新默认值解释成六步，也不会新建任务。API 默认 `cache_enabled=false`，当前演示请求显式开启缓存。命中须同作用域、流程/步骤/处理器版本、输入/上游字节指纹、参数、seed 和 generation_tag 兼容；`bypass_cache=true` 强制重算。publish_result 不缓存，失败/取消/未提交或损坏媒体不能成为有效来源。

## AIGC 任务调度与监控平台

API 启动后访问 `http://127.0.0.1:8000/` 查看任务队列，也可直接打开 `http://127.0.0.1:8000/tasks/<任务ID>`。页面由同一 FastAPI 提供静态 HTML/CSS/原生 JavaScript，不需要 npm、前端构建、外部 CDN 或额外端口。

- 左侧队列支持按状态筛选、任务 UUID / 流程标识搜索、每页 10 条翻页；顶部是全库任务、运行、成功、失败数，筛选不改变全库计数。无指定任务时默认打开最新一条；在同一搜索框提交完整 UUID 可直接打开，不再提供重复查询表单。
- 顶部统一标明“模拟演示环境”：模型推理与算力为模拟实现，调度状态和运行记录由系统实际生成。右侧分为生成工作流、执行记录、任务事件、生成结果与资产四个页签；三步或六步图仍来自冻结依赖。主体分割/图生视频步骤采用 SAM 2、Wan2.1-I2V 的演示模型标签，不代表实际模型接入或路由能力。CACHED 不伪造计算 Attempt，进度只取已提交单位/成功步骤。
- 全局队列/资源约每 3 秒更新，选中任务约每 2 秒更新；统一连接提示区分监控/任务故障，非同一原子快照。只有一个刷新入口，同时刷新两路数据。事件按 seq 增量去重。断连保留上次状态并提示可能过期，不把网络错误改成 Task FAILED。
- 资源合并进顶部概览：默认显示节点在线数和待核实数量，展开查看各节点 CPU 调度预算、MOCK GPU 状态、owner 与异常历史入口。页面不再展示旧兼容执行域的摘要与资源区块；历史兼容 API、执行/恢复逻辑和未确认退出告警保留。无配置节点时显示空态，不回退展示旧域。未知占用不代表仍在计算，没有强制释放按钮或虚构 GPU 利用率。手机布局保留队列/详情，流程和宽表在内部横向滚动。
- 输入素材缩略图、实际尺寸、随机种子、已有目标帧数与缓存策略优先展示；无配置的数据不补造。任务 ID/时间/时限仍放在“任务信息”，点击步骤查看实际执行节点、次数、时间及技术标识。节点别名只用于展示，原始节点/步骤/处理器 ID 仍保留；任意自定义步骤名不强行映射。无上传/创建表单；历史三步保留标识，错误、数据过期、资源异常与全局模拟环境说明保留。
- “节点配置与执行能力”分为执行能力、可靠性与安全、运行时信息。任务处理器区分内置步骤处理器与自定义步骤处理器，列出输入来源、支持的工作流步骤和参考模型（演示）；可靠性使用执行请求鉴权、幂等启动、检查点恢复、进程退出确认等术语。主列表不逐行重复英文代码，原始标识集中在运行时信息并可悬停查看，不增加二级折叠栏。CPU 任务并发明确为当前占用/并发上限，节点报告值保留来源说明。节点及任务事件使用中文描述和字段明细，保留原始代码/未知字段；节点事件最多 5 条、任务事件最多 100 条。生成结果按已提交元数据展示实际格式、尺寸、帧数与下载入口，不把 GIF/JSON 宣称为 MP4，也不生成不存在的提示词或硬件利用率。
- 待核实执行器可“确认知悉”，保存后移出顶部提醒，在资源面板“已知悉记录”中保留详情与“恢复提醒”。知悉状态持久化于独立表，不确认进程退出、不释放资源、不改任务/Attempt/执行事件；异常状态变化后重新提醒。待核实与已知悉各显示最近 5 条，计数覆盖全部记录。升级需显式应用 008 迁移并重启 API；已有 007 的运行环境不需为这项展示功能重启 Worker/执行器。
- 仅失败任务显示人工恢复操作。连接中断后重试复用同一幂等键；未确认的键在同一标签页会话中保留，查询确认恢复轮次增加后才清除。浏览器禁用会话存储时仅能在当前页面内保留该键。
- QUEUED/RUNNING 可请求基本取消；CANCELING 等待真实退出和清理，未知身份继续隔离，只有查询到终态才显示已取消。离开页面不会停止 Worker。监控台不是账号/权限、复杂调度或完整运维管理平台。

只读聚合接口 `GET /api/v1/monitor?status=RUNNING&q=animation&limit=10&offset=0` 返回全库计数、匹配总数、队列步骤/尝试汇总、等待数量及待核实执行器；不领取任务或改写事件。`unconfirmed_count` 仍表示实际未确认退出总数，另有 `unacknowledged_count` / `acknowledged_count` 及对应列表。`POST /api/v1/monitor/attempts/{attempt_id}/acknowledgement` 确认当前告警知悉，`DELETE` 同路径恢复提醒，均只写提醒元数据。原 `GET /api/v1/tasks` 列表协议保持不变。

开发库保留原 MVP 恢复演示 `470844c4-869a-44ff-8c5e-44a616dd6d58`。增强演示：`e712320a-888f-4130-85a5-75127d4fb12f` 为真实图片/GIF；`a53bff34-cf5d-4551-bbd2-586f33c802c1` 为六步实际并行，`17481fd8-9151-493b-9c97-dffd561da26d` 命中前者五步缓存，publish_result 仍独立提交；`feb61811-b8db-4357-82b9-4036e4231944` 显式绕过。它们不是公网地址，需保留本地数据库和产物。

该开发库 P1 历史 CPU LOST Attempt 缺创建时间，仍保留未知退出占用，没有篡改成清理成功。此前本地并行演示曾显式使用 CPU_MAX_CONCURRENCY=3、单模拟 GPU 槽位，属于历史运行配置。2026-10-25 恢复启动后 legacy CPU 使用默认 2（仍占 1），两个配置节点各自 CPU 配额 2、各有两个模拟设备；新节点资源不与旧域混用。

2026-10-25 已在原开发库安全应用 006/007，未重建卷。最新可查询演示：`3f3f4d13-d476-4eb9-8e1d-05d06699e7a3` 为节点 A 上实际六步、48 帧 GIF；`ff36d4a3-e304-4a3c-aff1-0900702e1996` 为节点 A → 独立节点 B 的两步托管与真实上游读取。均可在 `/tasks/<任务 ID>` 查看。迁移前备份保存在 `.runtime/backups/`；当前 API/三个 Worker/三个执行器由持续受管终端会话承载，需保持该会话运行，未安装 Windows 服务或开机自启动。可复现的各进程独立终端命令见下面的启动配置；密钥、状态库与产物不可清空。

## 可选：独立执行器的步骤级托管

保持 PostgreSQL、API、Worker 的启动方式，先运行增量迁移至 008（`uv run --locked python -m app.db`）。执行后端升级时在没有活动任务的窗口停止旧 API/Worker，再按最终代码启动；不要让旧 Worker 将 external 任务误当成本地任务。仅从 007 升级提醒知悉功能时，008 只新增元数据表，可保留 Worker/执行器，仅重启 API。历史三步与已有 local 任务不改写，不切换运行中的后端。007 保留旧 id=1 槽位/未知退出，标为 legacy；不是把它伪装成新物理节点。

首次只生成一次稳定密钥，放在被忽略的本地运行目录，不覆盖已有密钥：

```powershell
New-Item -ItemType Directory -Path .runtime -Force | Out-Null
$secretFile = Join-Path $PWD '.runtime/external-executor.secret'
if (-not (Test-Path -LiteralPath $secretFile)) {
  [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32)) |
    Set-Content -LiteralPath $secretFile -NoNewline
}
```

然后在 API、每个 Worker 和执行器各自的终端中设置相同配置（仍不自动读取 .env）：

```powershell
$env:PYTHONPATH = Join-Path $PWD 'backend'
$env:ARTIFACT_ROOT = Join-Path $PWD '.runtime/artifacts'
$env:EXTERNAL_EXECUTOR_URL = 'http://127.0.0.1:8090'
$env:EXTERNAL_EXECUTOR_ID = 'executor-one'
$env:EXTERNAL_EXECUTOR_SECRET = Get-Content -Raw (Join-Path $PWD '.runtime/external-executor.secret')
```

中心先运行 `uv run --locked python -m app.db`，再按前述命令分别启动 API/Worker。独立执行器另开一个终端，执行同一配置块后运行：

```powershell
uv run --locked python -m app.external_service --executor-id executor-one `
  --artifact-root $env:ARTIFACT_ROOT --state-dir (Join-Path $PWD '.runtime/executor-one') --port 8090
```

服务不使用中央 PostgreSQL；独立 SQLite 记录命令、物理进程身份和输出事件。保留 state-dir、共享产物和密钥才能重启对账，不能通过删除它们“恢复”。服务重启会停止身份可核实的遗留进程，再由中心决定新 Attempt；不伪装成接管任意外部作业。无 PID 的不确定启动窗口保持隔离，需要可信人工核实。

提交六步 external 任务（演示绕过缓存，确保实际计算而非命中旧结果）：

```powershell
$request = Get-Content -Raw -Encoding utf8 examples/animation-pipeline.json | ConvertFrom-Json
$request.execution_backend = 'external'
$request.bypass_cache = $true
$key = [guid]::NewGuid().ToString()
$task = Invoke-RestMethod http://127.0.0.1:8000/api/v1/tasks -Method Post `
  -Headers @{ 'Idempotency-Key' = $key } -ContentType 'application/json' `
  -Body ($request | ConvertTo-Json -Depth 10)
Invoke-RestMethod "http://127.0.0.1:8000/api/v1/tasks/$($task.id)"
```

默认 `execution_backend=local`；external 未配置返回 422 / BACKEND_UNAVAILABLE，不默默退回本地。选择随新任务冻结，中心仍为每一步创建 Attempt、决定重试和提交结果。Attempt 查询包含 execution_backend、executor_id、external_job_id；external 的本地 executor_pid 为空不是“未执行”，物理身份由执行器持久化，稳定 command/job ID 等于该 Attempt UUID。

每个执行器接口要求绑定 Task/Step/Attempt/generation/worker/executor/job 的 HMAC capability；节点模式另签 node_id/resource_id。配套服务只运行固定 app.executor，独立实现只运行预装的计算子进程，不能上传程序或指定执行路径。旧配置仅回环 HTTP，新节点池允许受控的验证 HTTPS，不用代理/重定向或完整账号平台。各端须挂载同一真实共享存储；未做对象存储或跨机复制。30 秒心跳、300 秒可续租、默认关闭硬时限/任务截止时间不变；0.5 秒只是 RPC 超时，不是计算上限。

`PUT /v1/jobs/{id}` 的重复投递只查原命令；进度/检查点/结果作为带序号的候选事件，经中心当前执行权验证后才提交。调用成功不等于业务完成，RUNNING/续租不等于有新进度；停止请求也不等于退出确认。服务不可达或身份不明时保留 QUARANTINED / 未确认 CPU 占用，旧代次退出事实只清理其自身 owner，不覆盖新结果或释放新 owner。

### 另一种实现：独立 synthetic MOCK

`executors/standalone_mock.py` 是依据 [协议 v1](SPEC.md#121-可独立实现的-http-协议-v1) 实现的另一个服务，不导入中心 app 模块，也不启动 app.executor。使用标准库与已锁定的 psutil，计算限定合成 JSON，输出真实可读文件，不宣称生成 GIF。原配套服务仍承担媒体/GIF 基线。

先 `uv sync --locked`。以下为兼容的单执行器配置；API/所有 Worker 使用相同 URL/身份，不切换已运行任务。协议升级须同步重启服务和中心，旧服务缺能力端点会明确拒绝新派发。预配置多节点使用下一节，不是自动发现。

示例在各终端使用前述共享根和密钥，再将中心的配置改为：

```powershell
$env:EXTERNAL_EXECUTOR_URL = 'http://127.0.0.1:8091'
$env:EXTERNAL_EXECUTOR_ID = 'standalone-one'
```

独立执行器终端（保留同一稳定密钥和共享根；state-dir 不与配套服务共用）：

```powershell
& .venv/Scripts/python.exe -I (Join-Path $PWD 'executors/standalone_mock.py') `
  --executor-id standalone-one --artifact-root $env:ARTIFACT_ROOT `
  --state-dir (Join-Path $PWD '.runtime/standalone-one') --port 8091
```

前述六步提交命令中增加 `$request.processor_version = 'independent-mock-v1'`，保持 `execution_backend='external'`、`input.kind='synthetic'`；演示建议 `bypass_cache=$true`。其最终结果是 JSON 清单，沿用任务/步骤/Attempt/事件/产物查询接口；页面仍没有上传和创建入口。

每次派发先认证 `GET /v1/capabilities`。版本、输入类型、步骤或必要能力不兼容时，以 `INVALID_CONFIG` 结束该次派发，不启动计算，不静默回退本地；清理依然必须有绑定退出或未启动墓碑。启动响应丢失只重投同一持久化 ID。单独处理器版本隔离计算语义，恢复仍复用同任务的已提交检查点和成功上游。

## 可选：预配置节点与多模拟槽位

在安全升级窗口完成 007；默认空 `EXECUTION_NODES_JSON` 保持上述本地/旧单执行器路径。API/所有 Worker 需同一节点清单和所有节点的凭据引用，各执行器只需自己的密钥。配置示例为 [execution-nodes.json](../examples/execution-nodes.json)，包含配套 version=1 与独立 version=independent-mock-v1 两种节点；异构后继可以读取兼容的已提交产物，但**同一步接管必须有同处理器版本**的另一节点，不会偷换语义。

在仓库根目录首次生成两个独立密钥，不覆盖现有文件：

```powershell
New-Item -ItemType Directory -Path .runtime -Force | Out-Null
foreach ($name in @('node-a', 'node-b')) {
  $file = Join-Path $PWD ".runtime/$name.secret"
  if (-not (Test-Path -LiteralPath $file)) {
    [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32)) |
      Set-Content -LiteralPath $file -NoNewline
  }
}
```

中心 API/Worker 各终端在启动前设置（`.env` 不自动加载）：

```powershell
$env:PYTHONPATH = Join-Path $PWD 'backend'
$env:ARTIFACT_ROOT = Join-Path $PWD '.runtime/artifacts'
$env:ARTIFACT_STORAGE_ID = 'pipeline-artifacts'
$env:NODE_A_SECRET = Get-Content -Raw (Join-Path $PWD '.runtime/node-a.secret')
$env:NODE_B_SECRET = Get-Content -Raw (Join-Path $PWD '.runtime/node-b.secret')
$env:EXECUTION_NODES_JSON = Get-Content -Raw -Encoding utf8 examples/execution-nodes.json
```

节点 A 的独立终端只设置自己的密钥与共享根，再启动配套实现：

```powershell
$env:PYTHONPATH = Join-Path $PWD 'backend'
$env:NODE_A_SECRET = Get-Content -Raw (Join-Path $PWD '.runtime/node-a.secret')
uv run --locked python -m app.external_service --executor-id executor-a --node-id node-a `
  --secret-env NODE_A_SECRET --artifact-root (Join-Path $PWD '.runtime/artifacts') `
  --state-dir (Join-Path $PWD '.runtime/node-a') --port 8091 `
  --cpu-capacity 2 --mock-gpu-slots 'gpu-0,gpu-1'
```

节点 B 的独立终端只设置自己的密钥，运行不导入中心模块的实现：

```powershell
$env:NODE_B_SECRET = Get-Content -Raw (Join-Path $PWD '.runtime/node-b.secret')
uv run --locked python -I executors/standalone_mock.py --executor-id executor-b --node-id node-b `
  --secret-env NODE_B_SECRET --artifact-root (Join-Path $PWD '.runtime/artifacts') `
  --state-dir (Join-Path $PWD '.runtime/node-b') --port 8092 `
  --cpu-capacity 2 --mock-gpu-slots 'gpu-0,gpu-1'
```

业务通过原 API 提交 `execution_backend=external`，或运行 `uv run --locked python examples/business_sequence.py --business-id <稳定业务键> --backend external --processor-version independent-mock-v1`。每个 Worker 仍只监督一次执行，演示多设备并发需多个独立 Worker。清单中的 `accepting=false` 仅停止新派发，不强停既有计算；CPU 是调度配额，不是核心利用率。节点健康、真实 Attempt 位置、CPU/设备 owner 和事件在监控台的执行资源视图及 `/api/v1/nodes` 查询；页面不探测、不驱动执行或强制释放。

远端部署前必须提供实际机器、权限、网络和共享挂载。将清单 URL 改为受控 HTTPS origin，可设置中心本地 `ca_file`；执行器非回环监听必须同时提供 `--host`、`--tls-cert`、`--tls-key`。不支持关闭证书/主机名验证、代理或重定向。各机器共享根可为不同绝对路径，但须看到相同 `ARTIFACT_STORAGE_ID` 及 `_system/storage-id.json` 原字节；派发前实际验证该标记与本步输入/上游/检查点，不只相信路径字符串。中心不替用户建立远端共享盘或传文件。

保留密钥、产物及每个节点的 state-dir；命令固定该状态库的 `state_id`。同 URL/身份但换空状态库不能承认旧 job 已退出，也不能重启旧命令，资源继续隔离。地址/身份变更不能覆盖既有 Attempt 的路由。同机两个节点及本地 TLS 验证**不是**两台设备或网络分区容灾验收。

## 测试与恢复边界

自动测试使用真实 PostgreSQL、HTTP API 和子进程。首次建立独立测试数据库后运行：

```powershell
docker compose exec -T postgres psql -U pipeline -d pipeline -c 'CREATE DATABASE pipeline_test;'
$env:TEST_DATABASE_URL = 'postgresql://pipeline:pipeline@127.0.0.1:55432/pipeline_test'
uv run --locked pytest -q
```

未设置 `TEST_DATABASE_URL` 时跳过中央 PostgreSQL 集成；仍运行契约/媒体和独立执行器的 HTTP/子进程测试，不能当作中心闭环已验证。中央集成用例只在名称以 `_test` 结尾的数据库中创建并清理自己的随机 schema。配套外部集成见 `backend/tests/test_external_execution.py`，其协议/服务专项见 `backend/tests/test_external_service.py`；文档独立实现的真实接入见 `backend/tests/test_standalone_execution.py`，组件契约检查见 `backend/tests/test_standalone_contract.py`。后者的组件检查不代替真实 HTTP/PG 接入，不把本地后端结果冒充外部验收。

A12 数据库短断专项需要 Docker，并显式开启：

```powershell
$env:RUN_A12_POSTGRES_OUTAGE = '1'
uv run --locked pytest backend/tests/test_database_outage.py -q -s
uv run --locked pytest backend/tests/test_external_outage.py -q -s
Remove-Item Env:RUN_A12_POSTGRES_OUTAGE
```

专项自行建立一次性 PostgreSQL 容器并只对其停止、重启和清理，不停项目数据库、不删除项目卷；不要求 `TEST_DATABASE_URL`。覆盖租约内重连与租约过期后停止旧计算、拒绝旧写入和从已提交检查点接管。API 在数据库不可用时返回 `503 / DATABASE_UNAVAILABLE`；写请求响应丢失不代表事务未提交，重试必须复用原 `Idempotency-Key`。

当前专项共六项：本地两项，配套与独立 external 各两项。节点池的同机多身份/多设备、共享引用、故障/幂等及本地 TLS 检查在 `backend/tests/test_nodes.py`，007 历史兼容在 `backend/tests/test_node_migration.py`；不据此声称两台机器或节点池跨机停库通过。最新结果为全量发现问题后修复、定向复测通过，详见验收/开发记录。

默认 API + Worker 两个应用进程即可运行和恢复，不再要求启动 `app.coordinator`；该模块仅保留兼容入口，集成验收不启动它。Worker 空闲或正在监督长计算时都会扫描过期租约和退避；扫描不是心跳，不能替死计算无限续租。所有 Worker 都停止时无人执行扫描，重启一个 Worker 后才会接管。

在动画示例的 legacy 执行域，仅 `generate_frames` 使用单个 `mock-gpu` 槽位；managed-v1 按登记的资源种类执行，新节点池按节点/设备领取。争用时 Step=READY、wait_reason=WAITING_GPU，不产生计算 Attempt；其他任务的 CPU 素材准备不被槽位等待阻塞。租约失效会原子记录 LOST 并隔离旧槽位，**不是立即释放**。后续扫描按所选后端确认退出：本地验证主机、PID、创建时间，外部验证绑定原 Attempt / generation 的持久化退出 receipt。确认退出后才按原 owner 对账并释放，旧回调不能释放新所有者的槽位。

缺少 PID/创建时间、主机不匹配或权限不足时，保持 QUARANTINED，不能推断已清理。当前没有强制释放的 HTTP 接口；此类异常需要可信本地人工对账，不能靠修改租约或未经确认清空 owner 绕过。正常已登记本地执行器的硬退出/接管路径已纳入回归。`MOCK_GPU_SLOTS` 仅约束 legacy 域，允许 0/1，0 会为新任务冻结禁用策略并等待，不代表新增第二张卡。

验收使用真实 PostgreSQL、HTTP 和本地 CPU 执行进程；快测时钟不代表小时级实测，模拟槽位不代表 CUDA 或真实硬件清理。A12 两种真实数据库短断、页面重新查询/人工恢复/结果下载与桌面手机布局已直接验证。页面的 HTTP 断连注入仅验证页面行为，不替代 A12。详见开发记录和验收表。

## 核心目标

1. HTTP 提交后立即获得任务 ID，任务不依赖请求或页面连接存活。
2. 分开记录 Task、Step、Attempt；重试保留失败历史。
3. Worker 中断后从已持久化状态接管，而不是所有步骤从头开始。
4. 同任务恢复复用成功步骤/检查点；显式开启时另支持兼容跨任务缓存，保持来源可查。
5. 通过 API 和轻量监控台查询任务队列、依赖、执行历史、等待原因及节点所属模拟槽位。
6. 支持小时级运行，不把短租约当作模型运行上限。

## 长任务策略

**模型完成优先：租约周期性续期；默认不设置步骤硬执行时限和任务总截止时间。**

初始配置约定为 30 秒心跳、300 秒租约。两者用于存活检测和执行所有权，正常续期的步骤可以运行数小时。耗时超出软阈值只产生告警，不杀进程。确认的执行失败、显式硬时限、无法续租导致所有权失效仍需处理；仅提供已实现的基本取消，不建设复杂取消/抢占。故障演示使用独立快测配置。

这不是“无条件保证完成”：不可恢复输入、资源永远不可用、存储损毁、执行永久卡住或外部服务持续失败，仍需明确报错、告警或人工介入。详见 [长任务决策](ADR-001-long-running-execution.md)。

## 示例任务

当前六步为 normalize_asset → segment_subject / analyze_motion → generate_frames → encode_package → publish_result。合成输入产生 JSON，图片输入生成真实 GIF；不开放任意步骤定义或用户代码。历史三步仍按其冻结图执行，但不接受新建。

示例：[可直接提交的六步请求](../examples/animation-pipeline.json)、[六步冻结图与策略说明](../examples/animation-dag-v2.json)。前者用于 POST，后者是图定义参考，不直接作为请求体提交。API 不接受任意步骤列表。心跳/租约由进程配置提供，默认 30/300 秒，不允许提交请求关闭。

## 方案与文档导航

| 文档 | 用途 |
| --- | --- |
| [PRD](PRD.md) | 目标、用户场景、边界和成功条件 |
| [路线图](ROADMAP.md) | 阶段、优先级与完成条件 |
| [执行规范](SPEC.md) | 状态机、持久化、领取、恢复、缓存和 GPU 协议 |
| [实施计划](IMPLEMENTATION.md) | 开工顺序、模块划分及首个可运行闭环 |
| [验收场景](ACCEPTANCE.md) | 第一步实测与后续恢复/资源验收边界 |
| [长任务决策](ADR-001-long-running-execution.md) | 租约、超时、独立心跳和完成保障边界 |
| [开发记录](DEVELOPMENT_LOG.md) | 工具、token、实际工时、失败记录和交付说明 |
| [协作约定](../AGENTS.md) | 后续开发边界与规则 |

当前技术栈：Python 3.12.10 / FastAPI / psycopg / PostgreSQL / 独立 Worker 与计算子进程，附同源静态监控台；不创建完整前端体系。

## 本阶段停止条件

页面无创建入口、旧三步停止新建及冻结任务兼容继续保持；本次在动态顺序托管上增加预配置节点、节点所属预算/模拟设备、受控传输、共享引用检查与只读监控。必要验证、启动说明和记录完成即收尾，不扩展自动发现、新调度服务、任意作业接管、通用插件、动态并行或真实 GPU。跨设备部署须用户提供实际环境；提交、推送仍需单独授权。

所有“通过”结论须来自实际运行，记录在 [开发记录](DEVELOPMENT_LOG.md)。未连接真实模型/GPU，未做公网部署。

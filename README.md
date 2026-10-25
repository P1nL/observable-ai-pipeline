# Observable AI Pipeline

**题 D：可恢复、可观察、可复用的 AI 长任务执行管线 MVP。** 以动画素材处理为例，计算使用 CPU MOCK，任务状态、独立执行进程、PostgreSQL 持久化、检查点和结果文件均真实。

重点不是生成效果，而是回答：**执行到哪一步？为什么等待或失败？中断后从哪里继续？哪些结果可以复用？旧进程还占不占资源？**

- 技术栈：Python 3.12.10、FastAPI、PostgreSQL、独立 Worker/Supervisor、同源 HTML/CSS/JavaScript。
- 无需模型密钥、真实 GPU、Redis、npm 或前端构建；Docker Compose 只运行数据库。
- 页面用于监控、取消和人工恢复；通过 API 提交任务，没有前端上传/创建入口。
- 合成输入输出 JSON；通过 API 上传静态 PNG/JPEG 后可生成实际可预览的 GIF。AI 分割/动作分析/帧生成仍是 MOCK。

## 1. 题目完成情况与交付边界

| 题目要求 | 当前实现与证据 |
| --- | --- |
| 多步骤耗时任务、上游查询状态 | 异步提交返回任务 ID；六步依赖、Task/Step/Attempt、事件、进度与结果查询；轻量监控台 |
| 单步失败、进程中断后恢复 | 有限自动重试、退避、人工恢复、Worker 接管、已提交批次检查点与成功步骤保留 |
| 长时间任务 | 心跳与计算分离、租约可续期、默认关闭硬时限；跨多个租约周期的快测通过，未做小时级实测 |
| 步骤依赖与结果复用 | 固定六步分支/汇合；同任务复用，显式开启的跨任务兼容缓存及来源记录；缓存命中不造 Attempt |
| 状态、历史、可视化 | PostgreSQL 持久化；队列搜索/筛选/分页、依赖图、Attempt/事件/结果页签、资源异常提示 |
| GPU 调度和可观察性 | MOCK 设备互斥/排队/owner/失联隔离/退出确认；增强版有每节点预算/多个 MOCK 设备，不等于真实 GPU 验收 |
| 公网 URL 或公开源码 | 已有公开 GitHub 仓库；2026-10-25 本次核查时远端仍是文档基线，完整实现须推送后才构成可复现的公开交付；未部署公网服务 |

**判断：本地核心 MVP 与主要加分项已实现；“实现完成”不等于“公开交付完成”。** 还需源码推送及第 8 节的 token/个人投入说明。题目允许 MOCK，真实 GPU 与完整分布式调度不是为交付必须继续扩建的范围。

历史 219 项通过证据来自全量与修复后的定向复测，**不是一次全量 219 passed，也不是每次改动均重跑全量**。后续页面/提醒变更另有专项记录。本次交付核查另跑提交兼容、DAG/缓存、分支恢复、中断/fencing、资源互斥/隔离：**8 passed**。完整边界见 [验收场景](docs/ACCEPTANCE.md) 与 [开发记录](docs/DEVELOPMENT_LOG.md)。

## 2. 快速开始：先跑本地六步示例

前置条件：Git、PowerShell 7、uv、已启动的 Docker Desktop。以下终端均在仓库根目录。首次同步可能下载 Python、依赖和数据库镜像，不承诺固定启动耗时。

### 2.1 获取源码、启动数据库并迁移

> 以下 clone 面向完整源码推送后的仓库；若 checkout 缺少 `pyproject.toml`、`compose.yaml` 和 `backend/`，说明仍是文档基线。已有本地工作树请跳过 clone/cd，不要覆盖它。

```powershell
git clone https://github.com/P1nL/observable-ai-pipeline.git
cd observable-ai-pipeline
docker compose up -d --wait
uv sync --locked
$env:PYTHONPATH = Join-Path $PWD 'backend'
uv run --locked python -m app.db
```

迁移命令应用全部增量迁移（当前至 008），无需逐个运行 SQL。默认数据库为 `127.0.0.1:55432/pipeline`，本地演示用户名/密码均为 `pipeline`；文件在 `.runtime/artifacts/`。应用**不自动读取 `.env`**，自定义参数须在各终端显式设置，参见 [.env.example](.env.example)。

### 2.2 两个终端分别启动 API 和 Worker

终端 A：

```powershell
$env:PYTHONPATH = Join-Path $PWD 'backend'
uv run --locked uvicorn app.api:app --host 127.0.0.1 --port 8000
```

终端 B：

```powershell
$env:PYTHONPATH = Join-Path $PWD 'backend'
uv run --locked python -m app.worker --worker-id local-worker
```

一个 Worker 即可完成六步和恢复扫描。观察分支实际并行时，另开终端按同样方式启动 `--worker-id local-worker-2`；无需启动 coordinator。新终端需重新设置 `PYTHONPATH`。端口已有本项目服务时复用，不重复启动。

### 2.3 提交任务并查看结果

在另一个终端执行；首次绕过缓存以观察真实 MOCK 执行：

```powershell
$base = 'http://127.0.0.1:8000'
$request = Get-Content -Raw -Encoding utf8 examples/animation-pipeline.json | ConvertFrom-Json
$request.bypass_cache = $true
$body = $request | ConvertTo-Json -Depth 10
$headers = @{ 'Idempotency-Key' = [guid]::NewGuid().ToString() }
$task = Invoke-RestMethod "$base/api/v1/tasks" -Method Post `
  -Headers $headers -ContentType 'application/json' -Body $body
$id = $task.id
Write-Output "监控地址：$base/tasks/$id"
Invoke-RestMethod "$base/api/v1/tasks/$id"
```

若 POST 响应丢失，**保留 `$headers` 和 `$body` 重试该 POST**，不要为同一次提交重新生成键。浏览器打开输出的地址，或访问 `http://127.0.0.1:8000/`；接口文档为 `http://127.0.0.1:8000/docs`。新库是空队列，需先通过 API 提交。

预期六步最终为 `SUCCEEDED`，每次实际执行有 Attempt 和事件；本例产出 **JSON 清单，不是 GIF/MP4**。完成后在结果页签下载，或查询：

```powershell
Invoke-RestMethod "$base/api/v1/tasks/$id/events"
Invoke-RestMethod "$base/api/v1/tasks/$id/steps/generate_frames/attempts"
Invoke-RestMethod "$base/api/v1/tasks/$id/artifacts" |
  Select-Object id,kind,availability,download_url
Invoke-RestMethod "$base/api/v1/resources"
```

观察跨任务缓存：把 `bypass_cache` 改为 `false`（示例已开启 `cache_enabled`），保持输入/参数不变，用**新提交键**提交第二个任务。兼容且文件仍可读时前五步可为 `CACHED`，`publish_result` 始终独立提交。最小本地模式的 legacy 槽位通过 `/resources` 查询；页面节点面板仅展示已配置节点，未配置时为空，不代表没有执行。

停止时先在 API/Worker 终端按 Ctrl+C，再 `docker compose stop`。不要用 `docker compose down -v` 或删除 `.runtime` 来停止服务，它们保存恢复所需数据。关闭页面不取消任务；全部 Worker 停止时无人推进，重启后才扫描接管。

图片/GIF、动态顺序托管和外部节点配置见 [运行与部署手册](docs/RUNBOOK.md)，首次体验无需配置这些增强项。

## 3. 实现方式与步骤边界

```text
上游业务 / 监控页面 → FastAPI → PostgreSQL（执行事实来源）
                                  ↑
                     Worker / Supervisor（领取、续租、恢复）
                                  ↓
                  独立计算子进程 → 持久化文件（候选/检查点/产物）
```

API 不等待计算。Worker 在短事务内领取就绪步骤和资源、创建 Attempt；计算在子进程中进行，Supervisor 负责心跳和退出检测。进度/检查点/结果经中央执行权校验才能提交，监控查询本身不驱动执行。

默认动画图 `animation-dag-v2`：

```text
normalize_asset ─┬→ segment_subject ─┐
                └→ analyze_motion ──┴→ generate_frames → encode_package → publish_result
```

| 步骤 | 独立边界与恢复理由 |
| --- | --- |
| 素材归一化 | 校验输入、统一尺寸/格式；成功后保留，后续失败不重复处理 |
| 主体分割 / 动作分析 | 相同上游、互不依赖；分别失败/重试，并行受 CPU 预算约束 |
| 帧生成 | 等待两分支；代表昂贵 GPU 推理，以已提交批次为恢复点，避免整段重算 |
| 编码打包 | 与推理解耦；编码失败重用成功帧，当前图片路径产出真实 GIF |
| 结果发布 | 只提交本系统清单，不调用外部发布服务；不缓存，区分提交与计算复用 |

以**稳定输入输出、独立错误边界和可持久化中间结果**划分步骤，不按每个函数拆任务。真实模型只有提供可验证的批次/分片恢复点时才能沿用检查点机制；无恢复接口的推理只能整步重试，不能承诺模型内部任意位置续跑。

图版本、依赖、处理器版本、输入/参数及重试/时限策略随任务冻结。历史三步继续执行和恢复，但拒绝旧流程的新提交。增强模式 `managed-v1` 支持按需登记**顺序**步骤，须显式 finish 才关闭流程；不提供任意代码插件或 DAG 编辑器。

## 4. 状态管理与持久化选择

| 层级 | 状态 / 关键区分 |
| --- | --- |
| Task | `QUEUED → RUNNING → SUCCEEDED / FAILED`；显式恢复 `FAILED → RUNNING`；取消经 `CANCELING → CANCELED` |
| Step | `PENDING / READY / RUNNING / RETRY_WAIT / SUCCEEDED / CACHED / FAILED / BLOCKED / CANCELED` |
| Attempt | `RUNNING / SUCCEEDED / FAILED / TIMED_OUT / LOST / CANCELED`；重试/接管追加记录，不覆盖旧尝试 |
| 等待/资源 | `WAITING_GPU` 是等待原因，不造 Attempt；`LOST` 是执行权失效，不是物理进程退出 |

| 持久化内容 | 为什么保存 |
| --- | --- |
| Task 配置快照、幂等键、状态、恢复轮次 | 重启后可查询；不重复创建；升级不改写历史语义 |
| Step 依赖、状态、当前 Attempt/代次、重试时间、输出引用 | 接管时区分成功和待执行工作，不依赖内存队列 |
| Attempt 租约、心跳、进度、错误、进程身份及退出/清理状态 | 区分失败、失联和资源占用；拒绝过期写回，保留排障历史 |
| 检查点/产物元数据、指纹与文件 | 只恢复已提交且兼容的批次；检测损坏，大文件不入库 |
| 事件序号、缓存来源、资源 owner | 增量查询、来源追溯和原 owner 对账，不伪造耗时或进度 |
| 增强模式的命令、节点身份及业务登记 | 重投递幂等、绑定原节点/状态库、业务客户端恢复后继续登记 |

事务是权威提交边界，候选文件本身不代表成功。检查点/结果按 Attempt 隔离；提交验证 **当前 Attempt + generation + 有效租约**。缓存还验证输入/上游字节、版本、参数、seed、生成标识及产物可读性。

## 5. 长任务、失败恢复与 GPU

### 从秒级扩展到小时级

- **租约不是时限**：默认心跳 30 秒、租约 300 秒，健康计算持续续租；步骤硬时限和任务截止默认关闭，显式配置才生效。
- **心跳不是进度**：静默计算可续租，子进程退出必须被发现，不能无限续租假 RUNNING。软超时只告警。
- **检查点决定重算量**：按批次提交，权衡写入成本与重复计算；只恢复已提交、可读且兼容的数据。
- **不绑定 HTTP 生命周期**：API 快速返回；页面轮询和增量事件，断连提示数据过期，不把网络失败改成任务失败。
- **仍需真实长期验收**：小时级稳定性、磁盘/日志容量和保留策略、真实模型检查点兼容、GPU/驱动退出行为。快测证明跨租约语义，不证明这些环境边界已通过。

### 不同故障的行为

| 场景 | 行为 |
| --- | --- |
| 可重试的单步错误 | 有限次数、指数退避；耗尽后 FAILED；人工恢复保留成功前驱及历史 |
| 编码或一条分支失败 | 只恢复失败工作；成功帧/另一分支不重算，依赖未满足不启动后继 |
| Worker 被终止 | 过期记 LOST；核实旧进程/资源，再由新 Attempt 从已提交检查点继续 |
| 旧进程迟到写回 | 进度、检查点、结果等均经 fencing，不能覆盖新代次或释放新 owner |
| 数据库短断 | API 503、不新领取；租约内恢复可继续，过期走停止/接管；写请求重试沿用幂等键 |
| 产物丢失或损坏 | `ARTIFACT_UNAVAILABLE`，阻止错误复用/依赖推进，不篡改成功历史 |
| 取消 | 撤销派发/提交权，确认退出和清理后才 CANCELED；未知进程保持隔离 |

GPU 用 MOCK 槽位表达资源所有权：`AVAILABLE / RUNNING / QUARANTINED`，保存 owner、代次和节点。争用排队不造 Attempt。**丢失租约不是 GPU 已释放的证据**：本地按主机/PID/创建时间核实退出，外部按绑定原节点/状态库的退出回执对账；未知则隔离。

增强版提供每节点 CPU 配额/多个 MOCK 设备的预配置选择，无自动发现、抢占或新调度服务。页面显示预算/owner/等待/异常，不虚构显存、温度或硬件利用率。真实 GPU 指标与驱动/进程清理需另行接入验证。

## 6. 如何验证

先启动项目数据库，首次创建独立测试库（已存在则跳过 CREATE）：

```powershell
docker compose exec -T postgres psql -U pipeline -d pipeline -c 'CREATE DATABASE pipeline_test;'
$env:TEST_DATABASE_URL = 'postgresql://pipeline:pipeline@127.0.0.1:55432/pipeline_test'
uv run --locked pytest -q
```

中央集成只在名称以 `_test` 结尾的数据库中建立用例随机 schema，不清空开发库。未设 `TEST_DATABASE_URL` 会跳过中央集成，不能当作完整闭环通过。真实停库 A12 为 opt-in、用一次性容器，不停开发数据库，命令见 [运行手册](docs/RUNBOOK.md#测试与恢复边界)。

代表性用例：[中断/检查点/fencing](backend/tests/test_execution.py)、[DAG/缓存/恢复](backend/tests/test_dag_cache.py)、[资源等待/隔离](backend/tests/test_recovery_resources.py)。已有 GitHub Actions 普通回归配置，不以配置文件存在宣称远端 CI 已通过。

## 7. 时间规划与实际失败案例

三天是**规划窗口，不是个人实际工时**：Day 1 跑通数据库/API/Worker/固定三步；Day 2 实现重试、租约、fencing、检查点和模拟资源；Day 3 做轻量页面、故障验收、修复及文档。六步 DAG/缓存、监控、动态托管和节点池是后续授权增强，不倒推为原 MVP 必选项。见 [路线图](docs/ROADMAP.md)。

以下是已记录的实际问题，不是虚构的故障故事：

| 问题 | 原因、修正与边界 |
| --- | --- |
| 初期 Worker 中断后过期 RUNNING 无人推进 | 最小启动缺恢复扫描；后续纳入 Worker 独立维护线程，验收不再依赖额外 coordinator |
| 初期跨租约测试可能失真 | 以 batch_count 判断批次执行，跳过普通 smoke 的 duration；改按 checkpoint_mode 分流并保留时长回归 |
| 多节点并发死锁/健康作业丢租约 | 节点/资源锁范围过大，观察循环重复写静态配置；收窄锁并取消重复写入；复测验证健康作业单 Attempt，未放宽 fencing |
| 停库专项日志解析失败 | 并发日志拼行；添加日志写锁，不通过忽略坏记录掩盖问题 |
| 历史进程身份缺失，无法安全释放 | 无法证明退出；保留未知占用，提醒“确认知悉”不等于资源释放，不伪报自动恢复成功 |

不支持任意模型内部恢复、恰好一次计算、永久存储损毁自动灾备、任意代码插件或完整分布式调度。真实跨机/网络分区、小时级、真实 GPU/模型未验收。详见 [开发记录](docs/DEVELOPMENT_LOG.md)。

## 8. AI coding 与个人投入说明

| 题目所需说明 | 可确认内容 / 待补充 |
| --- | --- |
| 使用工具 | Codex；此前部分实现使用经用户授权的有边界子代理，阶段记录见开发日志 |
| 大概花费 token | **待补充**：没有完整 usage 汇总，不能把重复累计值相加或从代码行数倒推 |
| token 最多的部分 | **待补充**：需要阶段 usage，不能以代码最多或测试最久代替结论 |
| 个人时间最多的部分（不含 LLM 直接工作） | **待作者填写**：在哪项工作、约多少时间、具体做了什么；当前未提供，不用自动执行时长代填 |

正式交付前请补充以上三项；可提供注明口径的区间估计，无需虚构精确数字，也不要上传私人完整会话或密钥。

## 9. 文档导航

| 文档 | 用途 |
| --- | --- |
| [运行与部署手册](docs/RUNBOOK.md) | 图片/GIF、完整监控行为、动态托管、外部执行器/节点、故障专项 |
| [执行规范](docs/SPEC.md) / [长任务决策](docs/ADR-001-long-running-execution.md) | 状态机、事务、租约、检查点、缓存和协议 |
| [验收场景](docs/ACCEPTANCE.md) / [开发记录](docs/DEVELOPMENT_LOG.md) | 历史证据、实际失败、修正与未验收边界 |
| [需求](docs/PRD.md) / [路线图](docs/ROADMAP.md) / [实施计划](docs/IMPLEMENTATION.md) | MVP 与后续授权范围 |

源码尚未选择许可证；数据库卷、运行文件、密钥与日志不进入 Git。本项目未提供公网演示服务。

# 执行规范 v0.8：中心授权、步骤托管与预配置节点

状态：第 13 节动态顺序托管继续保留，最新授权为第 14 节预配置多节点与节点所属资源；早期单执行器/单槽位限制仅适用于 legacy 兼容域，不再限制新节点池。原三步 MVP、六步示例、缓存、媒体和恢复记录保留。与 [PRD](PRD.md)、[长任务决策](ADR-001-long-running-execution.md) 共同作为基线；不建设自动发现、任意代码插件、通用编辑器、外部发布服务或真实 GPU。实际验收与未覆盖边界见 ACCEPTANCE.md 和开发记录。

## 1. 组件边界

- API：持久化提交、查询、恢复、重试、取消；不执行耗时模型。
- 协调逻辑：按冻结依赖推进、退避到期、失联检测、取消清理与汇总。Worker 的后台维护线程扫描，不占唯一续租路径，不要求额外协调服务。多 Worker 以短事务串行修改同一任务；旧 coordinator 仅兼容入口。
- Worker Supervisor / 执行管理核心：领取、依赖推进、续租、Attempt/代次、结果提交、资源清理；不嵌入具体计算逻辑。
- 执行后端：统一启动、非阻塞观察、停止、退出确认与候选事件交互；local 管理本地子进程，external 通过认证 HTTP 管理独立执行器中的单步骤作业。
- Executor 子进程：运行 MOCK 或未来的模型，生成进度、检查点候选与产物。
- PostgreSQL：任务、历史、执行所有权和节点所属模拟槽位的事实来源；实际计算后端仍是 CPU MOCK。
- 持久化文件卷：输入、中间结果、检查点与最终产物。

长计算不持有数据库事务。不因 HTTP 请求结束、浏览器关闭或 API 重启而取消任务。

## 2. Task / Step / Attempt

### Task

`QUEUED → RUNNING → SUCCEEDED | FAILED`；仅显式人工恢复允许 `FAILED → RUNNING`。基本取消为 `QUEUED/RUNNING → CANCELING → CANCELED`，未确认退出/清理时保持 CANCELING；取消与成功提交按 Task 行锁决定先后，不能同时成为权威结果。

若显式配置的任务截止时间在排队期间到达，也允许 QUEUED → FAILED，原因记录为 TASK_DEADLINE，不创建虚假的执行 Attempt。

- RUNNING 包含资源等待、自动重试和失联恢复，通过 phase/wait_reason 区分。
- 固定示例的 SUCCEEDED 要求冻结图全部必要步骤 SUCCEEDED/CACHED，且终端引用已提交：历史三步终端为 encode_package，六步为 publish_result。managed-v1 还必须显式关闭登记，全部已登记步骤成功、末步骤输出已提交且没有未确认退出；开放流程即使当前步骤全成功也只进入 WAITING_STEPS。
- 某必要步骤最终失败：停止新派发，允许其他在途步骤收尾，必要后继标记 BLOCKED，随后 FAILED。
- 取消 API 只写入请求和撤销派发/提交权，不在 HTTP 请求内等待计算。监督器/扫描器停止登记进程、确认退出后终结；未知身份不伪装成已清理，无优先级/抢占。

### Step

本次流程状态：PENDING、READY、RUNNING、RETRY_WAIT、SUCCEEDED、CACHED、FAILED、BLOCKED、CANCELED；每次计算单独记录 Attempt，缓存命中不创造虚假 Attempt。

- PENDING 在必要依赖全部满足后进入 READY。
- READY 可实际领取为 RUNNING，或验证兼容来源后成为 CACHED；同任务恢复保留原 SUCCEEDED/CACHED 步骤与产物。
- RUNNING 成功进入 SUCCEEDED；可重试失败进入 RETRY_WAIT；不可恢复或预算耗尽进入 FAILED。
- RETRY_WAIT 到期进入 READY。
- FAILED 与 BLOCKED 仅通过显式恢复操作重新评估；成功步骤不被清空。
- 等待模拟 GPU 使用 READY + WAITING_GPU，不占 Attempt 或伪造执行时长。legacy 保持 0/1 策略，配置节点按第 14 节分别领取设备；不提供任意显存请求或通用 UNSCHEDULABLE 机制。

### Attempt

状态：RUNNING、SUCCEEDED、FAILED、TIMED_OUT、LOST、CANCELED。

一次领取创建一个 Attempt。重试与接管创建新的记录，不覆盖历史。LOST 是执行所有权失效，不是物理进程停止证明。另记录 stop_requested_at、process_exit_confirmed_at、resource_cleanup_state。

## 3. 持久化模型

| 表 | 关键字段与约束 |
| --- | --- |
| tasks | id、idempotency_key、request_fingerprint、pipeline/config_snapshot、status、phase、created_at、deadline_at(nullable)、recovery_round、next_event_seq、registration_closed（旧任务默认 true） |
| steps | task_id+step_key 唯一、固定 depends_on、processor_version、status、current_attempt_id、execution_generation、next_retry_at、checkpoint_artifact_id、output_refs、检查点兼容键；动态步骤另有 registration_order 和 input_snapshot |
| step_attempts | id、step_id+attempt_number 唯一、recovery_round、worker_id、fencing_token、lease_expires_at、heartbeat_at、started_at、finished_at、progress、error_code、retryable、executor_host/pid/created_at、resource_slot_id、stop/cleanup fields |
| artifacts | id、kind、immutable_uri、content_fingerprint、producer_attempt_id、batch_index/context、committed_at、availability |
| task_events | task_id+seq 唯一、type、step_id/attempt_id、payload、created_at |
| resource_slots | 固定 id=1 的本地模拟槽位、state、host_id、owner_attempt_id、owner_generation；不建立通用节点发现表 |
| execution_commands | Attempt UUID 为稳定命令/job ID、不可变绑定与信封、派发观察和退出 receipt |
| business_operations | task_id+idempotency_key 唯一、register/finish 操作、请求指纹、原 Step 引用；与登记或关闭同事务 |

新任务可用 managed-v1 按需登记顺序步骤（见第 13 节）；默认保留 animation-dag-v2 六步示例：normalize_asset → segment_subject / analyze_motion → generate_frames → encode_package → publish_result，帧生成等待两个分支。服务端验证唯一 key、依赖存在且无环，不接受任意步骤/代码。历史 animation-v1 的三步冻结图、执行/恢复、JSON 结果与历史保留，不再接受新建。大文件不入数据库；关键阶段写事件，不逐帧写历史。002 模拟槽位、003 uploaded_inputs、004 step_cache、005 外部命令均保留；006 增量添加业务登记字段/操作表，不改写旧图/Attempt/成功引用。

任务配置冻结处理器版本、重试/时限/槽位策略、resource_kind、依赖和输入。上传输入必须是本系统存在且可读取的 UUID，冻结实际文件指纹、尺寸和引用，拒绝请求提供路径。新快照 execution_schema_version=2；旧快照继续使用原检查点兼容键，不因增加缓存协议而失效。迁移前 CPU 步骤不自动改成 GPU。

PNG/JPEG 上传 raw body 最大 8 MiB，解码最大 16MP/边长 8192，拒绝多帧图和不支持格式；不接视频。归一化最长边 256、最多 120 帧及有界总像素预算。输入、PNG 帧和 GIF 真实；AI 分割/分析/生成仍为确定性 MOCK。媒体不可变且随 JSON 引用通过原 fencing 提交，未提交媒体候选不可下载。所有嵌套媒体验证实际字节指纹、格式/解码及引用链，不信任文件后缀。

## 4. 原子领取、租约与 fencing

领取在短事务中完成：

1. 选择依赖已满足、所属 Task 可运行的 READY 步骤并加行锁；多 Worker 使用 FOR UPDATE SKIP LOCKED。
2. generate_frames 在同一事务中锁定唯一模拟槽位，必须 AVAILABLE 且为本机可用；否则记录 WAITING_GPU，不创建 Attempt。等待者不能阻塞其他任务的 CPU 准备。
3. 增加 execution_generation，创建 RUNNING Attempt，设置 current_attempt_id 与租约，占用槽位，写事件。
4. 提交事务后启动执行子进程。命令行绑定 Attempt/generation，先确认本地进程身份并登记主机/PID/创建时间，fencing 成功后才发送计算配置。启动失败也需要结束 Attempt 并对账资源。

所有权的比较、续租与接管使用数据库时间。续租必须满足当前 Attempt、相同 generation、有效租约和允许执行的任务状态；已过期租约不能被旧 Worker 复活。

进度、检查点和最终结果提交均使用同样的条件更新。新 Attempt 领取后，旧 Attempt 即使进程恢复也无权改写状态。协调器标记 LOST 与撤销当前所有权必须原子完成。

同一时刻只承认一个有效所有者，但网络故障时旧物理计算可能仍存在。因此不承诺 exactly-once 计算，只限制权威提交和资源复用。

## 5. 长时间执行策略

默认 long-running 配置：

| 字段 | 默认值 | 语义 |
| --- | --- | --- |
| heartbeat_interval_seconds | 30 | Supervisor 续租与存活报告周期 |
| lease_ttl_seconds | 300 | 最近一次成功续租后的所有权期限，可持续延长 |
| coordinator_scan_interval_seconds | 5 | 状态协调与失联扫描周期 |
| attempt_hard_timeout_seconds | null | 默认关闭；显式设置时从每次 Attempt 开始计时 |
| task_deadline_seconds | null | 默认关闭；显式设置时自任务创建起包含排队/重试 |
| step_soft_warning_seconds | 21600 | 从步骤首次执行开始的累计墙钟耗时告警，不杀进程 |
| task_soft_warning_seconds | 86400 | 从提交起的总耗时告警，不自动判失败 |

环境变量模板中的 0 对时限/告警字段表示关闭，加载后规范化为 null；不能解释为立即超时。实例配置优先级：显式请求覆盖允许覆盖的默认值，再将最终值冻结到任务。资源安全与租约约束不允许请求任意关闭。

Supervisor 与计算子进程分离，不能在模型计算循环里顺便发心跳。Supervisor 必须检查子进程退出，否则会为已死计算无限续租。子进程活着但无进度时只告警，不把推理中的静默当成死亡。

如果无法续租，在有效期内重试连接；租约到期仍无法确认所有权则停止权威写入，尝试停止子进程。临时文件可保留为候选，但不得越权提交检查点。GPU 清理按第 9 节处理。

显式硬超时需记录 ATTEMPT_TIMEOUT 或 TASK_DEADLINE，先请求停止并确认退出/隔离，不能立刻把 GPU 当作空闲。HTTP 请求超时不自动变成计算超时。

测试可使用独立 fast-recovery 配置（例如心跳 1 秒、租约 5 秒、扫描 1 秒）；必须在测试启动时明确选择，不写回默认配置。跨租约验证的步骤应持续至少三个租约周期。

## 6. 错误、重试与人工恢复

- 默认每个步骤每个 recovery_round 最多 3 次 Attempt，含首次执行。LOST 也消耗一次尝试。
- 临时外部错误和执行器中断可重试；输入错误、无效配置、确定的相同资源下 OOM 不盲目重试。
- 指数退避、抖动与最大间隔；next_retry_at 持久化；等待不占 GPU。
- 不可恢复错误或次数耗尽进入 FAILED，保留已成功产物。
- 人工 resume 或 retry 仅对 FAILED Task 生效，使用客户端操作幂等键，增加 recovery_round 并记录操作者动作。不无限抹掉历史计数。
- resume 重开所有失败步骤并重评估 BLOCKED；单步 retry 仅在目标是任务中唯一 FAILED 步骤时允许，重开该步骤及其必要后继。如果还有其他独立失败步骤，返回冲突并要求 resume，避免全局失败汇总立即终止局部恢复。
- 正常自动重试期间不允许人工并行启动第二个 Attempt。
- 失败后只在兼容的原配置下恢复；改变模型、输入或步骤参数应新建 Task。

## 7. 检查点与产物提交

产物按 Task/Step/Attempt 隔离写入，不覆盖其他 Attempt 的已提交文件。文件全部写完且可读取后，才在受 fencing 保护的事务中登记 artifacts、更新步骤状态和写事件。

- 文件写完、数据库提交前崩溃：文件仍是未引用候选，不能自动认定步骤成功。
- 数据库提交后进程崩溃：以已提交状态为准，不因 Worker 没收到响应就撤销结果。
- GPU 释放与结果提交分开追踪；即使结果提交，也需确认执行进程结束才释放槽位。

generate_frames 每批保存产物，检查点记录已提交批次、输入/处理器版本和恢复所需上下文。恢复前验证兼容性，只跳过已提交且可用的批次；中途批次重新计算。当前产物为明确标注 source=mock 的 JSON 合成数据，encode_package 直接提交最终清单，不模拟真实视频编码成功。

百分比不是检查点。不支持恢复协议的模型只保证步骤级重跑。MVP 不自动收集任意模型/GPU kernel 的内部状态。

已提交文件丢失时标记不可用并阻止当前依赖，给出 ARTIFACT_UNAVAILABLE，不篡改历史成功。新任务的跨任务缓存查验遇到失效来源时不命中、重新计算；这不等于恢复任意存储损毁。

## 8. 同任务结果保留与提交幂等

提交幂等与恢复复用是两种不同机制：

- 同一 Idempotency-Key + 相同请求指纹返回原 Task；相同键不同请求返回冲突。
- 同一任务恢复保留 SUCCEEDED 步骤、产物引用和历史；失败步骤才能创建新 Attempt。
- 检查点兼容键包含步骤/处理器版本、规范化参数、上游实际产物指纹、合成输入和 seed；只接受已提交且可用的批次。
- API 默认 cache_enabled=false，演示请求显式开启；页面不再创建任务。bypass_cache=true 强制计算，generation_tag 参与兼容键。键含作用域、流程、步骤/处理器版本、规范化参数、seed/生成标识、实际输入/上游字节指纹；相同图片不同上传 UUID 可复用。
- 仅 Task 成功事务发布其实际成功步骤缓存条目，查验来源 Task/Step 成功、引用仍相同且所有媒体可读。取消/失败任务、候选不成为来源。Step=CACHED 时保留共享引用并记录 source Task/Step，不新建 Attempt。
- publish_result 永不跨任务缓存；只提交本系统的新 JSON 引用，可引用 encode_package 已提交媒体，不访问外部发布平台。不承诺 exactly-once 计算、同键并发合并或缓存清理平台。

CPU 步骤领取由数据库咨询锁与活动/未确认退出计数限制，CPU_MAX_CONCURRENCY 默认 2（1–4）；API 和所有 Worker 使用一致配置，一个 Worker 一次监督一个执行，两个 Worker 可实现两分支并行。失联 CPU 未确认退出仍占预算；扫描通过所选后端确认退出，本地检查 PID/创建时间，外部检查绑定作业的退出 receipt。GPU 仍是独立唯一模拟槽位，不为并行关闭互斥。CPU 容量不是机器核心发现或多节点调度。

MVP 只有单用户/单作用域，不建设多租户或跨用户复用机制。

## 9. 原 MVP 的单本地模拟 GPU 槽位（legacy）

三天 MVP 仅一个模拟槽位：互斥、等待、失联隔离和确认后再释放。数据库状态真实，计算全部运行在 CPU，不建设多节点发现或真实 GPU 指标采集。

- 唯一槽位同一时刻只归一个 Attempt / generation，申请与领取在同一事务。
- 等待步骤展示 WAITING_GPU，不先创建虚假的计算 Attempt。
- 正常退出确认后释放；故障或显式硬超时先停计算，确认退出后再复用。
- 失联时在同一事务记录 LOST、撤销权威步骤所有权并将槽位置为 QUARANTINED，不直接释放。后续扫描在数据库事务外确认/停止主机、PID、创建时间一致的旧执行器，再在短事务中按原 owner Attempt/generation 释放，不能把租约过期当作进程停止。
- PID 已复用代表原进程退出，不能杀掉复用该 PID 的新进程。身份缺失、权限不足或其他主机保持 QUARANTINED，等待可信本地人工对账；不提供未核实的强制释放 HTTP 接口。
- 清理确认只补充物理退出事实并释放匹配的旧槽位，不允许旧 Attempt 更新计算进度、检查点、结果或新 owner。已有成功结果与资源清理状态分别保留。
- 只记录 source=mock、槽位状态、占用 Attempt、等待/执行时长和清理状态；不承诺 CUDA、显存隔离、真实温度/利用率、DCGM 或多节点发现。

## 10. API 与事件协议

统一前缀 /api/v1。已实现提交、列表/详情、事件、Attempt、JSON/GIF、上传、resume / retry、基本 cancel 和资源查询；不返回未实现功能的占位成功。

| 方法与路径 | 用途 |
| --- | --- |
| POST /tasks | 默认六步新任务；Idempotency-Key；提交成功或已有键匹配重放返回 202 |
| GET /tasks | 列表、状态过滤和分页 |
| GET /monitor | 只读全库计数、可搜索/筛选分页队列、步骤/真实尝试汇总、等待与待确认退出执行器 |
| GET /tasks/{id} | 任务快照、步骤、有效配置、等待原因、结果引用 |
| GET /tasks/{id}/events?after_seq=... | 按任务内序号增量读取历史 |
| GET /tasks/{id}/steps/{step_id}/attempts | 每次执行、错误、租约、资源与恢复来源 |
| POST /tasks/{id}/resume | 人工恢复失败任务 |
| POST /tasks/{id}/steps/{step_id}/retry | 人工重试指定失败步骤 |
| GET /tasks/{id}/artifacts | 产物元数据与安全下载引用 |
| GET /tasks/{id}/artifacts/{artifact_id} | 仅下载该任务实际引用且可用的产物 |
| GET /resources | 唯一本地模拟槽位、owner Attempt/generation、隔离及清理状态；source=mock、physical_backend=cpu |
| POST /inputs | 保留的后端 PNG/JPEG raw body 上传与稳定输入引用；页面无上传入口 |
| GET /inputs/{id}、/inputs/{id}/content | 读取合法输入元数据及原图，校验路径/指纹 |
| POST /tasks/{id}/cancel | 幂等请求取消，不阻塞等待进程清理 |
| GET /tasks/{id}/artifacts/{artifact_id}/preview | 仅本任务已提交引用且完整可读的 GIF，正确 image/gif 类型 |

状态变更与事件同事务提交。锁定 Task 行分配任务内 seq，防止游标读取越过未提交事件。选中任务约 2 秒轮询，全局监控约 3 秒轮询，不依赖 WebSocket 在线状态；重新连接从数据库恢复。

`GET /monitor` 使用 PostgreSQL REPEATABLE READ / READ ONLY 事务生成单次一致读模型，不推进任务、不创建 Attempt/事件。status 为 Task 状态；q 为 UUID 或流程标识的大小写不敏感字面子串（最多 64 字符）；limit 默认 20、范围 1–100，offset 非负。前端每页 10 条；全库 counts / total / waiting 不随筛选变化，filtered_total 与 items 应用相同过滤，按创建时间和 ID 降序。completed_steps 含 SUCCEEDED/CACHED，attempt_count 只统计真实 Attempt，heartbeat_at 只取 RUNNING Attempt。unconfirmed_count 为未确认物理退出的 CPU_PROCESS_UNCONFIRMED / QUARANTINED 数量，unconfirmed_executors 最多列最近 5 条，不代表进程仍在运行。observed_at 是数据库观测时间；分页请求、任务详情及独立资源查询之间不承诺同一原子快照。原 GET /tasks 返回结构不变。

提醒知悉（008 增量）：`monitor_acknowledgements` 只保存 Attempt 对应的知悉时间和当时的 status / resource_cleanup_state / stop_requested_at。`POST /api/v1/monitor/attempts/{attempt_id}/acknowledgement` 请求体必须携带这三个观察字段；锁定该 Attempt 校验仍未确认退出、仍属待核实状态且观察一致，过期请求返回 409 / MONITOR_CONDITION_CHANGED。相同条件重复请求保留原知悉时间；不存在的 Attempt 返回 404。`DELETE` 同路径幂等移除知悉标记、恢复提醒。两者都不更新 Task、Step、Attempt、槽位、退出证明或执行事件，也不调用执行器。此本地监控功能不引入账号系统或身份审计。

GET /monitor 保持 unconfirmed_count / unconfirmed_executors 的物理退出未确认语义，另增 unacknowledged_count / unacknowledged_executors 和 acknowledged_count / acknowledged_executors；两个列表各取最近 5 条、各自计数全量，已知悉记录不得挤掉更早的待处理项。只有知悉快照三个字段与当前 Attempt 一致时才移出顶部提醒，条件变化自动回到待处理；真实退出确认后不再计入任何未确认分组。前端仅在服务端确认与重新查询后移走提醒，失败/响应不明确不据此隐藏。已知悉记录仍显示“退出未核实、资源未释放”、原尝试链接、知悉时间和恢复提醒入口。

数据库连接不可用时，API 返回 503 / DATABASE_UNAVAILABLE 和 Retry-After。响应丢失不证明写事务未提交；创建/恢复写请求重试沿用原 Idempotency-Key，不在请求内启动计算。

新建准入：pipeline_id 缺省为 animation-dag-v2，公开 OpenAPI 同时列 animation-dag-v2 和 managed-v1，默认示例不变。animation-v1 新键返回 422 / PIPELINE_RETIRED，不写 Task/Step/事件；已有键先按原请求匹配，相同请求可返回旧任务，显式改变流程/输入仍 409。对已有键省略 pipeline_id 时使用该键原版本匹配，不能把历史重试改成新六步。内部请求解码保留旧标识仅用于此兼容；smoke-v1 新建需要显式 ENABLE_TEST_FAULTS，否则 422 / INTERNAL_PIPELINE_DISABLED，不是当前产品流程。已有任务执行与 resume 不经过新建准入，不重新构造其冻结图。

现有 API 同源提供 `/` 和 `/tasks/{id}` 的 AIGC 任务调度与监控平台，页面壳不依赖数据库可用。左侧队列/全库计数，右侧按冻结依赖绘制步骤图，详情以生成工作流、执行记录、任务事件、生成结果与资产页签组织；事件按 seq 增量去重。HTTP 失败保留快照并提示过期，不修改业务状态。页面保留基本取消和失败任务人工恢复，未确认操作继续保留原幂等键。资源入口展示容量、模拟槽位及未知退出历史，允许独立提醒知悉操作，不提供未经核实的释放操作。进度、Attempt、槽位及产物来自 API，不创建浏览器侧执行引擎、假曲线或硬件指标。

展示采用按需展开：资源摘要并入概览，输入素材/生成配置优先展示，任务技术标识与时限默认折叠，步骤详情仅在点击节点时出现。完整 UUID 直达与搜索共用输入框；页面没有上传/新建入口，保留一个刷新入口、取消、人工恢复与独立提醒知悉。任务由上游或演示命令调用 API 提交，后端上传/媒体能力和既有 GIF 读取保留。新任务可按下节选择步骤执行后端，不改变页面边界。统一连接提示保留两路故障来源，不能隐藏实际错误、过期数据或真实未确认退出状态。

产品化展示只映射显示名称：顶部持续标明模拟演示环境，SAM 2/Wan2.1-I2V 是已知工作流步骤的演示模型标签，不写入冻结任务、处理器配置或节点路由。任意自定义步骤和未知事件仍保留原标识；已知标识的原值在技术信息中可查。输入图片走既有只读内容接口，种子/目标帧数来自冻结配置，结果格式/尺寸/帧数来自已提交资产。不存在的提示词/时长/分辨率不补造；总耗时明确包含排队，CPU_ONLY 不能当作进程退出证明。本轮为桌面内容调整，不新增移动端适配或模型/硬件接入。

日志包含 task_id、step_id、attempt_id、worker_id 和错误码，不记录密钥。业务进度报告已完成单位/总单位，未知时长显示阶段与耗时，不按时间伪造百分比。

公开仓库不等于开放写 API。若后续公网部署，创建任务、恢复和故障注入必须限制访问与并发；不得允许上传任意代码执行。

## 11. 实现与验证范围

以当前需求和验收标准约束设计与测试，优先采用满足需求的最简单完整方案，不为假设性的未来场景增加复杂度。

- 新增抽象、依赖、恢复分支或测试前，应能说明其保护的当前需求、直接受影响保证、已支持的安全边界或已知回归。没有具体用途，不提前建设。
- 验证本次变更及直接受影响的调用链、数据和恢复路径，不扩展为无关的防御性测试矩阵。按阶段执行 [验收场景](ACCEPTANCE.md)，不以固定测试数量或覆盖率数字代替行为正确性判断。
- 持久化状态、Attempt 历史、租约续期、旧执行器提交隔离、检查点、产物提交和 GPU 安全释放直接对应项目要求，不能作为“过度设计”省略。取消、幂等及其他已承诺能力也需在实现时验证。
- 相关代码、配置、运行条件及最终状态未变化时复用已有有效结果，不重复执行相同验证。纯文档变更不触发无关的整套运行验收；会影响执行契约的配置变更仍需针对性验证。
- 不为证明完成而额外生成无实际消费方的哈希清单、证明文件或验证脚本；缓存键、产物指纹及真实完整性协议所需机制不受此限制。
- 保留明确要求的验收、已有强制检查、安全边界和已知故障回归。不把减少测试当作删除现有保证的理由。
- 当前请求范围内的实现和必要验证完成后收尾，不追加无关优化、重复审查或新清单。未覆盖能力和验证边界如实记录，不虚报通过。

## 12. 执行后端与步骤级托管

### 模块与新任务路由

- repository.py / worker.py 保留权威状态、依赖、Attempt、重试、租约、提交与资源决策；业务计算仍在 executor.py / media.py。
- backends.py 的 ExecutionHandle 规定 activate、poll、can_heartbeat、renew、stop、close 和退出 receipt / 消息队列。local 复用原 stdin/stdout 子进程，external 的 HTTP I/O 在线程中运行，不阻塞唯一监督路径；不引入插件目录或调度服务。
- 新提交字段 execution_backend=local（默认）或 external，随配置冻结。external 只绑定服务器配置中的一个 executor_id，调用方不能提交 URL/命令/路径；未配置时 422。旧快照缺少路由字段按 local 运行，旧键缺省 local 的请求指纹兼容，不改写历史。
- 第一版 external 可覆盖新六步任务的每个 Attempt，但中心仍逐步选择和推进。发给执行器的是单个已批准的 StepSpec、绑定身份、已提交依赖和检查点，不是让它调度整张图。不接管任意已有外部作业。

### 持久化启动与认证

- 005 为 Attempt 增加 execution_backend、executor_id、external_job_id；execution_commands 与 Attempt 同事务插入，保存不可变单步骤信封、绑定和稳定命令 ID。command ID / job ID 都等于本次 Attempt UUID，真正重跑必须新建 Attempt / generation。
- 监督器只在命令已持久化后发送 PUT /v1/jobs/{job_id}。服务 SQLite 在 Popen 前提交启动意图、请求指纹与绑定；相同 PUT 返回原作业，不因响应丢失重启计算。不同内容或身份拒绝；服务崩溃留下无 PID 的 STARTING 时保持 UNCERTAIN，不能推测“未启动”而再执行一次。
- 每个接口校验绑定 executor_id/job_id/task_id/step_id/attempt_id/generation/worker_id 的 HMAC capability，恒时比较；URL 必须是配置的回环 origin，中心不使用环境代理或跟随重定向。服务只监听 127.0.0.1，不提供开放文档或任意程序执行。密钥不进入任务快照、计算子进程或 Git，执行器不使用中央数据库凭据。
- 配套服务只启动固定 app.executor 和批准的 MOCK 步骤；独立实现只启动其预先安装的固定计算程序，先持久化 PID/创建时间/主机，再发送 stdin；共享根仅由服务 CLI 指定。请求内任意 artifact_root、command 或不支持的处理器拒绝，原路径/媒体引用验证继续适用。

### 租约、候选事件与退出事实

- PUT 携带数据库租约的绝对 UTC 到期时间；只有中心成功续租后才能通过 POST /lease 延长。默认仍 30/300 秒，正常计算可无限跨租约周期；RPC 超时只约束一次网络调用，不是计算时限。已过期/停止的作业不能续期复活。
- 服务 HTTP/独立 watchdog/计算子进程分离，观察 PID 真实退出。中心只有近期观察到 RUNNING 才续中央租约，服务健康接口不代表计算活着；心跳不改写业务进度。外部续租请求失败也继续独立查询终态事件，不能因作业已退出、拒绝续租而漏掉最终输出。
- GET /jobs 返回带 seq 的持久化 progress/checkpoint/result/error 候选。正常退出仅在 stdout EOF、事件写完且物理退出后公布；服务/输出 reader 丢失时可以确认物理退出，但不得补造 returncode=0 或成功结果。中心的 progress、checkpoint、succeed 都经当前 Attempt/generation/worker/有效租约和后端绑定校验，最终事务仍二次 fencing。
- execution_commands.dispatched_at / EXTERNAL_COMMAND_OBSERVED 仅表示已观察到命令，不是业务成功。exit_confirmed_at 是绑定作业的物理退出确认；STEP_SUCCEEDED 与产物提交另行决定。网络调用成功、进程存活和已提交进度不得合并成同一种状态。
- 产物采用同机共享真实目录。服务仅写候选文件，中央验证引用、指纹、命名空间和兼容性后登记；后续 local 或 external 执行器只能复用已提交依赖/检查点，不扫描孤立候选提升为成功。未做跨机同步、对象存储或任意模型内部检查点恢复。
- POST /stop 是请求，只有已核实进程退出或未启动墓碑才确认退出。先停止一个尚不存在的 job 会持久化墓碑，拒绝迟到 PUT；不确定启动窗口不能被墓碑伪装成未启动。中央取消/故障/失联清理在数据库事务外请求后端，再按原 owner/generation 清理；服务不可达、身份不明保留隔离与 CPU 占用。
- 迟到退出 receipt 只能记录原 Attempt 的物理事实，不能提交进度/结果、复活旧代次或释放新 owner。服务重启停止可核实的旧 PID，由中心按重试策略创建新 Attempt，并复用中央已提交检查点；无法核实身份时等待可信人工对账。

本地与外部路径分别验收。以上为第一版单回环服务的兼容契约；第 14 节新增预配置节点、验证 HTTPS 与共享挂载检查。不承诺跨机时钟/网络分区容灾、自动发现、完整权限平台或 exactly-once 计算。

### 12.1 可独立实现的 HTTP 协议 v1

以下是接入契约，不要求导入中心 Python 模块。执行器不能访问中央数据库、创建 Attempt、决定重试或调度下游步骤。唯一允许的计算程序由部署方预先安装，命令中没有可执行文件、脚本、URL 或共享根路径。原配套服务仍复用 app.executor；另一个独立 MOCK 实现只能使用这里的线协议和共享文件，不包装该程序。

**传输与认证**：同机回环 HTTP；请求/响应为 UTF-8 JSON，禁止 NaN/Infinity。每次调用都有 `X-Execution-Protocol: 1`、`Content-Type: application/json`，以及以下两头。绑定对象恰为：

```json
{"executor_id":"executor-one","job_id":"00000000-0000-0000-0000-000000000003","task_id":"00000000-0000-0000-0000-000000000001","step_id":"00000000-0000-0000-0000-000000000002","attempt_id":"00000000-0000-0000-0000-000000000003","generation":1,"worker_id":"worker-one"}
```

- UUID 使用小写规范格式，job_id 必须等于 attempt_id，generation 是大于零的整数；两个身份是非空、不含控制字符的字符串。
- canonical(binding) 是按字段名排序、紧凑分隔符 `,`/`:`、ASCII 转义非 ASCII 字符的 JSON（Python 等价 `json.dumps(binding, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)`）。
- `X-Execution-Binding` = canonical UTF-8 字节的 URL-safe Base64（保留 `=` padding）。`Authorization` = `Bearer ` + HMAC-SHA256(secret UTF-8, canonical 字节) 的小写 hex。secret 至少 32 字符，中心与执行器部署时共享，不交给计算子进程。
- 认证恒时比较；每次校验路径 job、executor_id、持久化绑定完全相同。该 capability 只授权一个 Attempt 的查询/续租/停止/重复启动，不能换身份。回环与可信共享磁盘是部署边界，不是公网授权方案。

**派发前协商**：`GET /v1/capabilities` 使用同一绑定认证，不创建作业。响应：

```json
{"protocol_version":1,"executor_id":"executor-one","capabilities":["durable-start-id","bound-auth","renewable-lease","confirmed-exit","ordered-events","shared-artifacts-v1","committed-checkpoint-v1"],"processors":{"mock":["1"]},"step_keys":["normalize_asset","segment_subject","analyze_motion","generate_frames","encode_package","publish_result"],"input_kinds":["synthetic","uploaded"],"artifact_schema_versions":[1]}
```

中心在任何 PUT 前核对协议精确版本、执行器身份、上述全部 capability、processor + processor_version、step_key、input.kind 与产物 schema=1。未知附加 capability 可忽略，缺失/不兼容必须以 INVALID_CONFIG 拒绝派发，不能猜测兼容或静默本地回退。网络/5xx 是不确定性，不当成不兼容或启动成功。执行器 PUT 也检查版本和自己实际支持的计算类型，防止绕过协商。GET /health 仅为服务健康检查，不具备计算/协商语义。独立 MOCK 使用 `mock / independent-mock-v1` 支持 synthetic 固定六步，并以 `mock-step / independent-mock-v1` 支持第 13 节动态业务步骤；明确拒绝 uploaded/旧 smoke，不伪造 GIF。不同计算语义使用不同处理器版本，不能复用旧版本缓存。

| 方法/路径 | 请求体与行为 |
| --- | --- |
| PUT /v1/jobs/{job_id} | 恰为 `{ "claim": <不可变信封>, "lease_expires_at": "绝对UTC时间" }`。先持久化身份、命令及内容指纹，再启动；相同 ID/信封只返回原状态。重复 PUT 不是续租。不同信封 409。不得因响应丢失、进程崩溃或 STARTING 状态不明再次启动同 ID |
| GET /v1/jobs/{job_id}?after_seq=0 | 返回 seq 大于 cursor 的全部持久化事件，按 seq 递增；不改变计算或中央进度。未知 ID=404。所有剩余事件含最终结果必须与 EXITED 状态一起可读，不用未声明的分页丢掉尾部 |
| POST /v1/jobs/{job_id}/lease | 恰为 `{ "lease_expires_at": "绝对UTC时间" }`，仅中心 PG 续租成功后调用；只能延长，重复/更早但仍有效的授权不缩短。已过期、停止或退出=409，不能复活 |
| POST /v1/jobs/{job_id}/stop | 空对象 `{}`；持久化停止请求并观察实际退出。不存在的 ID 建持久化未启动墓碑，start_count=0，禁止迟到 PUT。已存在却身份不明不能假装未启动 |

时间采用带 `Z` 或 `+00:00` 的 UTC ISO8601。默认 30/300；硬时限和截止时间 null，显式 0 同样表示关闭，绝不能解释成立即到期（中心正常冻结为 null）。中心不在 PG 不可用时续授远端租约。执行器独立 watchdog 在过期/停止时终止身份确定的计算；失去租约不等于进程已退出。重启后先对账已持久化 PID+创建时间+主机，无法确定则 UNCERTAIN；不能把 PID 复用的其他进程杀掉。启动前持久化后、PID 记录前崩溃同样保留不确定，不重启、不强释。

**统一状态响应**（PUT/lease/stop 可以 events=[]，中心之后用 GET 拉事件）：

```json
{"binding":{},"state":"RUNNING","exit_confirmed":false,"returncode":null,"start_count":1,"observed_at":"2026-10-25T00:00:00+00:00","events":[{"seq":1,"event":{"type":"progress","progress":{"completed_units":0,"total_units":6,"percent":0}}}]}
```

binding 必须为上面完整原绑定，不是空对象；这里仅省略重复展示。state 仅 STARTING/RUNNING/UNCERTAIN/EXITED；仅 EXITED 对应 exit_confirmed=true。未确认退出 returncode 必须 null；确认退出但无法恢复成功输出时仍为 null，不能编造 0。正常完成要先持久化全部事件、确认计算子进程退出，再公布 returncode=0；中心还要求有效 result 和当前执行权才记成功。start_count 是持久化真实启动次数，只能 0/1；observed_at 是这次观察时间，不是进度时间。事件 seq 从 1 单调递增，同一事件重查内容不变。

错误 HTTP 语义：401 无效认证；403 绑定/执行器/路径不匹配；404 未知作业或不支持端点；409 命令冲突、墓碑、过期或已停止续租；422 请求格式错误或不支持的计算类型；426 不支持协议版本。错误体 `{ "detail": "可读原因" }`，不含密钥。中心将明确协商拒绝或 PUT 的 4xx 记为非重试配置/协议错误；RPC 超时/断链/5xx 不证明未启动，只重试原 ID，并独立查询终态。真正重跑由中央新建 Attempt。取消/清理仍必须获取绑定的退出事实，不因错误响应释放资源。

### 12.2 不可变信封、事件和共享产物 v1

claim 的字段如下。执行器只能读取该信封里的当前 Step；dependencies/checkpoint 是中心已提交引用，不按磁盘扫描推断提交。除了列出的字段外拒绝命令字段，尤其不能接受 command/script/artifact_root。

| 字段 | 格式/用途 |
| --- | --- |
| task_id, step_id, attempt_id, generation, worker_id, executor_id, external_job_id | 与认证绑定一致，external_job_id=job_id；execution_backend 固定 external |
| step_key, spec | spec 恰含 key、processor、processor_version、depends_on（步骤 key 列表）、parameters（下表）、cache_enabled bool、checkpoint_mode、resource_kind。step_key=spec.key；generate_frames 为 mock-gpu / committed-batch-manifest，其余 cpu / none |
| attempt_number, recovery_round | 正整数尝试序号、非负恢复轮；仅供计算识别注入失败，不能自行重试 |
| input, input_asset | input.kind 为 synthetic 或 uploaded。独立 MOCK 只接受 synthetic 的 JSON 输入；input_asset=null。配套媒体服务另支持已提交输入引用 |
| compatibility_key | 不透明非空兼容键，所有输出/检查点原样回传，不自行计算或改写 |
| dependencies | 已提交 output artifact 数组；顺序不作拓扑含义，以 metadata.step_key 区分；必须验证文件、指纹及引用，不再调度它们 |
| checkpoint | null 或当前 Step 已提交 checkpoint artifact；只复用其连续批次前缀，不能纳入命名空间中其他候选文件 |
| runtime_policy, retry_policy | 中心冻结策略；不自行改默认、不自行创建重试。runtime_policy 含 heartbeat_interval_seconds、lease_ttl_seconds、coordinator_scan_interval_seconds、attempt_hard_timeout_seconds、task_deadline_seconds、step_soft_warning_seconds、task_soft_warning_seconds |
| lease_expires_at, started_at, first_started_at, created_at, deadline_at | UTC 字符串或允许为空的时间；claim 不随续租改写，真正执行租约以 PUT 外层及 POST /lease 的授权为准 |
| resource_slot_id | mock-gpu 为正整数，其他 null；执行器不直接操作中心资源表 |

parameters 的键是允许列表，不要求每个键都出现：duration_seconds、batch_duration_seconds（有限非负数），batch_count、frames_per_batch（正整数），fail_first_attempts、seed（非负整数）。未提供时的计算默认值分别为 duration_seconds=0.05、batch_count=3、frames_per_batch=1、batch_duration_seconds=duration_seconds/batch_count、fail_first_attempts=0、seed=input.seed（也缺省则 0）；非批次步骤不要求帧参数。默认值只在计算时读取，不能补写并改变已持久化信封或幂等指纹。生产计算允许 MOCK；定向故障测试 fail_first_attempts 由中心测试开关约束。固定六步按中心提供的单步依赖推进，不开放用户 graph。

事件 envelope 为 `{ "type": ... }`，每个均包含在状态响应的 `{seq,event}` 中：
- progress：`{"type":"progress","progress":{"completed_units":1,"total_units":6,"resumed_batches":0,"percent":16.6667}}`，仅业务实际进展，不把心跳当进度。
- checkpoint / result：`{"type":"checkpoint","artifact":<artifact>}` 或 `{"type":"result","artifact":<artifact>}`；result 必须最后，成功退出只能一个 result；不能再追加业务事件。失败可以先提交检查点，再 error 后非零退出。
- error：`{"type":"error","error_code":"TRANSIENT_ERROR","message":"...","retryable":true}`；输入/配置/产物不兼容等不可重试错误为 false。中心决定实际重试。无 error 的非零退出由中心归类 EXECUTOR_EXIT。

artifact 恰至少含 id（新 UUID）、kind（checkpoint 或 output）、relative_path、content_fingerprint（文件原始字节 SHA256 小写 hex）、metadata（JSON 文件顶层对象本身，不再套一层）。JSON UTF-8；metadata 必须与文件解码内容相等。原子写新文件并 fsync 完成后才发事件，不覆盖已有文件；路径为 `task_id/step_key/attempt_id/<新UUID>-<标签>.json`，POSIX 相对路径，禁止绝对路径、反斜线、盘符、空组件、`.`/`..` 和逃逸 symlink。共享根只从执行器部署 CLI 获取，可信磁盘不对任意用户开放。

所有 MOCK 输出包含 `schema_version:1, source:"mock", execution_resource:"cpu", resource_kind, compatibility_key, step_key, processor_version, input, seed, dependencies:[上游文件指纹], completed_units, resumed_batches`。独立 MOCK 可以产生自己的确定性 JSON，不冒充真实图像或原处理器版本。

批次文件是实际可读 JSON；它的引用为 `{ "index":0, "relative_path":"...-batch-0.json", "content_fingerprint":"64位hex" }`。checkpoint.metadata 结构：

```json
{"schema_version":1,"compatibility_key":"原样回传","total_batches":6,"completed_batches":[{"index":0,"relative_path":"原Attempt路径","content_fingerprint":"文件原始字节的SHA256"}]}
```

completed_batches 必须为 0 起连续前缀、无重复路径；校验每个引用。恢复读取传入 checkpoint 的 metadata 与文件，检查 compatibility_key、total_batches、所有批次指纹，再从前缀长度继续写当前 Attempt 的新批次；前缀引用必须完全保留，不能重写旧已提交文件。generate_frames 的最终 output 在上述通用字段之外包含 total_batches/completed_batches，必须覆盖全部批次，resumed_batches 为实际复用数量。每批写完可以提交新 checkpoint + progress；无新批次的失败重试也不得伪造计算。

encode_package output 增加 `result_manifest:{source:"mock",format:"mock-animation-package-v1",frame_manifests:[generate_frames的{id,relative_path,content_fingerprint}]}`。publish_result 只读取 encode_package 已提交依赖，将它的 result_manifest 原样放入最终 output，附 `encoded_artifact:{id,relative_path,content_fingerprint}` 与 `publication:"local-only-mock"`，不是外部发布。本次独立 MOCK 的最终可读结果是 JSON，不是 GIF；原配套媒体能力继续保留。

## 13. 新授权：业务驱动的动态顺序托管

本节是固定六步之后新增的范围。业务端按需决定下一步，中心只托管已注册处理器的执行；六步动画和历史冻结任务原样保留，不是动态流程的限制。不建设动态并行、循环表达式、通用 DAG、代码插件或步骤人工审批。

### 13.1 业务接口与生命周期

- `POST /api/v1/tasks` 的 pipeline_id 新增 `managed-v1`，initial steps 为空。execution_backend 与策略仍在创建时冻结；第一版动态任务 input 仅 synthetic，步骤不启用跨任务缓存。创建请求中的动画 mock 预设不用于动态步骤，以下登记请求独立冻结每步配置。
- `POST /api/v1/tasks/{id}/steps` 要求 Idempotency-Key，body 为 `{key, after_step, processor:"mock-step", processor_version, input, parameters, resource_kind}`。key 是 `[A-Za-z0-9][A-Za-z0-9_-]{0,63}`；after_step 为当前末步骤 key，首步为 null。中心按此建立单一前驱，不接受 depends_on/graph/command/path。可在前驱运行时登记下一步，但只能待前驱成功后获执行授权。
- 同一 task/key 的步骤输入、参数、处理器版本与资源类型不可修改；重投同一登记幂等键和请求返回原 Step，不重复追加，改变请求则 409。不同幂等键不能重复占用已有 step key。过期业务客户端指向旧末步骤返回 TAIL_CONFLICT，不能意外追加到新链上。
- input 缺省表示使用创建时冻结的输入；明确提供则为该步骤的有限 synthetic JSON。parameters 键仅 duration_seconds、batch_count、frames_per_batch、batch_duration_seconds、fail_first_attempts，分别默认 .05/1/1/.1/0；计数 1..1000，时间有限非负，测试失败需 ENABLE_TEST_FAULTS。resource_kind 为 cpu（默认）或 mock-gpu。
- 首个已注册动态处理器为 mock-step。local/配套服务支持版本 `1`，文档独立实现支持 `independent-mock-v1`。它是预安装的确定性 JSON 批次计算，不是任意代码；中心派发 spec.key 使用业务 key，checkpoint_mode 固定 committed-batch-manifest。任何动态 key（即使叫 encode_package）都执行这个处理器，不暗中调用动画分支。最终 output 使用 12.2 通用字段和完整批次清单，无需动画 result_manifest/publication 字段。
- 中心先持久化 Step，记录 registration_order/input_snapshot/spec，再检查依赖、处理器/版本与资源；未授权前没有 Attempt、执行代次、租约或计算文件。已知未安装类型/非法配置在登记时拒绝，远端能力检查失败的已登记 Step 明确失败但不伪造 Attempt；暂时无法联系执行器保持等待，不能退回本地。业务适配层可代办这些 HTTP 调用，业务计算代码不需要权限散点调用。
- `GET /api/v1/tasks/{id}` 返回已持久化步骤、registration_order、input_snapshot、registration_closed，以及已有状态/结果/历史接口。业务端恢复后先读取这些真实记录，从现有末步骤继续，不重登记已成功工作。
- 当前登记步骤都成功但 registration_closed=false 时，Task 不得 SUCCEEDED；phase/wait_reason=WAITING_STEPS，等待业务登记或显式结束。已登记步骤数不是预知的总工作量，页面不能把它呈现为整个开放流程已完成。
- `POST /api/v1/tasks/{id}/finish` 要求 Idempotency-Key，body=`{last_step:"当前末步骤key"}`（空流程为 null）。持久化关闭登记，拒绝后续追加；允许在末步骤运行时提前关闭，但只有全部登记步骤成功、结果已正式提交且执行/清理无未确认部分时才能成功终结。原失败/取消语义保留，不把 finish 当作成功上报或取消。close 与 append 在 Task 行锁下串行；相同 finish 键可重放，改变目标末步骤冲突。

### 13.2 动态处理器的能力与授权边界（v1 的可选扩展）

capabilities 可增加 `named_step_processors:["mock-step"]`，同时 processors 声明其确切版本。只有列表里的预安装处理器可以接受任意合法业务 key；原 mock 仍只接受 step_keys 中的固定动画/内部 smoke key。未声明此扩展的执行器必须拒绝动态步骤，绝不能将 key 当命令或路径执行。

动态派发前的能力查询发生在 Attempt 授权之前，因此使用**只读协商 capability**：仍携带完整绑定（中心生成候选 Attempt UUID，用实际 Task/Step/下一代次），增加 `X-Execution-Scope: capabilities`，Authorization 的 HMAC 消息为 UTF-8 `capabilities:` 前缀拼接 canonical(binding) 原字节。此凭据只允许 GET /v1/capabilities，所有 job 端点必须 403 拒绝它；不能借查询凭据启动计算。

能力通过后中心在 Task/Step 事务中重新核对 READY、代次和资源，才持久化真正 Attempt、租约、命令；只有此时使用原 12.1 的 job capability 发 PUT。协商结果不等于执行授权；能力/状态竞态仍由不可变 spec、授权事务和执行器 PUT 再校验封口。原已授权作业查询/续租/停止使用 job capability 不变，清理必须核实实际退出。服务器可以继续接受已授权 job capability 查询能力，以兼容原固定流程客户端。

mock-step 的独立实现也只执行这一个 Step：验证共享依赖/检查点，复用指定的连续已提交批次，写自己 Attempt 的新批次，发布 checkpoint/progress/result 候选并实际退出；不导入中心模块、不决定下游、不自行重试。其 checkpoint/文件协议、退出确认、独立监督、30/300 默认及关闭硬时限语义全部沿用 12.1/12.2。

新增范围只有运行并验收后才能标为完成。必测：不同数量与非动画 key 的顺序步骤、登记重试、未授权无计算、业务恢复续登记、等待时不误终结、finish 与实际完成区分；同时保留固定历史和外部故障基线。

## 14. 新授权：预配置多节点与节点所属资源

本节替换单个执行器/单个模拟槽位的范围限制，不开放自动发现、动态并行、插件或真实 GPU。执行管理仍在原中心；预配置来源、执行控制、资源观察分工，不建设新的调度服务。下面是本轮实现契约，完成状态必须以新增运行记录为准。

### 14.1 节点清单与兼容

EXECUTION_NODES_JSON 为受控 JSON 数组，每项含 node_id、executor_id、url、secret_env、accepting、cpu_capacity、mock_gpu_slots（设备名字数组）、processors（处理器到版本数组）、storage_id、可选 ca_file。身份和设备名使用安全 ASCII 标识；executor_id 在清单中唯一。secret_env 引用独立环境变量，至少 32 字符，值不进数据库、冻结配置、计算子进程或日志。正常仍 30/300，硬时限和任务截止关闭。

默认无清单时原 local/单 external 配置继续工作。新配置清单用于新 external 任务：冻结 node_ids 和共享 storage_id，不冻结某一个执行器；每个 Step 再选择符合其处理器/版本/输入的节点。历史冻结路由不改写。每次 Attempt 固定 node_id、executor_id、job ID、资源及代次；路由与执行器持久化 state_id 随命令保存。配置目标或返回的状态库身份与原记录不符时拒绝使用，不能用新地址/新状态库上的未启动墓碑证明旧节点已退出。

007 增量保留原 id=1 槽位及旧 Attempt，统一标为 legacy 兼容执行域，明确它是旧单槽/预算语义而非新物理节点，不猜测旧未知进程所在的新节点。新节点的 CPU 容量分别记账，模拟设备由 (node_id,device_id) 唯一标识，单设备一个所有者。旧域与新节点资源不混用；配置减少或停止接单不清除 owner，不删除隔离状态。节点状态与接单开关分开，停止接单不等于停止在途计算。

### 14.2 受控传输、节点绑定与共享存储

回环可用 HTTP；远端配置只能 HTTPS，必须证书/主机名校验，可通过 ca_file 指定受信 CA，不支持关闭验证、环境代理或重定向。服务 CLI 增加 --host、--tls-cert、--tls-key；非回环监听必须提供 TLS。无用户提供的第二台设备/部署权限时不得自行猜地址或部署，只能同机模拟和本地 TLS 验证。

配置节点的服务 CLI 增加 --node-id、--storage-id（默认 pipeline-artifacts）、--cpu-capacity（默认 2）、--mock-gpu-slots（逗号分隔，默认 gpu-0）。无 --node-id 时保留旧协议。配置节点启用后，绑定对象在原七个字段之外恰增加 node_id 与 resource_id，两者都纳入原 canonical/HMAC。resource_id 为 cpu 或模拟设备名；只读协商可用 inspection，不能用于 job。每个 job 请求必须与服务自身 node_id/executor_id、claim.node_id/resource_device_id、已持久化绑定相符；旧无节点绑定凭据不能操作已配置节点。CPU 的资源标识是调度预算，不冒充真实 CPU 核心利用率。

claim 对节点模式仅增 node_id、resource_device_id、artifact_storage_id、executor_state_id；不能携带 URL、密钥或任意共享根。state_id 为服务状态库首次创建时持久化的 UUID，正常重启不变。PUT 核实与原状态库一致，再在持久化启动意图同一事务内检查预算/设备；未知执行继续占用，租约过期不放开设备。重复 command 只查原 job。状态库丢失后不能盲重发旧命令，查询/停止回执也不能证明旧计算退出；保留隔离，等待原状态库或可信人工对账。

能力扩展必需标记为 node-binding-v1、shared-storage-inspection-v1。GET /v1/capabilities 在原字段上增加 node_id、storage_id、state_id、cpu_capacity、cpu_in_use、mock_gpu_slots=[{device_id,state,owner_attempt_id,owner_generation}]；作业状态/退出响应也含 state_id。设备 state 为 AVAILABLE/RUNNING/QUARANTINED，来自持久化作业与退出观察，不是显存、温度或 GPU 利用率。后继可读取不同处理器版本生产的兼容 schema=1 上游输出，不能把生产者版本误当当前处理器版本；同一步检查点仍须符合中心 compatibility_key，不跨语义混用。

只读 scope=capabilities 另允许 POST /v1/capabilities，body 恰为 {storage_id,references}。返回上述能力，加 references_readable:boolean（所有引用均可实际读取/验证）和不可达时的 inspection_error=ARTIFACT_UNAVAILABLE。此操作只读文件、不创建 job、不授权执行；所有 job 路由仍拒绝只读 scope。检查 output/checkpoint 的字节指纹、JSON 元数据和嵌套批次；input 类引用验证原文件；不能据目录扫描把候选提升为提交。

中心共享 ARTIFACT_STORAGE_ID 默认 pipeline-artifacts。中心在真实共享根的 _system/storage-id.json 原子创建一次命名空间标记（storage_id、随机 id），作为 kind=storage-probe 的引用参与 POST 检查；它用于识别实际共享挂载，不是验收证明文件。只读检查允许这一固定管理路径，验证实际原字节指纹。还要检查这次 Step 的已提交依赖、输入与兼容检查点。一个节点上的绝对路径不发送给另一个节点，所有引用仍相对各自部署的共享根。缺挂载/错误挂载的节点不能获得计算授权；不实现跨机复制或对象存储。

### 14.3 选择、监督与观察

简单确定顺序选择：排除停止接单、身份/处理器/版本不匹配、共享存储不可达的节点；逐个检查在线/真实引用可读及可用资源，在节点行/槽位锁内原子创建 Attempt 并取得资源。首选节点繁忙或不可用时继续其他合适节点，不把队首等待变成全局阻塞。CPU 按该节点 capacity 与 RUNNING/未确认退出计数；GPU 仅领取该节点启用且空闲的设备。所有提交/清理校验原节点、资源、Attempt、代次和有效租约，旧消息不能跨节点或释放新 owner。

RPC I/O、资源/节点观察与恢复扫描不运行在唯一续租路径：Worker 使用独立后台监督扫描，计算和各作业网络轮询仍分离；观察失败不续死计算、不直接改业务进度。节点失联只改变观察状态；过期 Attempt 仍先 LOST/隔离，恢复后只能找原正确节点确认物理退出。进程/服务健康、HTTP 成功、容器状态都不是业务成功。

GET /api/v1/resources 保留旧域 slots/cpu 兼容读，另增 nodes 数组。GET /api/v1/nodes 返回同一节点读模型。节点条目含 node_id、executor_id、origin、accepting、configured、status（UNKNOWN/ONLINE/OFFLINE/STALE）、storage_id、last_seen_at、last_probe_at、last_error、capabilities、cpu:{capacity,in_use}、slots（id/node_id/device_id/state/owner_attempt_id/owner_generation/task_id/step_key）、events（近期节点观察变化）。凭据与 CA 私有路径不返回。API 只读，不探测/调度/强制释放。

Attempt 查询增加 node_id、resource_device_id；Task 的 Step 增加 execution_node_id/execution_resource_id（最近实际 Attempt，无 Attempt 则 null）。监控台复用同源页面，明确 MOCK 调度槽位、在线/失联与停止接单区别、当前所有者/等待/相关事件，不恢复创建入口，不把开放流程已登记数量当整体进度。

首版只验收可实际提供的环境：至少两个独立节点身份与多个设备、互斥/繁忙与不可达绕行、能力拒绝、真实共享引用消费、检查点恢复、失联/迟到/清理不越权和启动响应丢失。真实跨设备、小时级和真实 GPU 不由同机模拟推定完成。未来注册中心/运行平台适配仍必须履行同一执行授权、租约、退出确认及中央重试责任，不能另起一套权威状态。

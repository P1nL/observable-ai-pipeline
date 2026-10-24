# 执行规范 v0.1

状态：待实现协议。与 [PRD](PRD.md)、[长任务决策](ADR-001-long-running-execution.md) 共同作为实现基线。

## 1. 组件边界

- API：持久化提交、查询、恢复、重试、取消；不执行耗时模型。
- 协调器：依赖推进、退避到期、失联检测、任务汇总。MVP 单实例。
- Worker Supervisor：领取、续租、子进程监管、结果提交、资源清理。
- Executor 子进程：运行 MOCK 或未来的模型，生成进度、检查点候选与产物。
- PostgreSQL：任务、历史、执行所有权和 GPU 槽位的事实来源。
- 持久化文件卷：输入、中间结果、检查点与最终产物。

长计算不持有数据库事务。不因 HTTP 请求结束、浏览器关闭或 API 重启而取消任务。

## 2. Task / Step / Attempt

### Task

`QUEUED → RUNNING → SUCCEEDED | FAILED`；非终态可进入 `CANCELING → CANCELED`。仅显式人工恢复允许 `FAILED → RUNNING`。MVP 不恢复 CANCELED 任务，重新提交可复用有效缓存。

若显式配置的任务截止时间在排队期间到达，也允许 QUEUED → FAILED，原因记录为 TASK_DEADLINE，不创建虚假的执行 Attempt。

- RUNNING 包含资源等待、自动重试和失联恢复，通过 phase/wait_reason 区分。
- SUCCEEDED 要求所有必要步骤为 SUCCEEDED 或 CACHED，且最终结果引用已提交。
- 某必要步骤最终失败：停止新派发，允许其他在途步骤收尾，必要后继标记 BLOCKED，随后 FAILED。
- CANCELING 需要处理在途执行；未确认的进程退出不能被伪装成“已清理”。

### Step

状态：PENDING、READY、RUNNING、RETRY_WAIT、SUCCEEDED、CACHED、FAILED、BLOCKED、CANCELED。

- PENDING 在必要依赖全部满足后进入 READY。
- READY 命中有效缓存可直接进入 CACHED，否则领取进入 RUNNING。
- RUNNING 成功进入 SUCCEEDED；可重试失败进入 RETRY_WAIT；不可恢复或预算耗尽进入 FAILED。
- RETRY_WAIT 到期进入 READY。
- FAILED 与 BLOCKED 仅通过显式恢复操作重新评估；成功步骤不被清空。
- 等待 GPU 使用 READY + WAITING_GPU；资源配置不可能满足使用 READY + UNSCHEDULABLE 告警，不占 Attempt 或伪造执行时长。

### Attempt

状态：RUNNING、SUCCEEDED、FAILED、TIMED_OUT、LOST、CANCELED。

一次领取创建一个 Attempt。重试与接管创建新的记录，不覆盖历史。LOST 是执行所有权失效，不是物理进程停止证明。另记录 stop_requested_at、process_exit_confirmed_at、resource_cleanup_state。

## 3. 持久化模型

| 表 | 关键字段与约束 |
| --- | --- |
| tasks | id、idempotency_key、request_fingerprint、pipeline/config_snapshot、status、phase、created_at、deadline_at(nullable)、recovery_round、next_event_seq |
| steps | task_id+step_key 唯一、depends_on、processor_version、status、current_attempt_id、execution_generation、next_retry_at、checkpoint_artifact_id、output_refs、cache_key、reused_from_step_id |
| step_attempts | id、step_id+attempt_number 唯一、recovery_round、worker_id、fencing_token、lease_expires_at、heartbeat_at、started_at、finished_at、progress、error_code、retryable、stop/cleanup fields |
| artifacts | id、kind、immutable_uri、content_fingerprint、producer_attempt_id、batch_index/context、committed_at、availability |
| task_events | task_id+seq 唯一、type、step_id/attempt_id、payload、created_at |
| workers | id、node_id、capabilities、heartbeat_at、status |
| resource_slots | node_id+device_id 唯一、kind、capacity、state、owner_attempt_id、owner_generation |

小型 DAG 的依赖先存 JSON，不额外建设图数据库。大文件不进入数据库。进度保留最新快照，关键阶段写事件，避免逐帧写历史。

任务配置创建时冻结，包括处理器、模型版本、重试策略、时限策略和输入引用。不同配置需创建新任务，不能通过修改环境变量悄悄改变在途任务。

## 4. 原子领取、租约与 fencing

领取在短事务中完成：

1. 选择依赖已满足、所属 Task 可运行的 READY 步骤并加行锁；多 Worker 使用 FOR UPDATE SKIP LOCKED。
2. 若需要 GPU，在同一事务内锁定本节点匹配的空闲槽位。资源不足不领取。
3. 增加 execution_generation，创建 RUNNING Attempt，设置 current_attempt_id 与租约，占用槽位，写事件。
4. 提交事务后启动执行子进程。启动失败也需要结束 Attempt 并对账资源。

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

generate_frames 每批保存产物，检查点记录已提交批次、输入/处理器版本和恢复所需上下文。恢复前验证兼容性，只跳过已提交且可用的批次；中途批次重新计算。

百分比不是检查点。不支持恢复协议的模型只保证步骤级重跑。MVP 不自动收集任意模型/GPU kernel 的内部状态。

已提交文件意外丢失时不得报告可用：标记产物不可用并阻止依赖执行，给出 ARTIFACT_UNAVAILABLE。MVP 不自动篡改历史成功状态；用户通过新任务重算受影响分支并复用其余有效缓存。存储损毁不属于普通进程恢复。

## 8. 缓存与幂等

提交幂等与结果缓存是两种不同机制：

- 同一 Idempotency-Key + 相同请求指纹返回原 Task；相同键不同请求返回冲突。
- 缓存键包含步骤/处理器版本、模型版本、规范化参数、上游实际产物指纹、seed/生成标识和作用域。
- 仅引用成功提交、可访问且兼容的结果；缓存命中记 CACHED、来源 Step 和产物引用，不伪造计算 Attempt。
- 用户要求重新采样时改变生成标识或禁用缓存。
- 不声称重新计算一定产生逐字节一致的结果。首版不实现同时计算同一缓存键的跨任务合并。
- publish_result 具有任务级副作用，不从其他任务直接缓存该步骤，只幂等写入当前 Task 的结果清单。

MVP 只有单用户/单作用域。未来多租户必须明确隔离，不能把缓存扩展成跨用户数据访问渠道。

## 9. GPU 调度

- CPU/GPU 分逻辑队列。GPU 使用 node_id/device_id 整卡独占，不用显存估算实现卡内多任务。
- GPU Worker 只消费本节点可满足的请求；申请与领取在同一事务。
- 槽位状态：AVAILABLE、RESERVED、RUNNING、QUARANTINED、OFFLINE。
- 正常退出后释放；超时/取消先停计算，再释放。
- Worker/节点失联时，不把租约过期等同于硬件空闲。槽位置为 QUARANTINED，由节点恢复时对账进程和执行代次。
- 新 Attempt 可以使用另一张已确认空闲的卡，但不得复用未清理的旧槽位。物理重复计算风险需要显式记录。
- MockGPUProvider 只模拟资源。真实 GPU provider 的进程控制、显存与健康指标需要另行验收。

观测项：节点/卡标识、容量、占用 Attempt、队列等待时长、执行时长、利用率、显存、温度、OOM/失联次数。每个指标记录 source=mock 或 real；GPU 利用率不代表业务完成百分比。

## 10. API 与事件协议

统一前缀 /api/v1；以下为待实现接口。

| 方法与路径 | 用途 |
| --- | --- |
| POST /tasks | 创建任务；Idempotency-Key；提交事务成功后返回 202 |
| GET /tasks | 列表、状态过滤和分页 |
| GET /tasks/{id} | 任务快照、步骤、有效配置、等待原因、结果引用 |
| GET /tasks/{id}/events?after_seq=... | 按任务内序号增量读取历史 |
| GET /tasks/{id}/steps/{step_id}/attempts | 每次执行、错误、租约、资源与恢复来源 |
| POST /tasks/{id}/resume | 人工恢复失败任务 |
| POST /tasks/{id}/steps/{step_id}/retry | 人工重试指定失败步骤 |
| POST /tasks/{id}/cancel | 幂等请求取消 |
| GET /tasks/{id}/artifacts | 产物元数据与安全下载引用 |
| GET /resources | Worker、GPU 占用和健康情况 |

状态变更与事件同事务提交。锁定 Task 行分配任务内 seq，防止游标读取越过未提交事件。前端约 2 秒轮询，不依赖 WebSocket 在线状态；重新连接从数据库恢复。

日志包含 task_id、step_id、attempt_id、worker_id 和错误码，不记录密钥。业务进度报告已完成单位/总单位，未知时长显示阶段与耗时，不按时间伪造百分比。

公开仓库不等于开放写 API。若后续公网部署，创建任务、恢复和故障注入必须限制访问与并发；不得允许上传任意代码执行。

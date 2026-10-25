# Observable AI Pipeline

可恢复、可观察的 AI 长任务管线 MVP，以动画素材处理为例。计算使用 CPU MOCK，任务状态、执行进程、数据库、检查点和结果文件均真实。

**已实现**：步骤重试、中断恢复、结果复用、依赖调度、执行历史、监控页面及 MOCK GPU 资源管理。未接入真实模型/GPU，未做小时级或真实跨机验收。

## 快速开始

需要 **PowerShell 7、uv、Docker Desktop**，以下命令在完整源码的仓库根目录执行。技术栈为 Python 3.12、FastAPI、PostgreSQL 和原生 HTML/CSS/JS，无需前端构建。

**1. 初始化数据库和依赖**

```powershell
docker compose up -d --wait
uv sync --locked
$env:PYTHONPATH = Join-Path $PWD 'backend'
uv run --locked python -m app.db
```

**2. 分别在两个终端启动 API 和 Worker**

```powershell
# 终端 A
$env:PYTHONPATH = Join-Path $PWD 'backend'
uv run --locked uvicorn app.api:app --host 127.0.0.1 --port 8000
```

```powershell
# 终端 B
$env:PYTHONPATH = Join-Path $PWD 'backend'
uv run --locked python -m app.worker --worker-id local-worker
```

**3. 另开终端提交示例**

```powershell
$headers = @{ 'Idempotency-Key' = [guid]::NewGuid().ToString() }
$body = Get-Content -Raw -Encoding utf8 examples/animation-pipeline.json
$task = Invoke-RestMethod 'http://127.0.0.1:8000/api/v1/tasks' -Method Post `
  -Headers $headers -ContentType 'application/json' -Body $body
Write-Output "http://127.0.0.1:8000/tasks/$($task.id)"
```

浏览器打开输出地址查看步骤、历史和结果；接口文档为 `http://127.0.0.1:8000/docs`。页面不提供创建入口。示例输出 JSON；上传 PNG/JPEG 后可生成真实 GIF，见 [运行手册](docs/RUNBOOK.md)。提交响应丢失时保留原 `$headers` 和 `$body` 重试。

停止应用按 Ctrl+C，数据库用 `docker compose stop`。不要删除数据库卷或 `.runtime`。应用不自动读取 `.env`，自定义配置见 [.env.example](.env.example)。

## 设计说明

API 接收任务后立即返回 ID；独立 Worker/Supervisor 领取、续租和恢复，子进程负责计算，页面只查询和管理。

```text
素材归一化 ─┬→ 主体分割 ─┐
            └→ 动作分析 ─┴→ 帧生成 → 编码打包 → 结果清单
```

按稳定输入输出和独立失败边界拆步骤：分割/分析可并行，帧生成按批次恢复，编码失败复用成功帧。一个 Worker 可顺序完成；增加第二个 Worker 才能观察分支并行。

| 重点 | 实现 |
| --- | --- |
| 状态与历史 | 分开记录 Task、Step、Attempt；重试追加 Attempt，不覆盖失败历史 |
| 持久化 | PostgreSQL 保存配置、依赖、状态、租约、错误、事件和资源 owner；文件保存输入、检查点及结果，确保重启后可恢复 |
| 长任务 | 独立心跳 30 秒、可续租租约 300 秒；默认无硬时限，子进程退出后不再无限续租 |
| 失败恢复 | 有限重试与退避；耗尽后可人工恢复；中断后从已提交批次继续，成功步骤不重算 |
| 结果提交与复用 | 当前 Attempt、执行代次和租约共同校验，拒绝旧进程写回；兼容缓存保留来源，不伪造计算记录 |
| GPU 资源 | MOCK 设备互斥、等待和隔离；失联不立即释放，确认物理退出后才复用，不虚构硬件利用率 |

扩展到小时级还需验证长期稳定性、存储/日志容量和真实模型检查点。当前支持批次恢复，不承诺任意模型内部续跑或恰好一次计算。

## 开发与验证

- **实际开发过程**：2026-10-24 完成三步执行闭环、失败恢复、检查点和模拟 GPU 验收，随后增加图片/GIF、六步 DAG、缓存与监控台；10-25 完善外部执行器、动态顺序托管、多节点资源管理及界面，进行回归修复和交付文档整理。
- **实际问题**：早期恢复扫描缺失导致中断任务停滞，后纳入 Worker；多节点锁范围过大引发死锁，后收窄锁并移除重复配置写入。详见 [开发记录](docs/DEVELOPMENT_LOG.md)。
- **验证**：已有真实 PostgreSQL/HTTP/子进程恢复验收，本轮定向回归 8 项通过；测试命令和完整边界见 [运行手册](docs/RUNBOOK.md#测试与恢复边界) 与 [验收记录](docs/ACCEPTANCE.md)。
- **AI 工具**：Codex。Token 总量、消耗最多的阶段及个人投入最多的工作/时长尚待作者补充，未用自动执行时间代填。
- **交付状态**：核心 MVP 和主要加分项已实现；完整源码已本地提交，尚待推送至公开仓库，未部署公网服务。

详细状态机、持久化和执行协议见 [执行规范](docs/SPEC.md)；外部执行器、动态顺序托管和节点配置均为增强项，不影响最小启动。

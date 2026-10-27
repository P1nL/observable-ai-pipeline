# Observable AI Pipeline

可恢复、可观察的 AI 长任务管线 MVP，以动画素材处理为例。计算使用 CPU MOCK，任务状态、执行进程、数据库、检查点和结果文件均真实。

**已实现**：步骤重试、中断恢复、结果复用、依赖调度、执行历史、监控页面及 MOCK GPU 资源管理。

## 仓库结构

```text
backend/
├─ app/          后台服务、任务执行与恢复逻辑
│  └─ web/       监控页面
├─ migrations/   数据库变更脚本
└─ tests/        自动化测试
executors/       独立执行器示例
examples/        任务提交、业务接入与节点配置示例
docs/            运行手册、设计说明和验收记录
compose.yaml     本地数据库启动配置
pyproject.toml   Python 依赖与测试配置
uv.lock          固定依赖版本
```

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

## 项目简要说明

**1. 实现方式与开发安排**

用 Python 编写后台服务，数据库保存任务记录，网页展示进度和结果。提交任务后立即返回编号，耗时计算交给独立程序，关闭网页也不会中断任务。图片处理和文件生成是真实的，AI 计算用模拟过程代替。

开发顺序是先跑通任务，再解决中断恢复，最后完善页面。2026 年 10 月 24 日完成基础流程和恢复验证，随后加入图片/GIF、六步处理和结果复用；10 月 25 日完善独立执行服务、多节点资源管理、界面及文档。先验证核心能力，再增加功能，而不是同时铺开。

**2. 长时间执行、状态和失败恢复**

重点是让任务运行多久都能查到状态，出问题后尽量少重做。系统分别记录整个任务、每个步骤和每次执行的情况，重试不会抹掉失败历史。计算程序由独立程序看护，定期确认是否仍在运行，不因耗时较长就直接判失败。

临时错误会有限次数重试，仍失败则等待人工恢复。程序中断后，从已确认保存的进度继续，成功步骤不重做；旧程序迟到的结果不能覆盖新结果。模拟 GPU 被占用时其他任务排队，失联后也要确认原程序已经退出，才能再次分配。

**3. 步骤与先后关系**

动画任务分为整理素材、分离主体、分析动作、生成画面、编码打包和保存结果清单。分离主体与分析动作可同时进行，生成画面必须等待两者完成。这样拆分是为了让每步有明确的输入和结果：例如编码失败只重做编码，不必重新生成画面；画面按批保存，中断后不必全部重算。

**4. 哪些信息需要长期保存**

数据库保存任务参数、步骤顺序、等待/运行/成功/失败等状态、每次执行的时间和错误，以及执行程序和设备占用情况。输入文件、已完成批次和最终结果单独保存。这样即使服务重启，也知道做到哪里、哪些结果还能用、哪些资源尚未释放，并能查清失败经过。

**5. AI coding 工具与 token**

使用 Codex 辅助编写代码、测试和整理文档。Token 总量及消耗最多的部分尚无完整统计，待补充实际记录或注明依据的估算。

**6. 个人投入最多的部分**

待作者补充本人投入最多的工作、具体做了什么及大致耗时，不将 AI 自动编写和运行测试的时间计为个人工时。

**7. 失败案例与能力限制**

早期只考虑了正常执行，没有在基本启动方式中加入中断检查，导致程序退出后任务一直停在运行中；后来加入定期检查和接续处理。另一次在 Linux 上把“计算已结束、但系统尚未清理完记录”误判为仍在运行，导致资源无法释放，修正判断后相关测试通过。

目前只能从程序主动保存的批次恢复，不能从任意模型的任意位置继续，因为模型未必提供保存和恢复内部进度的能力。小时级运行、真实 GPU 和跨机器部署还未验证，需要实际环境测试，不能用模拟结果代替。

更多实现细节见 [执行规范](docs/SPEC.md)，测试结果与问题记录见 [验收记录](docs/ACCEPTANCE.md) 和 [开发记录](docs/DEVELOPMENT_LOG.md)。

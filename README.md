# 地震灾害科学协同服务

这是一个面向地震台网与应急指挥中心的模块化后端，集中管理地震事件、台站观测、震情计算、灾情协同、用户权限、会话、审计和可恢复后台任务。项目使用 FastAPI 与 SQLite，所有运行数据保存在单个本地数据库文件中，不依赖另行部署的数据库、缓存或消息队列。

## 主要模块

- 震情档案：登记地震事件、震源参数和台站观测，保留计算输入摘要。
- 科学计算：提供震级、距离和烈度的确定性计算，以及可恢复后台任务。
- 灾情协同：管理灾情报告、公告、部门责任和跨部门办理状态。
- 身份与权限：用户、角色、细粒度权限、会话令牌、账号停用和会话撤销。
- 审计记录：关键身份操作留痕，并对口令和令牌等敏感字段做过滤。
- 后台任务：使用 SQLite 保存待执行任务，支持去重、租约、重试和完成回执。

## 运行环境

- Python 3.11
- SQLite 3，由 Python 标准库提供
- Linux、macOS 或 Windows

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

配置项均以 `TOWNSHIP_` 开头。可以复制 `.env.example` 后按需设置，默认数据库位于 `./data/township.db`。

## 初始化与检查

```bash
python -m app.cli init-db
python -m app.cli check-db
```

## 启动服务

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8432
```

健康检查：

```bash
curl -sS http://127.0.0.1:8432/api/system/health
```

首次部署可创建唯一的初始管理员：

```bash
curl -sS -X POST http://127.0.0.1:8432/api/auth/bootstrap   -H 'Content-Type: application/json'   -d '{"username":"admin","password":"Admin!23456","client_label":"initial-setup"}'
```

之后通过 `/api/auth/login` 获取会话令牌，并在管理接口请求头中使用 `Authorization: Bearer <token>`。

## 测试

```bash
python -m pytest
```

测试覆盖身份初始化、登录、用户与角色维护、权限计算、账号停用后的会话撤销、审计脱敏、事件与台站观测、烈度计算、后台任务去重与领取，以及数据库时间格式。

## 编译检查

```bash
python -m compileall -q app tests
```

## API 冒烟

```bash
python -m app.cli smoke
```

该命令在进程内启动应用并检查服务根路径与健康接口，适合部署前快速确认路由和数据库初始化是否正常。

## 烈度网格计算任务流程

烈度估计以离线任务方式运行，状态全部持久化在 SQLite（`seismic_computations` 表），不依赖消息队列。状态机：

```text
queued ──claim──▶ leased ──complete──▶ done（完成回执，不可变）
  ▲                 │
  │             fail│（未超 max_attempts，指数退避）
  └──── retry ◀─────┤
                    └──超过 max_attempts──▶ failed ──人工 retry──▶ queued
leased 且 lease_until 已到期 ──recover/下次 claim 自动回收──▶ retry
```

关键约定：

- **提交去重**：任务键 = 事件输入摘要（震级、震源深度、全部台站观测）+ 模型版本 + 网格步长/半径的规范化哈希，数值参数会归一化（`20` 与 `20.0` 视为同一任务）。重复提交返回同一行并带 `deduped: true`，不会产生两份数据。
- **完成回执**：结果内嵌 `model_version`、`input_digest`、`input_summary`（震级/深度/台站清单等）、`grid.order`（固定 `lat_asc_lon_asc`：纬度外循环升序、经度内循环升序）、`grid.shape` 与全部格点，并对这些内容计算 SHA-256 `checksum`。
- **版本防护**：完成时校验结果的模型版本与输入摘要必须与任务行一致（SQL 同时带 `model_version` 条件），旧工作者无法用旧模型结果覆盖较新版本；已完成任务重复回执返回冲突，不覆盖既有结果。
- **失败重试**：工作者用 `/fail` 上报错误，按退避时间回到 `retry`；达到 `max_attempts` 置 `failed`，可由人工 `/retry` 清零重新入队。只有租约持有者能完成或上报失败。
- **重启恢复**：工作者崩溃或服务重启后，`leased` 任务在租约到期后由 `recover` 接口或下一次 `claim` 自动回收，attempts 累加后可重新领取。

命令行（完全离线，不启动 HTTP 服务）：

```bash
python -m app.cli seismic list [--status queued]
python -m app.cli seismic enqueue <event_id> --model-version gmpe-2026.1 --grid-step-km 20 --radius-km 100
python -m app.cli seismic claim worker-1            # 领取
python -m app.cli seismic run worker-1              # 领取+执行+校验
python -m app.cli seismic fail <task_id> worker-1 "网格生成失败" --retry-seconds 30
python -m app.cli seismic retry <task_id>           # failed 任务重新排队
python -m app.cli seismic status <task_id>          # 查看状态与回执
python -m app.cli seismic verify <task_id>          # 校验 checksum/版本/摘要/坐标顺序
python -m app.cli seismic recover [--grace-seconds 0]  # 回收过期租约
python -m app.cli seismic demo                      # 端到端演示（去重/校验/重启恢复）
```

HTTP 接口（前缀 `/api/seismic`）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/events/{id}/computations` | 提交任务（幂等，返回 `deduped`） |
| GET | `/computations?status=` | 任务列表 |
| GET | `/computations/{id}` | 任务状态与回执 |
| POST | `/computations/claim?worker_id=` | 领取（自动回收过期租约） |
| POST | `/computations/{id}/calculate?worker_id=` | 执行并完成 |
| POST | `/computations/{id}/fail` | 上报失败（body：`worker_id`、`error_message`、可选 `retry_seconds`） |
| POST | `/computations/{id}/retry` | 人工重试失败任务 |
| POST | `/computations/recover` | 批量回收过期租约（重启后调用） |
| GET | `/computations/{id}/verify` | 结果校验报告 |

## 目录结构

```text
app/
  api/             用户、角色、审计、认证和系统接口
  core/            时钟、安全、异常和分页能力
  repositories/    SQLite 查询与持久化读取
  routers/         灾情、事件、公告、部门和信访业务接口
  seismic/         地震事件、台站观测和科学计算服务
  schemas/         管理接口输入模型
  services/        身份、审计和后台任务领域服务
  cli.py           初始化、检查和冒烟入口
  database.py      SQLite 连接、事务、表结构与基础权限
tests/             核心、管理接口和原有业务回归测试
tools/             本地维护脚本
```

## 数据一致性

SQLite 连接默认启用外键、WAL、busy timeout 与同步写入策略。需要跨多张表更新的管理操作在即时事务中执行，失败会整体回滚。会话令牌只保存摘要；用户停用会撤销仍有效的会话。审计事件保存操作者、动作、资源、结果和前后状态，但不会保存明文密码或令牌。

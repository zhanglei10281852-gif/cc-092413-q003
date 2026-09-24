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

## 烈度网格计算任务（离线队列）

烈度估计以**任务**形式离线执行，状态持久化在 `seismic_computations` 表中，不依赖外部消息队列。状态机为：

```text
queued ──claim(租约)──▶ leased ──calculate──▶ done      （完成回执，终态）
                          │
                          ├──fail（未超次数）──▶ retry ──available_at 到期──▶ queued
                          ├──fail（达到 max_attempts）──▶ failed（终态，可人工 retry 复活）
                          └──租约到期（工作者崩溃/服务重启）──▶ recover ──▶ retry
```

### 去重与模型版本

- 任务键 `task_key = sha256(规范化JSON(event_id, input_digest, model_version, grid_step_km, radius_km))`，带 UNIQUE 约束。
- `input_digest` 是事件参数与全部台站观测（按 id 排序）的 sha256；输入或参数任一变化都会得到不同任务。
- **重复提交返回同一行**（响应头 `X-Idempotent-Replay: 1`），不会产生两份数据；任务完成后再次提交也只返回既有结果。
- `model_version` 是任务键的一部分：升级模型产生独立的新任务，**物理上不可能覆盖旧版本结果**；完成时还会校验结果中的模型版本与输入摘要，不一致拒绝写入。

### 结果信封与校验

完成结果（`result_json`）固定包含：

- `model_version`：采用的模型版本；
- `input_digest` / `input_summary`：输入摘要与震级、震源深度、经纬度、观测数量等可读摘要；
- `grid.axis_order = "lat,lon"`、`grid.scan_order = "lat-asc/lon-asc"`、`grid.origin = "southwest"`、`grid.shape = [纬度数, 经度数]`：显式声明网格坐标顺序，`points` 为行主序（纬度外层升序、经度内层升序），供人口暴露分析对齐；
- `result_checksum`：结果信封规范化 JSON 的 sha256，在完成时写入，可随时复算比对，发现存储损坏或篡改。

### 命令行

```bash
# 演示数据：一次地震 + 3 个台站观测 + 提交任务
python -m app.cli seismic-seed
# 提交（重复执行返回同一任务）
python -m app.cli seismic-submit --event-id 1 --model-version gmpe-2026.1 --grid-step-km 20 --radius-km 60
python -m app.cli seismic-list [--event-id 1] [--status queued]
python -m app.cli seismic-status 1          # 状态回执（尝试次数、租约、错误、校验和）
python -m app.cli seismic-recover           # 重启后回收过期租约
python -m app.cli seismic-worker --worker-id w1   # 离线工作者：先恢复，再领取-计算-回执直到队列空
python -m app.cli seismic-verify 1 [--include-grid]  # 复算 sha256 校验结果，退出码 0/1
python -m app.cli seismic-retry 1           # 人工重新排队终态失败任务（done 拒绝重试）
# 手动单步（可用于模拟崩溃：claim 后不 complete，租约到期再 recover）
python -m app.cli seismic-claim --worker-id w1 --lease-seconds 60
python -m app.cli seismic-calculate 1 --worker-id w1
python -m app.cli seismic-fail 1 --worker-id w1 --error "..." [--retry-seconds 10]
```

### HTTP 接口（前缀 `/api/seismic`）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/events/{id}/computations` | 提交任务，202；重复提交返回同一行并带 `X-Idempotent-Replay: 1` |
| GET | `/computations?event_id=&status=&limit=` | 任务列表（不含大结果体） |
| POST | `/computations/claim?worker_id=&lease_seconds=` | 领取任务（顺带回收过期租约） |
| POST | `/computations/{id}/calculate?worker_id=` | 执行计算并写完成回执（仅租约持有者） |
| POST | `/computations/{id}/fail` | 上报失败，指数退避重试（5、10、20…，上限 300s），5 次后终态失败 |
| POST | `/computations/{id}/retry` | 人工重排失败任务；`done` 返回 409 |
| POST | `/computations/recover` | 重启恢复：回收全部过期租约 |
| GET | `/computations/{id}` | 任务完整行（含结果 JSON） |
| GET | `/computations/{id}/receipt` | 完成/状态回执（计数、校验和、时间戳、错误） |
| GET | `/computations/{id}/verify?include_grid=` | 复算 sha256、核对模型版本/输入摘要/坐标顺序 |

崩溃恢复语义：租约带 `lease_until`；工作者进程死亡不会上报完成，重启后调用 `recover`（或下一次 `claim`）只回收**已到期**租约并退回 `retry`，`attempts` 保留；原工作者迟到的完成回执因不再持有租约被拒绝（409），因此重试不会覆盖、也不会产生重复结果。


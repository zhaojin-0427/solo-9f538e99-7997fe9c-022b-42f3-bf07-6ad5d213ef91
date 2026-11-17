# 招标公告语料采集器（Tender Corpus Collector）

面向招标语料数据员的采集 API：提交多个官网公告 URL，后台按共享配额抓取，
提取 HTML 标题与正文，全部成功后提供完整 JSON 语料下载。

技术栈：FastAPI + httpx（异步）+ SQLite（持久化），Docker Compose 一键启动
API 与两个可模拟**双向跳转、限流（429/503 + Retry-After）、慢响应**的本地站点。

## 快速开始

```bash
docker compose up --build
```

然后打开 <http://localhost:8000/docs>（Swagger UI），即可**采集、查询、下载**：

1. `POST /jobs` 提交 1–20 个公告 URL（示例见下），返回 `job_id` 与每条的
   `announcement_id`；
2. `GET /jobs/{job_id}` 轮询进度（每条公告的状态/跳数/重试次数/错误）；
3. `GET /jobs/{job_id}/download` 在**全部成功**后下载完整 JSON 语料
   （有失败或未完成时返回 409，不提供半份结果）；
4. `GET /cooldowns` 查看当前生效的源冷却；`GET /jobs` 列出所有作业。

### 接续（原件 / 更正件）——语料员取"当前可用文本"

采集成功后，用公告 ID 把更正件挂到原件后面，再查询有效语料：

1. `POST /api/continuations`，`{"previous_id": 原件ID, "announcement_id": 更正件ID}`。
   **原件必须已采成功；更正件可以仍在采集**。每件至多一个前件、至多一个活动后件，
   成环会被拒绝；
2. `GET /api/announcements/{id}` / `GET /api/announcements/{id}/chain`
   查看单条公告及其接续链；
3. `GET /api/corpus/effective` 查询**每个接续链当前有效的一份文本**：
   沿链取**最后一件已采成功**的公告；若最新更正件仍在采集或采集失败，
   继续提供上一件，并在 `pending_reason` / `failed_reason` 中给出原因；
4. `GET /api/corpus/effective/download` 下载有效语料 JSON
   （`?require_all_available=true` 时若存在没有任何成功文本的链则 409）。

规则细节：

| 场景 | 行为 |
|---|---|
| 原件未采成功就登记更正 | 409 `original_not_collected` |
| 重复登记同一条边 | 201 `already_registered`（幂等） |
| 给同一原件再挂第二个更正件 | 已有待采更正 → 409 `previous_has_pending_successor`；已有成功更正 → 409 `previous_has_collected_successor` |
| 把已有前件的更正件改挂别处 | 409 `announcement_already_has_predecessor` |
| 把已有后件的公告改成别人的更正件 | 409 `announcement_has_successor`（避免拼接两条链） |
| 成环 / 自环 | 409 `cycle` |
| **失败**的更正件改挂新更正件 | 允许：旧失败件被摘下（`detached_at`），保留在 `replaced_corrections` 供审计，新件成为活动后件 |
| 更正件待采/失败 | 查询仍返回上一份成功文本，附 `state=pending` / `state=failed` 与原因 |
| 登记与采集完成交错 | 整条链在**一次加锁读**中取快照；只会看到"切换前"或"切换后"，不会出现两个有效版本或短暂漏供 |
| 重启 | 接续边与公告状态都在 SQLite；未完成任务照常恢复，结论一致 |
| 原作业下载 | `GET /jobs/{id}/download` 行为完全不变 |

两个模拟站点：宿主机上 `http://localhost:8001`（site_a）、`http://localhost:8002`
（site_b）可浏览；**提交给 API 的 URL 请使用容器网络名**
`http://site_a:8000/...`、`http://site_b:8000/...`（API 容器内解析）。

### 可试的 URL（在 /docs 的 POST /jobs 里提交）

| URL | 行为 |
|---|---|
| `http://site_a:8000/article/1` | 普通公告（提取标题+正文） |
| `http://site_a:8000/to-peer/2` | 302 跳到 site_b 的公告（跨源 1 跳） |
| `http://site_a:8000/chain/3` | 双向跳转链，恰好 3 跳成功 |
| `http://site_a:8000/chain/4` | 第 4 跳超出上限 → 失败 |
| `http://site_a:8000/flaky/1?fails=2&ra=3` | 前 2 次 429 + `Retry-After: 3`，第 3 次成功 |
| `http://site_b:8000/busy` | 始终 503 + `Retry-After: 1` → 重试 2 次后失败 |
| `http://site_a:8000/slow/1?delay=3` | 慢响应（3 秒） |
| `http://site_a:8000/redirect-loop` | 双向死循环跳转 → 跳数上限失败 |

## 需求实现对照

| 要求 | 实现 |
|---|---|
| 提交至多 20 个 URL | `POST /jobs`，pydantic 校验 1–20 条、http/https、去重 |
| 提取标题和正文 | `api/app/extract.py`：stdlib `html.parser` 提取 `<title>` 与 `<body>` 文本（剔除 script/style） |
| 全局最多 2 个在途请求 | 全局 `asyncio.Semaphore(2)`，所有作业共享一个引擎 |
| 同源最多 1 个 | 每个源（scheme://host:port）一条串行"pump"协程 |
| 跳转按目标源排队 | 不自动跟随重定向；302 目标作为新任务投入**目标源**的队列 |
| 每条公告最多 3 跳 | `hops >= 3` 再遇 3xx 即失败（`too many redirects`） |
| 429/503 + 整数 Retry-After → 同源所有作业未发请求延后 | 设置源级冷却（内存 + SQLite），该源 pump 发送前等待 |
| 等待不占全局名额 | 冷却 `sleep` 发生在获取全局信号量**之前** |
| 其他源仍能采集 | 冷却是 per-origin 的，其他源的 pump 不受影响 |
| 每个 URL 最多重试 2 次 | `attempts` 计数（429/503/网络错误），跳转后的新 URL 重新计数 |
| 可查进度 | `GET /jobs/{id}`（逐条状态）、`GET /jobs`、`GET /cooldowns` |
| 重启保留冷却、继续未完成、成功不重取 | SQLite 持久化作业/公告/冷却；启动时恢复未完成的（pending/fetching），成功的不重取 |
| 全部成功才开放下载 | `GET /jobs/{id}/download`：非全成功返回 409 |
| /docs 可采集、查询、下载 | Swagger UI 暴露全部端点；下载带 `Content-Disposition: attachment` |
| 按公告 ID 登记原件/更正件 | `POST /api/continuations`，`previous_id` + `announcement_id` |
| 原件须已采成功、更正件可在采集 | 校验 `previous.status == success`；更正件允许 pending/fetching |
| 每件至多一个前件/后件、拒绝成环 | PK + 活动边唯一索引 + 单事务校验；环检测在写入前 |
| 只提供链上最后一件已采成功 | `GET /api/corpus/effective`：取链上最后一个 `success` |
| 更正待采/失败则继续提供上一件并显示原因 | `state=pending/failed` + `pending_reason`/`failed_reason`，`document` 仍为上一件 |
| 失败更正可改挂新更正件 | 摘下失败边（`detached_at`）+ 新边插入，同事务；旧件进入 `replaced_corrections` |
| 登记与采集完成交错不出现双版本/漏供 | 链快照在同一把锁内一次读完（`corpus_snapshot`，单连接单锁）；测试并发轮询验证 |
| 重启后结论一致 | 接续边/状态持久化，启动恢复未完成任务；测试重启前后比对语料 |
| 原作业下载保持原样 | 未改动 `/jobs/{id}/download`，测试验证仍返回原始逐条文档 |

> 说明：登记更正件用的 `announcement_id` 来自 `POST /jobs` 返回的公告 ID
> （更正件可以先提交采集、再登记，甚至在采集途中登记）。

## 目录结构

```
api/              采集 API（FastAPI）
  app/main.py     端点与生命周期（启动时恢复冷却与未完成任务）
  app/engine.py   抓取引擎：全局/同源配额、冷却、跳转、重试
  app/db.py       SQLite 持久化（jobs / announcements / cooldowns / continuations）
  app/continuity.py 接续链解析：有效文本选择、待定/失败原因
  app/extract.py  HTML 标题正文提取
sites/            模拟招标站点（同一镜像以环境变量区分 site_a/site_b）
tests/            端到端自测脚本（本地进程模拟 compose 拓扑）
docker-compose.yml
```

## 自测

```bash
python3 tests/e2e_test.py                 # 全量：提取/跳转/限流/配额/下载门控/重启持久化
python3 tests/cross_job_cooldown_test.py  # 专项：跨作业共享冷却
python3 tests/continuation_test.py        # 专项：接续链/有效语料/交错/重启/下载
```

## 已知限制

- "源"按 `scheme://host:port` 判定；同一服务器的不同端口/主机名视为不同源。
- 失败（重试耗尽）是终态，重启不会自动重试失败公告；需重新提交作业
  （接续场景下正确做法是把新更正件登记到上一个成功件上）。
- 一条接续链只在**同一个 API 进程/同一个 SQLite 文件**内一致；本实现由
  Docker Compose 启动单实例 API，不支持多副本共享同一 DB 的横向扩展。
- 提取器面向常规 HTML 公告页，不执行 JavaScript。

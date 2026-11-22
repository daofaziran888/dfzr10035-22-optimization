# 网关工坊 · API接口采集与数据网关平台

面向第三方API接入注册、接口鉴权编排、限流熔断、响应解析、schema映射、结果缓存、重放恢复与交付订阅的API接口采集与数据网关平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 接口提供方 | `api_provider` | API提供方 | 提供方编号、提供方名称、认证方式 |
| 鉴权配置 | `auth_profile` | 鉴权方案 | 方案编号、提供方、鉴权类型 |
| 接口定义 | `endpoint_def` | 接口端点 | 端点编号、提供方、请求路径 |
| 限流熔断 | `rate_limit` | 限流策略 | 策略编号、目标端点、QPS上限 |
| 请求批次 | `request_batch` | 调用批次 | 批次编号、关联端点、参数模板 |
| 响应解析 | `response_parse` | 解析规则 | 规则编号、目标端点、JSON路径 |
| Schema差异 | `schema_diff` | 差异记录 | 记录编号、关联端点、变更字段 |
| 结果缓存 | `data_cache` | 缓存策略 | 策略编号、目标端点、缓存TTL |
| 重放队列 | `retry_queue` | 重放任务 | 任务编号、失败端点、失败原因 |
| 数据投递 | `data_delivery` | 投递管道 | 管道编号、数据来源、目标地址 |
| 订阅管理 | `subscriber` | 订阅方 | 订阅编号、订阅方、数据主题 |
| 接口监控 | `api_monitor` | 监控指标 | 指标编号、目标端点、监控类型 |
| Webhook路由 | `webhook_router` | 回调规则 | 规则编号、触发事件、目标URL |
| 传输加密 | `data_encrypt` | 加密配置 | 配置编号、数据范围、加密算法 |
| 调用日志 | `call_log` | 调用记录 | 记录编号、接口端点、请求参数 |
| 数据版本 | `data_version` | 数据快照 | 版本编号、数据来源、行数统计 |
| 管道审计 | `pipeline_audit` | 审计记录 | 记录编号、管道名称、操作人 |
| 治理报告 | `govern_report` | 治理报告 | 报告编号、评估范围、合规指标 |
| 增量追踪 | `incr_tracker` | 增量标记 | 标记编号、目标端点、增量字段 |
| 抽样校验 | `sample_check` | 抽样计划 | 计划编号、目标端点、抽样比例 |
| 用量计费 | `cost_account` | 用量账单 | 账单编号、提供方、计费周期 |
| 任务调度 | `task_schedule` | 调度计划 | 计划编号、关联端点、Cron表达式 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 分段采集与缓存快照

结果缓存的`强制刷新`按分段采集执行，缓存侧的事实收在
`backend/app/services/cache_snapshot.py`，交付账本收在
`backend/app/services/delivery_ledger.py`：

- 一个端点声明多个采集分区（默认两段，取自投递管道的`分区策略`，
  如 `partitions=p0,p1`）。全部分区提交成功才`发布`，发布是唯一的可见性翻转。
- 未发布版本（`采集中`/`已中断`）留在隐藏暂存区，`/api/chain/cache-stage/{endpoint}`
  可以区分隐藏数据与已发布快照；恢复时原位续写，已完成分区不会重复写入。
- 同一版本同一分区、同内容的重复提交只记一次写入（`commits` 保持 1）。
- 投递只读已发布版本：快照、交付清单、校验摘要在同一把锁内生成，
  一次投递两次取数的各段都来自同一版本；读不到完整快照时返回`未完成`、
  不写`已完成行数`、不创建数据版本。
- 发布结果同步到投递待办（固定`缓存版本`），交付记录按缓存版本生成，
  同一管道同一版本只生成一条；旧版本被取代后仍可按版本号复查。
- 数据版本登记`缓存版本`与`分区清单`，版本谱系统计只统计可追溯到完整分区的快照，
  并兼容历史完整结果（无缓存版本时从`依赖版本`/`版本标签`推断分区口径）。

相关接口：

| 接口 | 说明 |
| --- | --- |
| `GET /api/chain/cache-snapshot/{endpoint}?version=v0001` | 读取完整已发布快照，隐藏版本返回 409 |
| `GET /api/chain/cache-stage/{endpoint}` | 暂存区视图：区分采集中/已中断与已发布 |
| `GET /api/chain/delivery-records?delivery_id=1` | 交付账本，交付清单与版本统计分区一致 |

刷新动作支持备注，用于模拟分段写入与中断：
`{"action": "强制刷新", "remark": "分区=p0 中断"}`。
回归用例见 `backend/tests/test_cache_segments.py`。

领域：数据工程
子方向：数据采集、爬虫与RPA

# 流式导出工作台

边计算边下载的真实流式导出系统：用户断线后从**已确认的绝对行号**续传，后台源表
持续更新也不会污染已导出的结果。纯 Python 标准库实现（`http.server` + `sqlite3`），
无第三方依赖、无构建步骤，所有样例数据可用固定随机种子从零重建。

## 要解决的核心问题

1. **排序键会重复，查询还可能含聚合** —— 不能拿「最后一行的业务值」当续传游标。
   本系统用窗口函数 `ROW_NUMBER() OVER (用户的 ORDER BY ...)` 生成确定性绝对行号
   `_wb_rn`，游标只有「已确认行号 watermark」一个量，与业务列是否唯一完全无关。
2. **边计算边下载** —— result 模式下单生产者流式跑查询，每 500 行落一个分片；
   下载连接在数据尚未产出时阻塞等待下一帧，而不是报错或一次物化全部结果。
3. **断线、多连接、迟到确认不能跳行或重复** —— 每个下载连接带 fence 代次，
   共享单调 watermark；客户端再按帧的绝对行号区间二次校验（重复丢弃、缺口报错）。
4. **保留物会过期** —— TTL 到期回收分片/快照但保留元数据，续传返回 `410` 并说明
   为什么无法继续。

## 两种保留策略（创建导出时选择）

| 维度 | `result` 保留结果分片 | `snapshot` 固定输入快照 |
|---|---|---|
| 保留什么 | 查询产出的 CSV 分片（500 行/片） | 源库的在线一致性备份（`sqlite3` backup API） |
| 占用空间 | ≈ 导出结果大小 | ≈ 源库大小（与结果大小无关） |
| 恢复耗时 | 本地读分片，O(剩余字节)，无查询计算 | 按行号 keyset 分页**重算**，O(剩余行) 查询成本 |
| 源表继续更新 | 已产出分片不变；就绪前进程崩溃直接判失败，避免在变化的数据上拼出混合版本 | 快照定格在备份瞬间，任何时候重算都是同一份数据 |
| 到期后 | 分片删除，无法续传；重新导出得到的是新数据 | 快照删除，无法基于原数据重算；新快照不保证字节一致 |

两种模式走**同一个有效结果集 SQL**（窗口包装 + LIMIT/OFFSET 下推为行号过滤），
因此同一查询在两种模式下产出的文件与 sha256 完全一致。

## 快速开始

```bash
# 1. 从空目录启动（源库不存在会自动用固定种子重建 20000 订单 / ~50000 明细）
python3 -m app.server --port 8000
# 浏览器打开 http://127.0.0.1:8000

# 等价的脚本入口
python3 scripts/run.py rebuild --rows 20000
python3 scripts/run.py serve --port 8000

# 2. 端到端测试（自动使用独立临时数据目录与端口，跑完即删）
python3 -m tests.test_e2e
```

数据目录由环境变量 `WB_DATA_DIR` 控制，默认 `./data`；删掉该目录即可从零重来。

## 页面能做的验证（对应验收场景）

- **制造断线**：「模拟断线」只中止本地 HTTP 流（abort），不通知服务端；
  「正常关闭连接」会提升 fence。之后「打开/继续下载」从共享 watermark 续传。
- **更新源表**：插入/更新/删除/混合变更按钮，在导出进行中也可点击；
  已就绪导出的 sha256 保持不变。
- **恢复同一导出并校对**：页面按帧行号区间组装本地文件，用 Web Crypto 计算
  sha256，与服务端校验和逐字节比对。
- **取消**：立即释放保留物、关闭全部连接；之后的迟到确认与新开连接都被拒绝。

## HTTP 协议摘要

| 方法与路径 | 作用 |
|---|---|
| `POST /api/exports` | 创建导出 `{sql, mode: result|snapshot, ttl_seconds}` |
| `POST /api/exports/{id}/connections` | 打开下载连接，返回 `connection_id` 与 `fence` |
| `GET  /api/exports/{id}/connections/{cid}/stream` | NDJSON 流：`header` / `data`(带 `start_seq,end_seq`) / `eof` / `error` |
| `POST /api/exports/{id}/ack` | 确认 `{connection_id, fence, seq}`；重复确认幂等，越界/旧 fence 返回 409 |
| `POST /api/exports/{id}/connections/{cid}/close` | 正常关闭，fence +1 |
| `POST /api/exports/{id}/cancel` | 取消导出并释放保留物 |
| `GET  /api/exports/{id}/status` | 状态、水位、连接、保留空间、事件流 |
| `POST /api/source/mutate` / `rebuild` | 制造源表更新 / 重建样例库 |

## 确认协议的正确性规则（`app/manager.py`）

- ack 的 `seq` 不得超过该连接**已投递**的最大行号（防跳行）；
- `seq <=` 连接自身已确认位置时幂等返回 `duplicate`，水位不移动；
- 共享 watermark 只取 `max`，多个连接确认同一位置互不影响、绝不回退；
- fence 在连接关闭、导出取消、进程重启时递增，旧流的迟到 ack 一律 409；
- 所有状态变更持有导出锁后原子落盘 `meta.json`（临时文件 + rename）。

## 进程恢复

启动时扫描 `data/exports/*/meta.json`：

- `ready/completed` 且保留物完整 → 恢复为可续传，watermark 沿用持久化值；
- 就绪前崩溃（`running/preparing`）→ 标记 `failed`，要求重新发起（不悄悄重跑，
  避免在已变化的源表上把两个版本的数据拼进同一个文件）；
- 所有历史连接关闭并 fence+1，保证重启前的旧流确认全部失效。

## 目录结构

```
app/
  config.py    路径、TTL、分片/帧大小
  query.py     SQL 校验、顶层子句解析、窗口行号包装、有效结果集与分页
  source.py    可重建样例库、源表变更接口、预置查询
  csvutil.py   唯一的 CSV 编码入口（客户端与服务端同规范才能逐字节校对）
  manager.py   导出状态机、确认协议、两种保留策略、reaper、进程恢复
  server.py    ThreadingHTTPServer、NDJSON 流、静态页面
web/           无框架前端（index.html / app.js / styles.css）
tests/         90 项端到端断言，覆盖断线/迟到确认/多连接/重启/TTL/取消/聚合/LIMIT
scripts/run.py 命令行入口
```

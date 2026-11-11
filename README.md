# qmt-simlab

`qmt-simlab` 是一个可以独立运行的 QMT 风格交易仿真平台，面向量化策略开发、接口联调、回测前的交易流程验证和教学演示。项目包含仿真服务、浏览器管理页面、Python SDK，以及可选的 XM/QMT 网关桥接层，不依赖原有业务后端即可运行。

项目的 Python SDK 提供两层接口：

- `SimulationClient`：面向 qmt-simlab REST API 的完整类型化客户端。
- `qmt_sim_sdk.xtquant`：覆盖常用股票交易流程的 xtquant 风格兼容层，便于把已有策略迁移到仿真环境。

## 功能

- 多资金账户、初始资金和持仓配置
- SQLite 持久化，启动时自动创建数据表
- 限价单、市价单、买入、卖出、撤单和订单查询
- 全成、部分成交、拒单、撤单、手动成交等可配置场景
- 手续费、印花税、过户费和最低佣金配置
- TWAP、VWAP、POV、冰山单等算法任务接口
- REST API、Swagger 文档和内置浏览器管理页面
- 订单与成交回调轮询，支持 SDK 事件回调
- 可选 WebSocket 网关，用于接入已有交易执行器

## 快速开始

### 安装

```bash
git clone https://github.com/your-org/qmt-simlab.git
cd qmt-simlab

python -m venv .venv
# Windows PowerShell
.venv\Scripts\Activate.ps1
# macOS/Linux
# source .venv/bin/activate

pip install -e ".[dev]"
```

### 启动服务

```bash
qmt-simlab
```

也可以直接运行：

```bash
python -m qmt_simulator.server
```

启动后访问：

- 管理页面：<http://127.0.0.1:19720>
- API 文档：<http://127.0.0.1:19720/docs>
- 健康检查：<http://127.0.0.1:19720/api/status>

默认数据库为 `data/sim.db`。可通过 `DB_PATH`、`HTTP_HOST` 和 `HTTP_PORT` 环境变量修改运行配置；示例配置见 `.env.example`。

## Python SDK

```python
from qmt_sim_sdk import SimulationClient

client = SimulationClient("http://127.0.0.1:19720")
account = client.get_account("demo")
if account is None:
    account = client.create_account("demo", "Demo account", initial_cash=1_000_000)

result = client.submit_order(
    symbol="000001.SZ",
    direction="buy",
    quantity=100,
    account_id=account.id,
    price=10.0,
    wait=True,
)
print(result.status, result.filled_qty)
print(client.get_balance(account.id).to_dict())
```

完整的账户、资金、持仓、订单、成交、回调、手动成交和算法任务接口可以在 Swagger 页面中查看，也可以直接使用 `examples/quick_start.py`。

## SDK 行情注入与账号成交模式（0.3）

管理页面顶部新增 **使用说明 / SDK 行情**，包含字段说明和可复制的接入示例。
在左侧选择账号后，可以分别配置随机、手工、行情三种成交模式。
模式及行情设置只作用于新订单，已提交订单继续使用提交时的配置。

| 模式 | 值 | 行为 |
| --- | --- | --- |
| 随机 | `random` | 原有概率、批次与延迟场景；旧的 `fill_mode=auto` 仍可用 |
| 手工 | `manual` | 管理页“待成交”中决定成交、拒单或撤单 |
| 行情 | `quote` | 新订单及行情更新触发撮合，使用同一服务内的共享行情 |

在项目目录运行 `pip install -e .` 更新 SDK，然后运行下面的示例：

```python
from qmt_sim_sdk import SimulationClient

client = SimulationClient("http://127.0.0.1:19720")
client.create_account("QUOTE_DEMO", "行情演示", initial_cash=100000)
client.set_execution_mode("QUOTE_DEMO", "quote", quote_max_age_ms=3000,
                          quote_fallback="pending", use_depth=True)
order = client.submit_order("000001.SZ", "buy", 100, "QUOTE_DEMO", price=10.30)

# 在行情回调中持续调用；行情由所有账号共享，不需要传账号。
client.publish_quote(symbol="000001.SZ", last_price=10.23,
                     bid_price=10.22, ask_price=10.23,
                     bid_qty=12000, ask_qty=8000)
print(client.get_order(order.external_order_id).to_dict())
print(client.get_balance("QUOTE_DEMO").to_dict())
```

也可以运行 `python examples/quote_injection.py`，演示两笔行情分批撮合一笔订单。
批量调用 `client.publish_quotes([行情字典, ...])`，每批 1–1000 条；
`client.get_quote(symbol)` 与 `client.list_quotes()` 可查看最新快照。
`set_execution_mode` 保留账号其他配置；原来的 `update_account_config` 仍是完整替换。

| 字段 | 要求及含义 |
| --- | --- |
| `symbol` | 必填，含市场后缀的证券代码，例如 `000001.SZ`，统一转大写 |
| `last_price` | 必填，有限正数，价格单位元 |
| `ts` | Unix 毫秒时间戳，建议传行情产生时间；省略则使用服务端接收时间，不可超过服务端时间 30 秒 |
| `bid_price` / `ask_price` | 买一 / 卖一价，可选正数；缺失时用最新价近似撮合 |
| `bid_qty` / `ask_qty` | 买一 / 卖一数量，非负整数，单位股；0 表示无可成交量，缺失则不限制量 |
| `seq` | 可选非负整数，源内递增序号；用于重复、乱序过滤 |
| `trading_status` | `open`（默认）、`closed`、`halted`，后两者暂停撮合 |
| `source` | 可选行情源名称，供查询排查；不隔离行情流 |
| `last_qty` / `cum_volume` | 可选最新成交量 / 累计量，单位股，目前仅保存 |

每条消息是完整快照，未提供的字段不会保留上次值。一个证券应由一个行情源负责推送。
较旧的时间戳、相同时间且没有更大 seq、同一 UTC 日期内回退或重复的 seq 均被忽略。
重试时保留原始 ts/seq，避免把同一笔行情当成新的可用数量。单条响应中 `accepted` 为布尔值，
批量响应提供 `accepted` / `ignored` 数量；接受行情并不保证订单成交。
整个批次先校验，字段错误返回 HTTP 422 且不应用该批行情。

行情模式买入使用卖一、卖出使用买一；限价买单要求成交价不高于委托价，限价卖单要求不低于委托价。
开启 `use_depth` 时，同一账号的订单按提交顺序分配同一快照的盘口数量，耗尽后等待新快照。
不同账号各自拥有独立的模拟流动性；关闭深度限制或没有盘口量时，满足价格条件即可全部成交。
最新价模式是近似撮合，不模拟真实盘口排队。

默认 `quote_max_age_ms=3000`、`quote_fallback=pending`。缺失或过期行情会使订单继续挂起；
`reject` 会在提交或行情更新的撮合检查时拒单，`last` 允许使用过期快照，但无快照仍需等待。
已挂单不会单纯因为时间经过而触发一次新的撮合。`wait=True` 超时会撤单，并返回最终已成交数量。

成交以一个数据库事务更新订单、成交记录、现金与持仓，并复核含手续费的资金和可卖数量。
行情账号的市值和浮盈亏按最新价估值，缺失行情时使用成本价，持仓响应中的 `quote_stale` 标识过期或缺失；
成本价始终单独保留。随机和手工账号的估值保持原有行为。
服务端与网关算法在行情账号中统一从注入快照取价，动态报价算法启动前需有可用行情；
缺失时会失败并给出提示，不回退到固定价格。
POV 的模拟成交量和 VWAP 的预设分布仍沿用原实现，不会因注入累计量而变成真实量驱动算法。

本版行情只保存在内存，重启后需重新推送；已有挂单不自动恢复撮合，请在停服前撤单。
现有简化账户模型不冻结挂单资金、不模拟 T+1 或交易日历，不能等同于交易所级模拟。

REST 对应入口：`POST /api/market-data/quote`（对象）、`POST /api/market-data/quotes`（数组）、
`GET /api/market-data/quote/{symbol}`、`GET /api/market-data/quotes`、
`PUT /api/accounts/{account_id}/execution-mode`。详见运行服务的 `/docs`。

### 行情监控、代码查询与自动清理

管理页顶部点击 **行情监控**，无需选择账号即可查看全局行情。默认每 2 秒刷新，展示最近接收时间、
本次启动的接收 / 接受 / 重复或乱序忽略数量，以及缓存中的新鲜、过期行情数量。
表格包含最新价、买卖一价量、行情时间、接收时间、来源和交易状态，支持搜索、筛选、分页及完整 JSON 查看。
“最近收到推送”只表示服务最近收到了有效格式的请求，不代表行情源保持连接，也不保证行情时间足够新鲜。

```python
from qmt_sim_sdk import SimulationClient

client = SimulationClient("http://127.0.0.1:19720")
print(client.market_data_status())                      # 全局推送状态和清理策略
print(client.list_quotes(max_age_ms=3000))               # 所有最新快照，含过期标记
print(client.get_quote("000001.SZ"))                    # 无快照时返回 None
```

也可运行 `python examples/quote_monitor.py --symbol 000001.SZ`，或直接调用：

| 查询 | REST 接口 |
| --- | --- |
| 推送状态、接收计数、缓存与清理统计 | `GET /api/market-data/status` |
| 最新行情列表 | `GET /api/market-data/quotes` |
| 指定证券最新行情，无缓存时 HTTP 404 | `GET /api/market-data/quote/{symbol}` |

以上查询均可传 `max_age_ms`（默认 3000），仅改变监控的新鲜判定，不会修改账号撮合设置。
快照的 `ts` / `age_ms` 表示源行情时间及年龄，`received_at` / `received_age_ms` 表示这条快照被接受的时间及距今时长，
时间戳单位都是 Unix 毫秒。状态里的 `last_received_at` 包含被忽略的重复或乱序推送，
`last_accepted_at` 则只计被接受的新快照；格式不合法的请求不计入行情接收数量。

**行情不保存到 SQLite**，内存中每个证券只保留一条最新快照，没有逐笔行情历史表。
SQLite 保存账号、资金、持仓、订单与成交记录，行情清理不会删除这些业务数据。
为避免无人使用的证券不断占用内存，服务有两层自动清理：

- 默认 **1 小时未接受新行情**就移除快照；重复或乱序推送不延长保留时间。
- 最多保留 **10,000 个证券**，达到上限时淘汰最久未接受新行情的证券。

后台每 60 秒检查一次，读写行情时也检查；同时释放撮合模块对旧快照的引用。
启动服务前可用环境变量 `QUOTE_RETENTION_MS` 和 `QUOTE_MAX_SYMBOLS` 调整，均须为正整数，见 `.env.example`。
缓存保留期限与账号的 `quote_max_age_ms` 不同：前者控制内存占用，后者决定是否允许用于撮合。
使用 `quote_fallback=last` 也只能使用尚在缓存里的行情，清理后仍需等待新推送。
重启会清空行情和计数；快照被清理后，其时间戳 / 序号去重状态也会释放，重新发送旧消息可能被当作新快照，
因此接入程序应持续发送当前行情，不应依赖服务长期保存历史去重记录。

## xtquant 风格兼容接口

常见的股票策略可以使用熟悉的 trader、account、constant 和 callback 结构。兼容层的实现参考 [xtquant 官方交易接口文档](https://dict.thinktrader.net/nativeApi/xttrader.html)，并通过 `server_url` 指向本地仿真服务：

```python
from qmt_sim_sdk.xtquant import xtconstant
from qmt_sim_sdk.xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from qmt_sim_sdk.xtquant.xttype import StockAccount


class Callback(XtQuantTraderCallback):
    def on_stock_order(self, order):
        print("order:", order.order_id, order.order_status)

    def on_stock_trade(self, trade):
        print("trade:", trade.trade_id, trade.traded_volume)


trader = XtQuantTrader("", 1001, server_url="http://127.0.0.1:19720")
account = StockAccount("demo")
trader.register_callback(Callback())
trader.start()
assert trader.connect() == 0
trader.subscribe(account)

order_id = trader.order_stock(
    account,
    "000001.SZ",
    xtconstant.STOCK_BUY,
    100,
    xtconstant.FIX_PRICE,
    10.0,
    "demo_strategy",
    "first order",
)
print("submitted:", order_id)
```

当前兼容层包含以下常用方法：

| 类型 | 方法 |
| --- | --- |
| 生命周期 | `register_callback`、`start`、`connect`、`stop`、`run_forever` |
| 订阅 | `subscribe`、`unsubscribe` |
| 交易 | `order_stock`、`order_stock_async` |
| 撤单 | `cancel_order_stock`、`cancel_order_stock_async`、`cancel_order_stock_sysid` |
| 查询 | `query_stock_asset`、`query_stock_orders`、`query_stock_order`、`query_stock_trades`、`query_stock_positions`、`query_stock_position` |
| 回调 | `on_stock_order`、`on_stock_trade`、`on_order_error`、`on_cancel_error`、`on_order_stock_async_response`、`on_account_status` |

为避免覆盖用户机器上的专有 `xtquant` 包，推荐使用上面的 `qmt_sim_sdk.xtquant` 命名空间。该层主要兼容普通股票交易；行情订阅、融资融券、期货、期权和柜台专有功能不在当前范围内。策略迁移时通常只需替换导入路径，并给 `XtQuantTrader` 增加 `server_url`。

## 可选网关

仿真服务可以单独运行。需要把它接入现有 XM/QMT 执行器时，再启动网关桥接层：

```bash
qmt-simlab-gateway
```

也可以运行 `python -m qmt_simulator.gateway.gateway_main`。网关通过 `SIM_SERVER_URL`、`WS_URL`、`BACKEND_URL` 和 `EXECUTOR_ID` 等环境变量配置连接地址，不影响仿真服务本身的 REST API。

## 测试

```bash
pytest -q
```

测试覆盖仿真账户、订单引擎、成交场景、REST SDK 和 xtquant 风格兼容层。

## 项目结构

```text
qmt-simlab/
├── qmt_simulator/       # 仿真服务、数据库、订单引擎和可选网关
├── qmt_sim_sdk/         # REST SDK 与 xtquant 风格兼容接口
├── examples/            # 使用示例
├── tests/               # 自动化测试
├── scripts/              # Windows 启动脚本
├── data/                # 运行时 SQLite 数据（默认不提交）
├── pyproject.toml
└── README.md
```

## License

本项目采用 [MIT License](LICENSE) 开源协议。

Copyright (c) 2026 QMT SimLab Contributors。

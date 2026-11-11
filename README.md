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

MIT License。发布到 GitHub 前，请根据实际版权主体更新 `LICENSE` 和项目元数据。

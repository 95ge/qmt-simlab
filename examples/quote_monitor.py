"""Read current feed status and quote snapshots without changing simulation data."""

import argparse
import json

from qmt_sim_sdk import SimulationClient


def main():
    parser = argparse.ArgumentParser(description="查看行情推送状态和最新快照")
    parser.add_argument("--server-url", default="http://127.0.0.1:19720")
    parser.add_argument("--symbol", help="证券代码；省略时查询全部缓存行情")
    parser.add_argument("--max-age-ms", type=int, default=3000, help="监控新鲜阈值，不修改账号设置")
    args = parser.parse_args()
    if args.max_age_ms < 0:
        parser.error("--max-age-ms 必须为非负整数")
    client = SimulationClient(args.server_url)
    try:
        result = {"status": client.market_data_status(max_age_ms=args.max_age_ms)}
        if args.symbol:
            result["quote"] = client.get_quote(args.symbol, max_age_ms=args.max_age_ms)
        else:
            result["quotes"] = client.list_quotes(max_age_ms=args.max_age_ms)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    finally:
        client.session.close()


if __name__ == "__main__":
    main()

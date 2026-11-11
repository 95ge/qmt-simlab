"""Run after starting qmt-simlab: python examples/quote_injection.py."""

import time
import uuid

from qmt_sim_sdk import SimulationClient


def main():
    client = SimulationClient()
    account_id = f"QUOTE_{uuid.uuid4().hex[:8]}"
    client.create_account(account_id, "SDK 行情演示", initial_cash=100000)
    client.set_execution_mode(account_id, "quote", quote_max_age_ms=3000, use_depth=True)
    order = client.submit_order("000001.SZ", "buy", 100, account_id, price=10.30)
    ts = int(time.time() * 1000)
    # Send partial liquidity, then another complete snapshot for the remainder.
    for offset, qty in enumerate((40, 60)):
        client.publish_quote("000001.SZ", 10.23, ts=ts + offset,
                             bid_price=10.22, ask_price=10.23, bid_qty=12000, ask_qty=qty)
        print(client.get_order(order.external_order_id).to_dict())
    print("Account:", account_id)
    print("Balance:", client.get_balance(account_id).to_dict())


if __name__ == "__main__":
    main()

"""Create an account, submit one simulated order and inspect the result."""

from qmt_sim_sdk import SimulationClient


def main() -> None:
    client = SimulationClient("http://127.0.0.1:19720")
    print("server:", client.health())

    account_id = "demo-account"
    if client.get_account(account_id) is None:
        client.create_account(account_id, "Demo account", initial_cash=1_000_000)

    result = client.submit_order(
        symbol="000001.SZ",
        direction="buy",
        quantity=100,
        account_id=account_id,
        price=10.0,
        wait=True,
        timeout=30,
    )
    print("order:", result.to_dict())
    print("balance:", client.get_balance(account_id).to_dict())
    print("positions:", [position.to_dict() for position in client.get_positions(account_id)])


if __name__ == "__main__":
    main()


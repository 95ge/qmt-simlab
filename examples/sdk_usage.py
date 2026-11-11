"""A minimal example for external Python strategies."""

from qmt_sim_sdk import SimulationClient


client = SimulationClient()
accounts = client.list_accounts()
if not accounts:
    raise RuntimeError("Create a simulator account in the web UI first")

account = accounts[0]
print(f"Using {account.id}, cash={account.cash:.2f}")
print(client.submit_order(
    symbol="600000.SH",
    direction="buy",
    quantity=100,
    account_id=account.id,
    price=10.0,
).to_dict())


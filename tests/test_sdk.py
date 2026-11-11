from qmt_sim_sdk import Account, SimulationClient


class FakeResponse:
    status_code = 200
    content = b"{}"

    def __init__(self, payload):
        self._payload = payload
        self.content = b"1"

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self):
        self.headers = {}
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if url.endswith("/api/accounts"):
            return FakeResponse([{"id": "ACC1", "name": "demo", "cash": 1000}])
        return FakeResponse({"status": "healthy"})


def test_sdk_lists_typed_accounts():
    session = FakeSession()
    client = SimulationClient("http://simulator", session=session)

    accounts = client.list_accounts()

    assert isinstance(accounts[0], Account)
    assert accounts[0].id == "ACC1"
    assert session.calls[0][0] == "GET"


def test_sdk_health():
    client = SimulationClient("http://simulator", session=FakeSession())
    assert client.health()["status"] == "healthy"


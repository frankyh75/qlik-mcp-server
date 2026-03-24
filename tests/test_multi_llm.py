import pathlib
import sys
from unittest.mock import MagicMock

root = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))

from qlik_sense_mcp_server.multi_llm import MultiLLMTransport


def test_dispatch_adds_auth_header_and_returns_text():
    client = MagicMock()
    response = MagicMock()
    response.json.return_value = {"text": "ok"}
    response.raise_for_status.return_value = None
    client.post.return_value = response
    client.headers = {"Content-Type": "application/json"}

    transport = MultiLLMTransport(
        endpoints={"openrouter": "https://openrouter.ai"},
        api_keys={"openrouter": "token"},
        client=client,
    )

    result = transport.dispatch("openrouter", "model", "prompt")

    client.post.assert_called_once()
    assert "Authorization" in client.post.call_args.kwargs["headers"]
    assert result["text"] == "ok"


def test_broadcast_aggregates_responses():
    client = MagicMock()
    client.headers = {"Content-Type": "application/json"}
    response = MagicMock()
    response.json.return_value = {"text": "res"}
    response.raise_for_status.return_value = None
    client.post.return_value = response

    transport = MultiLLMTransport(
        endpoints={"one": "https://a", "two": "https://b"},
        api_keys={},
        client=client,
    )

    results = transport.broadcast("model", "prompt")

    assert len(results) == 2
    assert results[0]["provider"] == "one"
    assert results[1]["provider"] == "two"

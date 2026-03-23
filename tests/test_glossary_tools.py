import pathlib
import sys
from unittest.mock import MagicMock

root = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))

from qlik_sense_mcp_server.glossary_tools import QlikGlossaryClient


def _build_server_url() -> str:
    return "https://qlik.example.com"


def test_list_glossaries_calls_cloud_endpoint():
    mock_client = MagicMock()
    response = MagicMock()
    response.raise_for_status.return_value = None
    response.json.return_value = {"glossaries": [{"id": "g1"}]}
    mock_client.get.return_value = response

    client = QlikGlossaryClient(server_url=_build_server_url(), client=mock_client)
    result = client.list_glossaries()

    mock_client.get.assert_called_once_with("https://qlik.example.com/api/v1/glossaries")
    assert result == [{"id": "g1"}]


def test_list_terms_returns_entries():
    mock_client = MagicMock()
    response = MagicMock()
    response.raise_for_status.return_value = None
    response.json.return_value = {"data": [{"id": "term-1"}]}
    mock_client.get.return_value = response

    client = QlikGlossaryClient(server_url=_build_server_url(), client=mock_client)
    result = client.list_terms("gloss-1")

    mock_client.get.assert_called_once_with(
        "https://qlik.example.com/api/v1/glossaries/gloss-1/entries"
    )
    assert result == [{"id": "term-1"}]


def test_create_term_posts_payload():
    mock_client = MagicMock()
    response = MagicMock()
    response.raise_for_status.return_value = None
    response.json.return_value = {"id": "term-2"}
    mock_client.post.return_value = response

    client = QlikGlossaryClient(server_url=_build_server_url(), client=mock_client)
    result = client.create_term(
        "gloss-1", "Revenue", "Sales total", synonyms=["Sales"]
    )

    mock_client.post.assert_called_once_with(
        "https://qlik.example.com/api/v1/glossaries/gloss-1/entries",
        json={"term": "Revenue", "definition": "Sales total", "synonyms": ["Sales"]},
    )
    assert result == {"id": "term-2"}


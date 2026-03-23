import pathlib
import sys

root = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))

from unittest.mock import MagicMock

sys.modules.setdefault("websocket", MagicMock())

from qlik_sense_mcp_server.insight_tools.field_stats import collect_field_statistics


def test_collect_field_statistics_calls_engine_methods():
    api = MagicMock()
    api.open_doc_safe.return_value = {"qReturn": {"qHandle": 42}}

    def send_request(method, params=None, handle=-1):
        if method == "GetFieldList":
            return {"qFieldList": {"qItems": [{"qName": "FieldA"}]}}
        if method == "GetField":
            return {"qReturn": {"qHandle": 7}}
        if method == "GetLayout":
            return {
                "qLayout": {
                    "qCardinal": 123,
                    "qStateCounts": {"qLocked": 1},
                    "qSize": {"qTotal": 999},
                }
            }
        if method == "DestroyObject":
            return {}
        return {}

    api.send_request.side_effect = send_request

    result = collect_field_statistics(api, "app-foo")

    assert result == [
        {
            "name": "FieldA",
            "cardinal": 123,
            "state_counts": {"qLocked": 1},
            "size_total": 999,
        }
    ]
    api.close_doc.assert_called_once_with(42)


if __name__ == "__main__":
    test_collect_field_statistics_calls_engine_methods()
    print("Field stats test passed")

import pathlib
import sys

root = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))

from unittest.mock import MagicMock

sys.modules.setdefault("websocket", MagicMock())

from qlik_sense_mcp_server.insight_tools.master_items import list_master_items


def test_list_master_items_maps_catalog_entries():
    api = MagicMock()
    api.open_doc_safe.return_value = {"qReturn": {"qHandle": 5}}

    def send_request(method, params=None, handle=-1):
        if method == "GetObjects":
            return {
                "qList": {
                    "qItems": [
                        {
                            "qInfo": {"qId": "mi-1"},
                            "qMeta": {"qType": "masterobject", "title": "Revenue"},
                        }
                    ]
                }
            }
        return {}

    api.send_request.side_effect = send_request

    catalog = list_master_items(api, "app-x")

    assert catalog == [
        {"id": "mi-1", "type": "masterobject", "title": "Revenue"}
    ]
    api.close_doc.assert_called_once_with(5)


if __name__ == "__main__":
    test_list_master_items_maps_catalog_entries()
    print("Master items test passed")

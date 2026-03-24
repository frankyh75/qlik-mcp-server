import pathlib
import sys
from unittest.mock import MagicMock

root = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))

from qlik_sense_mcp_server.dataset_tools import list_dataset_fields, profile_dataset


def test_list_dataset_fields_uses_field_list():
    api = MagicMock()
    api.open_doc_safe.return_value = {"qReturn": {"qHandle": 10}}
    api.send_request.return_value = {"qFieldList": {"qItems": [{"qName": "Field1"}]}}

    fields = list_dataset_fields(api, "app", "dataset")

    assert fields == ["Field1"]
    api.close_doc.assert_called_once_with(10)


def test_profile_dataset_returns_summary():
    api = MagicMock()
    api.open_doc_safe.return_value = {"qReturn": {"qHandle": 20}}
    api.send_request.side_effect = [
        {"qReturn": {"qHandle": 7}},
        {"qLayout": {"qHyperCube": {"qSize": {"qcy": 3, "qcx": 2}, "qDimensionInfo": [{}]}}},
    ]

    summary = profile_dataset(api, "app", "data", fetch_size=60)

    assert summary["dataset_id"] == "data"
    assert summary["rows"] == 3
    assert summary["columns"] == 2
    assert summary["fetch_size"] == 60
    assert summary["field_count"] == 1
    api.close_doc.assert_called_once_with(20)

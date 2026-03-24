import pathlib
import sys
from unittest.mock import MagicMock

root = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))

from qlik_sense_mcp_server.sheet_tools import (
    describe_sheet,
    list_sheet_titles,
    update_visualization,
)


def _mock_api(items=None, layout=None, handle=7):
    api = MagicMock()
    api.open_doc_safe.return_value = {"qReturn": {"qHandle": 42}}
    if items is None:
        items = []
    api.send_request.side_effect = [
        {"qAppSheetList": {"qItems": items}},
        {"qReturn": {"qHandle": handle}},
        {"qLayout": layout or {"metadata": True}},
    ]
    return api


def test_list_sheet_titles_returns_titles_from_engine():
    items = [
        {"qMeta": {"title": "Sheet A"}},
        {"qMeta": {"title": "Sheet B"}},
    ]
    api = MagicMock()
    api.open_doc_safe.return_value = {"qReturn": {"qHandle": 1}}
    api.send_request.side_effect = [
        {"qAppSheetList": {"qItems": items}},
    ]

    titles = list_sheet_titles(api, "app-1")

    assert titles == ["Sheet A", "Sheet B"]
    api.close_doc.assert_called_once_with(1)


def test_describe_sheet_returns_layout():
    api = MagicMock()
    api.open_doc_safe.return_value = {"qReturn": {"qHandle": 2}}
    api.send_request.side_effect = [
        {"qReturn": {"qHandle": 3}},
        {"qLayout": {"obj": True}},
    ]

    layout = describe_sheet(api, "app-2", "sheetX")

    assert layout == {"obj": True}
    api.close_doc.assert_called_once_with(2)


def test_update_visualization_returns_false_when_missing_handle():
    api = MagicMock()
    api.open_doc_safe.return_value = {"qReturn": {"qHandle": 3}}
    api.send_request.return_value = {"qReturn": {}}

    result = update_visualization(api, "app-3", "id", {"foo": "bar"})

    assert result is False
    api.close_doc.assert_called_once_with(3)


def test_update_visualization_calls_set_properties_when_handle_present():
    api = MagicMock()
    api.open_doc_safe.return_value = {"qReturn": {"qHandle": 4}}
    api.send_request.side_effect = [
        {"qReturn": {"qHandle": 5}},
        {},
    ]

    result = update_visualization(api, "app-3", "obj", {"legend": True})

    assert result is True
    api.send_request.assert_called_with("SetProperties", [{"legend": True}], handle=5)
    api.close_doc.assert_called_once_with(4)

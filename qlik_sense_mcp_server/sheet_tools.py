"""Sheet and visualization builder helpers for MCP."""
from __future__ import annotations

from typing import Any, Dict, List

from qlik_sense_mcp_server.engine_api import QlikEngineAPI


def list_sheet_titles(api: QlikEngineAPI, app_id: str) -> List[str]:
    """Return a list of sheet titles for the given app."""
    doc = api.open_doc_safe(app_id)
    doc_handle = doc["qReturn"]["qHandle"]
    try:
        sheets = api.send_request("GetAppSheetList", [], handle=doc_handle).get("qAppSheetList", {}).get("qItems", [])
        return [sheet.get("qMeta", {}).get("title", "") for sheet in sheets if sheet.get("qMeta")]
    finally:
        api.close_doc(doc_handle)


def describe_sheet(api: QlikEngineAPI, app_id: str, sheet_id: str) -> Dict[str, Any]:
    """Return metadata for a specific sheet."""
    doc = api.open_doc_safe(app_id)
    doc_handle = doc["qReturn"]["qHandle"]
    try:
        params = {"qId": sheet_id}
        obj = api.send_request("GetObject", params, handle=doc_handle)
        layout = api.send_request("GetLayout", [], handle=obj.get("qReturn", {}).get("qHandle", -1))
        return layout.get("qLayout", {})
    finally:
        api.close_doc(doc_handle)


def update_visualization(api: QlikEngineAPI, app_id: str, object_id: str, properties: Dict[str, Any]) -> bool:
    """Apply new visualization properties."""
    doc = api.open_doc_safe(app_id)
    doc_handle = doc["qReturn"]["qHandle"]
    try:
        obj = api.send_request("GetObject", {"qId": object_id}, handle=doc_handle)
        handle = obj.get("qReturn", {}).get("qHandle")
        if handle is None:
            return False
        api.send_request("SetProperties", [properties], handle=handle)
        return True
    finally:
        api.close_doc(doc_handle)

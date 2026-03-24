"""Master item exploration helpers for Qlik Sense MCP."""

from typing import TYPE_CHECKING, Any, Dict, List

if TYPE_CHECKING:
    from qlik_sense_mcp_server.engine_api import QlikEngineAPI
else:
    QlikEngineAPI = Any


def list_master_items(api: QlikEngineAPI, app_id: str) -> List[Dict[str, Any]]:
    """Return a simplified catalog of master items and catalog entries."""
    doc = api.open_doc_safe(app_id)
    doc_handle = doc["qReturn"]["qHandle"]
    try:
        params = {
            "qOptions": {
                "qTypes": ["masterobject"],
                "qIncludeSessionObjects": False,
            }
        }
        response = api.send_request("GetObjects", [params], handle=doc_handle)
        items = response.get("qList", {}).get("qItems", [])
        return [
            {
                "id": item.get("qInfo", {}).get("qId"),
                "type": item.get("qMeta", {}).get("qType"),
                "title": item.get("qMeta", {}).get("title"),
            }
            for item in items
        ]
    finally:
        api.close_doc(doc_handle)

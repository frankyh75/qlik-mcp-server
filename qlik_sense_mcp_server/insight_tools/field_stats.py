"""Insight helper that summarizes field statistics via the Engine API."""

from typing import TYPE_CHECKING, Any, Dict, List

if TYPE_CHECKING:
    from qlik_sense_mcp_server.engine_api import QlikEngineAPI
else:
    QlikEngineAPI = Any


def collect_field_statistics(
    api: QlikEngineAPI, app_id: str, limit: int | None = None
) -> List[Dict[str, Any]]:
    """Collect basic statistics for each field inside a Qlik Sense document."""
    doc = api.open_doc_safe(app_id)
    doc_handle = doc["qReturn"]["qHandle"]
    try:
        field_list = (
            api.send_request("GetFieldList", [], handle=doc_handle)
            .get("qFieldList", {})
            .get("qItems", [])
        )
        max_fields = limit if limit is not None else len(field_list)
        summary: List[Dict[str, Any]] = []
        for entry in field_list[:max_fields]:
            name = entry.get("qName")
            if not name:
                continue
            field_response = api.send_request("GetField", [name], handle=doc_handle)
            field_handle = field_response.get("qReturn", {}).get("qHandle")
            if field_handle is None:
                continue
            layout = api.send_request("GetLayout", [], handle=field_handle).get("qLayout", {})
            summary.append(
                {
                    "name": name,
                    "cardinal": layout.get("qCardinal"),
                    "state_counts": layout.get("qStateCounts", {}),
                    "size_total": layout.get("qSize", {}).get("qTotal"),
                }
            )
            api.send_request("DestroyObject", [], handle=field_handle)
        return summary
    finally:
        api.close_doc(doc_handle)

"""Dataset stewardship helpers for Qlik Sense MCP."""
from __future__ import annotations

from typing import Any, Dict, List

from qlik_sense_mcp_server.engine_api import QlikEngineAPI


def list_dataset_fields(api: QlikEngineAPI, app_id: str, dataset_id: str) -> List[str]:
    """Return a list of dataset field names (mocked via fieldlist)."""
    doc = api.open_doc_safe(app_id)
    doc_handle = doc["qReturn"]["qHandle"]
    try:
        fields = api.send_request("GetFieldList", [], handle=doc_handle).get("qFieldList", {}).get("qItems", [])
        return [field.get("qName", "") for field in fields if field.get("qName")]
    finally:
        api.close_doc(doc_handle)


def profile_dataset(
    api: QlikEngineAPI,
    app_id: str,
    dataset_id: str,
    fetch_size: int = 100,
) -> Dict[str, Any]:
    """Describe dataset usage by creating a temporary cube."""
    doc = api.open_doc_safe(app_id)
    doc_handle = doc["qReturn"]["qHandle"]
    cube_handle = None
    try:
        request = {
            "qInfo": {"qType": "cube"},
            "qHyperCubeDef": {
                "qDimensions": [{"qDef": {"qFieldDefs": [f"Dataset::{dataset_id}"]}}],
                "qMeasures": [
                    {"qDef": {"qDef": "Count(DISTINCT DatasetId)"}}
                ],
                "qInitialDataFetch": [{"qTop": 0, "qLeft": 0, "qHeight": fetch_size, "qWidth": 1}],
            },
        }
        create_result = api.send_request("CreateSessionObject", [request], handle=doc_handle)
        cube_handle = create_result.get("qReturn", {}).get("qHandle")
        layout = api.send_request("GetLayout", [], handle=cube_handle).get("qLayout", {})
        cube = layout.get("qHyperCube", {})
        size = cube.get("qSize", {})
        return {
            "dataset_id": dataset_id,
            "rows": size.get("qcy"),
            "columns": size.get("qcx"),
            "fetch_size": fetch_size,
            "field_count": len(cube.get("qDimensionInfo", [])),
        }
    finally:
        if cube_handle:
            api.send_request("DestroySessionObject", [], handle=cube_handle)
        api.close_doc(doc_handle)

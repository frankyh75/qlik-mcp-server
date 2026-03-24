"""Helper that evaluates qHyperCube health characteristics."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from qlik_sense_mcp_server.engine_api import QlikEngineAPI
else:
    QlikEngineAPI = Any


def _build_hypercube_def(
    dimensions: List[str], measures: List[str], max_rows: int, max_columns: int
) -> Dict[str, Any]:
    return {
        "qInfo": {"qType": "cube"},
        "qHyperCubeDef": {
            "qDimensions": [{"qDef": {"qDef": dim}} for dim in dimensions],
            "qMeasures": [{"qDef": {"qDef": meas}} for meas in measures],
            "qInitialDataFetch": [
                {"qTop": 0, "qLeft": 0, "qHeight": max_rows, "qWidth": max_columns}
            ],
        },
    }


def measure_hypercube_health(
    api: QlikEngineAPI,
    app_id: str,
    dimensions: List[str],
    measures: List[str],
    max_rows: int = 20,
    max_columns: int = 5,
) -> Dict[str, Any]:
    """Create a temporary hypercube and surface health metrics."""
    doc = api.open_doc_safe(app_id)
    doc_handle = doc["qReturn"]["qHandle"]
    cube_handle: Optional[int] = None
    try:
        hypercube_def = _build_hypercube_def(dimensions, measures, max_rows, max_columns)
        create_result = api.send_request("CreateSessionObject", [hypercube_def], handle=doc_handle)
        cube_handle = create_result.get("qReturn", {}).get("qHandle")
        layout = api.send_request("GetLayout", [], handle=cube_handle).get("qLayout", {})
        cube = layout.get("qHyperCube", {})
        size = cube.get("qSize", {})
        return {
            "rows": size.get("qcy"),
            "columns": size.get("qcx"),
            "pages": size.get("qcg"),
            "dimension_count": len(cube.get("qDimensionInfo", [])),
            "measure_count": len(cube.get("qMeasureInfo", [])),
            "effective_columns": cube.get("qEffectiveInterColumnSortOrder", []),
        }
    finally:
        if cube_handle:
            api.send_request("DestroySessionObject", [], handle=cube_handle)
        api.close_doc(doc_handle)

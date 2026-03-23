import pathlib
import sys

root = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))

from unittest.mock import MagicMock

sys.modules.setdefault("websocket", MagicMock())

from qlik_sense_mcp_server.insight_tools.hypercube_health import measure_hypercube_health


def test_measure_hypercube_health_collects_geometry():
    api = MagicMock()
    api.open_doc_safe.return_value = {"qReturn": {"qHandle": 101}}

    def send_request(method, params=None, handle=-1):
        if method == "CreateSessionObject":
            return {"qReturn": {"qHandle": 7}}
        if method == "GetLayout":
            return {
                "qLayout": {
                    "qHyperCube": {
                        "qSize": {"qcy": 12, "qcx": 3, "qcg": 1},
                        "qDimensionInfo": [{}, {}],
                        "qMeasureInfo": [{}],
                        "qEffectiveInterColumnSortOrder": [0, 1, 2],
                    }
                }
            }
        if method == "DestroySessionObject":
            return {}
        return {}

    api.send_request.side_effect = send_request

    result = measure_hypercube_health(api, "app-foo", ["dim1"], ["sum(measure)"])

    assert result["rows"] == 12
    assert result["columns"] == 3
    assert result["dimension_count"] == 2
    assert result["measure_count"] == 1
    assert result["effective_columns"] == [0, 1, 2]
    api.close_doc.assert_called_once_with(101)


if __name__ == "__main__":
    test_measure_hypercube_health_collects_geometry()
    print("Hypercube health test passed")

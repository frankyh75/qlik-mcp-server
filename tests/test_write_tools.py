"""Unit tests for WRITE tools (create_measure, create_variable, etc.)"""

import pytest
from unittest.mock import MagicMock, patch
from src.qlik_client import QlikClient
from src.server import (
    handle_create_bar_chart,
    handle_create_kpi,
    handle_create_line_chart,
    handle_create_table,
)
from src.tools import (
    CreateBarChartArgs,
    CreateKpiArgs,
    CreateLineChartArgs,
    CreateMeasureArgs,
    CreateVariableArgs,
    CreateDimensionArgs,
    CreateSheetArgs,
    CreateTableArgs,
    CreateObjectArgs,
    GetSheetLayoutArgs,
    RepositionSheetObjectArgs,
    PlotDimension,
    PlotMeasure,
    create_bar_chart,
    create_kpi,
    create_line_chart,
    create_table,
    get_sheet_layout,
    reposition_sheet_object,
)


class TestCreateMeasureArgs:
    """Tests for CreateMeasureArgs validation"""

    def test_valid_args(self):
        """Test valid measure creation arguments"""
        args = CreateMeasureArgs(
            app_id="test-app-id",
            title="Revenue",
            expression="Sum(Sales)",
            description="Total revenue",
            label="Revenue Label",
            tags=["finance", "kpi"],
        )
        assert args.app_id == "test-app-id"
        assert args.title == "Revenue"
        assert args.expression == "Sum(Sales)"

    def test_minimal_args(self):
        """Test minimal required arguments"""
        args = CreateMeasureArgs(
            app_id="test-app-id",
            title="Revenue",
            expression="Sum(Sales)",
        )
        assert args.description == ""
        assert args.label == ""
        assert args.tags is None

    def test_empty_app_id_raises(self):
        """Test that empty app_id raises validation error"""
        with pytest.raises(ValueError):
            CreateMeasureArgs(app_id="", title="Test", expression="Sum(X)")

    def test_empty_title_raises(self):
        """Test that empty title raises validation error"""
        with pytest.raises(ValueError):
            CreateMeasureArgs(app_id="test", title="", expression="Sum(X)")

    def test_empty_expression_raises(self):
        """Test that empty expression raises validation error"""
        with pytest.raises(ValueError):
            CreateMeasureArgs(app_id="test", title="Test", expression="")


class TestCreateVariableArgs:
    """Tests for CreateVariableArgs validation"""

    def test_valid_args(self):
        """Test valid variable creation arguments"""
        args = CreateVariableArgs(
            app_id="test-app-id",
            name="vThreshold",
            definition="100000",
            comment="Sales threshold",
        )
        assert args.name == "vThreshold"
        assert args.definition == "100000"

    def test_minimal_args(self):
        """Test minimal required arguments"""
        args = CreateVariableArgs(
            app_id="test-app-id",
            name="vTest",
        )
        assert args.definition == ""
        assert args.comment == ""

    def test_empty_name_raises(self):
        """Test that empty name raises validation error"""
        with pytest.raises(ValueError):
            CreateVariableArgs(app_id="test", name="")


class TestCreateDimensionArgs:
    """Tests for CreateDimensionArgs validation"""

    def test_valid_args_single_field(self):
        """Test valid dimension creation with single field"""
        args = CreateDimensionArgs(
            app_id="test-app-id",
            title="Region",
            field_def="Region",
            description="Geographic region",
        )
        assert args.field_def == "Region"
        assert args.grouping == "N"

    def test_valid_args_drill_down(self):
        """Test valid drill-down dimension"""
        args = CreateDimensionArgs(
            app_id="test-app-id",
            title="Location",
            field_def=["Country", "Region", "City"],
            grouping="H",
        )
        assert args.field_def == ["Country", "Region", "City"]
        assert args.grouping == "H"

    def test_invalid_grouping_raises(self):
        """Test that invalid grouping raises validation error"""
        with pytest.raises(ValueError):
            CreateDimensionArgs(
                app_id="test-app-id",
                title="Test",
                field_def="Test",
                grouping="X",
            )


class TestCreateSheetArgs:
    """Tests for CreateSheetArgs validation"""

    def test_valid_args(self):
        """Test valid sheet creation arguments"""
        args = CreateSheetArgs(
            app_id="test-app-id",
            title="Dashboard",
            description="Main dashboard",
        )
        assert args.title == "Dashboard"

    def test_minimal_args(self):
        """Test minimal required arguments"""
        args = CreateSheetArgs(
            app_id="test-app-id",
            title="Dashboard",
        )
        assert args.description == ""


class TestCreateObjectArgs:
    """Tests for CreateObjectArgs validation"""

    def test_valid_args(self):
        """Test valid object creation arguments"""
        args = CreateObjectArgs(
            app_id="test-app-id",
            object_type="barchart",
            title="Sales by Region",
            properties={"qHyperCubeDef": {}},
        )
        assert args.object_type == "barchart"
        assert args.properties is not None

    def test_minimal_args(self):
        """Test minimal required arguments"""
        args = CreateObjectArgs(
            app_id="test-app-id",
            object_type="table",
            title="Data Table",
        )
        assert args.properties is None


class TestVisualizationDefinitionArgs:
    """Tests for structured dimension and measure definitions."""

    def test_dimension_allows_field(self):
        dimension = PlotDimension(field="Region", label="Region")
        assert dimension.field == "Region"

    def test_dimension_requires_field_or_library_id(self):
        with pytest.raises(ValueError):
            PlotDimension()

    def test_measure_allows_expression(self):
        measure = PlotMeasure(expression="Sum(Sales)", label="Sales")
        assert measure.expression == "Sum(Sales)"

    def test_measure_requires_expression_or_library_id(self):
        with pytest.raises(ValueError):
            PlotMeasure()


class TestVisualizationBuilderArgs:
    """Tests for dedicated visualization builder argument models."""

    def test_create_bar_chart_args_valid(self):
        args = CreateBarChartArgs(
            app_id="test-app-id",
            title="Sales by Region",
            dimensions=[PlotDimension(field="Region")],
            measures=[PlotMeasure(expression="Sum(Sales)")],
            orientation="horizontal",
            stacked=True,
        )
        assert args.orientation == "horizontal"
        assert args.stacked is True

    def test_create_line_chart_args_valid(self):
        args = CreateLineChartArgs(
            app_id="test-app-id",
            title="Sales Trend",
            dimensions=[PlotDimension(field="OrderDate")],
            measures=[PlotMeasure(expression="Sum(Sales)")],
            show_markers=False,
        )
        assert args.show_markers is False

    def test_create_kpi_args_valid(self):
        args = CreateKpiArgs(
            app_id="test-app-id",
            title="Total Sales",
            measures=[PlotMeasure(expression="Sum(Sales)")],
            subtitle="Current period",
        )
        assert args.subtitle == "Current period"

    def test_create_table_requires_content(self):
        with pytest.raises(ValueError):
            CreateTableArgs(app_id="test-app-id", title="Empty Table")


class TestGetSheetLayoutArgs:
    """Tests for GetSheetLayoutArgs validation"""

    def test_valid_args(self):
        args = GetSheetLayoutArgs(
            app_id="test-app-id",
            sheet_id="sheet-123",
        )
        assert args.app_id == "test-app-id"
        assert args.sheet_id == "sheet-123"

    def test_empty_sheet_id_raises(self):
        with pytest.raises(ValueError):
            GetSheetLayoutArgs(app_id="test-app-id", sheet_id="")


class TestRepositionSheetObjectArgs:
    """Tests for RepositionSheetObjectArgs validation"""

    def test_valid_args(self):
        args = RepositionSheetObjectArgs(
            app_id="test-app-id",
            sheet_id="sheet-123",
            object_id="chart-123",
            column=2,
            row=3,
            colspan=8,
            rowspan=4,
        )
        assert args.column == 2
        assert args.row == 3
        assert args.colspan == 8
        assert args.rowspan == 4

    def test_negative_column_raises(self):
        with pytest.raises(ValueError):
            RepositionSheetObjectArgs(
                app_id="test-app-id",
                sheet_id="sheet-123",
                object_id="chart-123",
                column=-1,
                row=0,
            )

    def test_zero_colspan_raises(self):
        with pytest.raises(ValueError):
            RepositionSheetObjectArgs(
                app_id="test-app-id",
                sheet_id="sheet-123",
                object_id="chart-123",
                column=0,
                row=0,
                colspan=0,
            )


@pytest.mark.asyncio
async def test_get_sheet_layout_calls_client():
    """Tool wrapper should call the matching client method and return app context."""
    with patch("src.tools.QlikClient") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.connect.return_value = True
        mock_client.get_sheet_layout.return_value = {
            "success": True,
            "sheet_id": "sheet-123",
            "objects": [{"object_id": "chart-123"}],
            "object_count": 1,
        }
        mock_client_cls.return_value = mock_client

        result = await get_sheet_layout(app_id="app-123", sheet_id="sheet-123")

        mock_client.connect.assert_called_once_with("app-123")
        mock_client.get_sheet_layout.assert_called_once_with(sheet_id="sheet-123")
        mock_client.disconnect.assert_called_once()
        assert result["success"] is True
        assert result["app_id"] == "app-123"
        assert result["sheet_id"] == "sheet-123"
        assert result["object_count"] == 1
        assert "timestamp" in result


@pytest.mark.asyncio
async def test_reposition_sheet_object_calls_client():
    """Tool wrapper should pass through reposition coordinates."""
    with patch("src.tools.QlikClient") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.connect.return_value = True
        mock_client.reposition_sheet_object.return_value = {
            "success": True,
            "sheet_id": "sheet-123",
            "object_id": "chart-123",
            "col": 4,
            "row": 1,
            "colspan": 10,
            "rowspan": 5,
        }
        mock_client_cls.return_value = mock_client

        result = await reposition_sheet_object(
            app_id="app-123",
            sheet_id="sheet-123",
            object_id="chart-123",
            column=4,
            row=1,
            colspan=10,
            rowspan=5,
        )

        mock_client.connect.assert_called_once_with("app-123")
        mock_client.reposition_sheet_object.assert_called_once_with(
            sheet_id="sheet-123",
            object_id="chart-123",
            col=4,
            row=1,
            colspan=10,
            rowspan=5,
        )
        mock_client.disconnect.assert_called_once()
        assert result["success"] is True
        assert result["app_id"] == "app-123"
        assert result["object_id"] == "chart-123"
        assert result["col"] == 4
        assert result["row"] == 1
        assert "timestamp" in result


@pytest.mark.asyncio
async def test_create_bar_chart_calls_client():
    """Bar chart wrapper should build the expected hypercube and call create_object."""
    with patch("src.tools.QlikClient") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.connect.return_value = True
        mock_client.create_object.return_value = {
            "success": True,
            "object_id": "chart-123",
            "object_type": "barchart",
            "title": "Sales by Region",
        }
        mock_client_cls.return_value = mock_client
        mock_client_cls.build_bar_chart_properties.side_effect = QlikClient.build_bar_chart_properties

        result = await create_bar_chart(
            app_id="app-123",
            title="Sales by Region",
            dimensions=[PlotDimension(field="Region", label="Region")],
            measures=[PlotMeasure(expression="Sum(Sales)", label="Sales")],
            orientation="horizontal",
            stacked=True,
            show_legend=False,
        )

        mock_client.create_object.assert_called_once_with(
            object_type="barchart",
            title="Sales by Region",
            properties={
                "qHyperCubeDef": {
                    "qDimensions": [{
                        "qLabel": "Region",
                        "qNullSuppression": False,
                        "qDef": {"qFieldDefs": ["Region"], "qFieldLabels": ["Region"]},
                    }],
                    "qMeasures": [{"qLabel": "Sales", "qDef": "Sum(Sales)"}],
                    "qInitialDataFetch": [{"qTop": 0, "qLeft": 0, "qHeight": 100, "qWidth": 2}],
                    "qSuppressZero": False,
                    "qSuppressMissing": False,
                    "qInterColumnSortOrder": [0, 1],
                },
                "orientation": "horizontal",
                "barGrouping": "stacked",
                "legend": {"show": False},
            },
        )
        assert result["success"] is True
        assert result["app_id"] == "app-123"


@pytest.mark.asyncio
async def test_create_line_chart_calls_client():
    with patch("src.tools.QlikClient") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.connect.return_value = True
        mock_client.create_object.return_value = {
            "success": True,
            "object_id": "line-123",
            "object_type": "linechart",
            "title": "Sales Trend",
        }
        mock_client_cls.return_value = mock_client
        mock_client_cls.build_line_chart_properties.side_effect = QlikClient.build_line_chart_properties

        result = await create_line_chart(
            app_id="app-123",
            title="Sales Trend",
            dimensions=[PlotDimension(field="OrderDate")],
            measures=[PlotMeasure(expression="Sum(Sales)")],
            show_markers=False,
        )

        mock_client.create_object.assert_called_once_with(
            object_type="linechart",
            title="Sales Trend",
            properties={
                "qHyperCubeDef": {
                    "qDimensions": [{
                        "qLabel": "OrderDate",
                        "qNullSuppression": False,
                        "qDef": {"qFieldDefs": ["OrderDate"], "qFieldLabels": ["OrderDate"]},
                    }],
                    "qMeasures": [{"qLabel": "Sum(Sales)", "qDef": "Sum(Sales)"}],
                    "qInitialDataFetch": [{"qTop": 0, "qLeft": 0, "qHeight": 100, "qWidth": 2}],
                    "qSuppressZero": False,
                    "qSuppressMissing": False,
                    "qInterColumnSortOrder": [0, 1],
                },
                "dataPoint": {"show": False},
                "legend": {"show": True},
            },
        )
        assert result["success"] is True


@pytest.mark.asyncio
async def test_create_kpi_calls_client():
    with patch("src.tools.QlikClient") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.connect.return_value = True
        mock_client.create_object.return_value = {
            "success": True,
            "object_id": "kpi-123",
            "object_type": "kpi",
            "title": "Total Sales",
        }
        mock_client_cls.return_value = mock_client
        mock_client_cls.build_kpi_properties.side_effect = QlikClient.build_kpi_properties

        result = await create_kpi(
            app_id="app-123",
            title="Total Sales",
            measures=[PlotMeasure(expression="Sum(Sales)", label="Sales")],
            subtitle="Current period",
        )

        mock_client.create_object.assert_called_once_with(
            object_type="kpi",
            title="Total Sales",
            properties={
                "qHyperCubeDef": {
                    "qDimensions": [],
                    "qMeasures": [{"qLabel": "Sales", "qDef": "Sum(Sales)"}],
                    "qInitialDataFetch": [{"qTop": 0, "qLeft": 0, "qHeight": 1, "qWidth": 1}],
                    "qSuppressZero": False,
                    "qSuppressMissing": False,
                    "qInterColumnSortOrder": [0],
                },
                "subtitle": "Current period",
            },
        )
        assert result["success"] is True


@pytest.mark.asyncio
async def test_create_table_calls_client():
    with patch("src.tools.QlikClient") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.connect.return_value = True
        mock_client.create_object.return_value = {
            "success": True,
            "object_id": "table-123",
            "object_type": "table",
            "title": "Sales Table",
        }
        mock_client_cls.return_value = mock_client
        mock_client_cls.build_table_properties.side_effect = QlikClient.build_table_properties

        result = await create_table(
            app_id="app-123",
            title="Sales Table",
            dimensions=[PlotDimension(field="Region")],
            measures=[PlotMeasure(expression="Sum(Sales)")],
        )

        mock_client.create_object.assert_called_once_with(
            object_type="table",
            title="Sales Table",
            properties={
                "qHyperCubeDef": {
                    "qDimensions": [{
                        "qLabel": "Region",
                        "qNullSuppression": False,
                        "qDef": {"qFieldDefs": ["Region"], "qFieldLabels": ["Region"]},
                    }],
                    "qMeasures": [{"qLabel": "Sum(Sales)", "qDef": "Sum(Sales)"}],
                    "qInitialDataFetch": [{"qTop": 0, "qLeft": 0, "qHeight": 100, "qWidth": 2}],
                    "qSuppressZero": False,
                    "qSuppressMissing": False,
                    "qInterColumnSortOrder": [0, 1],
                },
            },
        )
        assert result["success"] is True


def test_build_hypercube_def_structures_payload():
    """Client helper should create consistent qHyperCubeDef payloads."""
    hypercube = QlikClient.build_hypercube_def(
        dimensions=[{"field": "Region", "label": "Region"}],
        measures=[{"expression": "Sum(Sales)", "label": "Sales", "number_format": "#,##0"}],
        initial_rows=50,
    )

    assert hypercube["qInterColumnSortOrder"] == [0, 1]
    assert hypercube["qInitialDataFetch"][0]["qHeight"] == 50
    assert hypercube["qDimensions"][0]["qDef"]["qFieldDefs"] == ["Region"]
    assert hypercube["qMeasures"][0]["qDef"] == "Sum(Sales)"
    assert hypercube["qMeasures"][0]["qNumFormat"]["qFmt"] == "#,##0"


def test_create_visualization_sends_structured_hypercube():
    """Typed client helper should feed create_object with the expected payload."""
    client = QlikClient()
    client.app_handle = 99

    with patch.object(client, "_send_request", return_value={"qReturn": {"qGenericId": "viz-123"}}) as send_request:
        result = client.create_bar_chart(
            title="Sales by Region",
            dimensions=[{"field": "Region"}],
            measures=[{"expression": "Sum(Sales)"}],
            orientation="horizontal",
            stacked=True,
            show_legend=False,
        )

    send_request.assert_called_once()
    payload = send_request.call_args.args[2]["qProp"]
    assert payload["qInfo"]["qType"] == "barchart"
    assert payload["qHyperCubeDef"]["qDimensions"][0]["qDef"]["qFieldDefs"] == ["Region"]
    assert payload["qHyperCubeDef"]["qMeasures"][0]["qDef"] == "Sum(Sales)"
    assert payload["orientation"] == "horizontal"
    assert payload["barGrouping"] == "stacked"
    assert payload["legend"] == {"show": False}
    assert result["success"] is True


@pytest.mark.asyncio
async def test_handle_create_bar_chart_calls_tool():
    with patch("src.server.create_bar_chart") as create_bar_chart_mock:
        create_bar_chart_mock.return_value = {"success": True, "object_id": "chart-123"}

        result = await handle_create_bar_chart(
            CreateBarChartArgs(
                app_id="app-123",
                title="Sales by Region",
                dimensions=[PlotDimension(field="Region")],
                measures=[PlotMeasure(expression="Sum(Sales)")],
            )
        )

        create_bar_chart_mock.assert_called_once()
        assert result["success"] is True


@pytest.mark.asyncio
async def test_handle_create_line_chart_calls_tool():
    with patch("src.server.create_line_chart") as create_line_chart_mock:
        create_line_chart_mock.return_value = {"success": True, "object_id": "line-123"}

        result = await handle_create_line_chart(
            CreateLineChartArgs(
                app_id="app-123",
                title="Sales Trend",
                dimensions=[PlotDimension(field="OrderDate")],
                measures=[PlotMeasure(expression="Sum(Sales)")],
            )
        )

        create_line_chart_mock.assert_called_once()
        assert result["success"] is True


@pytest.mark.asyncio
async def test_handle_create_kpi_calls_tool():
    with patch("src.server.create_kpi") as create_kpi_mock:
        create_kpi_mock.return_value = {"success": True, "object_id": "kpi-123"}

        result = await handle_create_kpi(
            CreateKpiArgs(
                app_id="app-123",
                title="Total Sales",
                measures=[PlotMeasure(expression="Sum(Sales)")],
            )
        )

        create_kpi_mock.assert_called_once()
        assert result["success"] is True


@pytest.mark.asyncio
async def test_handle_create_table_calls_tool():
    with patch("src.server.create_table") as create_table_mock:
        create_table_mock.return_value = {"success": True, "object_id": "table-123"}

        result = await handle_create_table(
            CreateTableArgs(
                app_id="app-123",
                title="Sales Table",
                dimensions=[PlotDimension(field="Region")],
            )
        )

        create_table_mock.assert_called_once()
        assert result["success"] is True


class TestCreateMeasureIntegration:
    """Integration tests for create_measure function - requires live Qlik server"""

    @pytest.mark.skip(reason="Requires live Qlik server connection")
    def test_create_measure_success(self):
        pass

    @pytest.mark.skip(reason="Requires live Qlik server connection")
    def test_create_measure_connection_failure(self):
        pass


class TestCreateVariableIntegration:
    """Integration tests for create_variable function - requires live Qlik server"""

    @pytest.mark.skip(reason="Requires live Qlik server connection")
    def test_create_variable_success(self):
        pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

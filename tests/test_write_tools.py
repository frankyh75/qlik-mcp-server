"""Unit tests for WRITE tools (create_measure, create_variable, etc.)"""

import pytest
from unittest.mock import MagicMock, patch
from src.tools import (
    CreateMeasureArgs,
    CreateVariableArgs,
    CreateDimensionArgs,
    CreateSheetArgs,
    CreateObjectArgs,
    GetSheetLayoutArgs,
    RepositionSheetObjectArgs,
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

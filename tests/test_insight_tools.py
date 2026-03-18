"""Tests for the get_app_details insight tool."""

from unittest.mock import patch

import pytest

from src.qlik_client import QlikClient
from src.server import handle_get_app_details
from src.tools import GetAppDetailsArgs, get_app_details


def test_get_app_details_args_strip_app_id():
    args = GetAppDetailsArgs(app_id="  app-123  ")

    assert args.app_id == "app-123"
    assert args.include_fields is True
    assert args.include_master_items is True
    assert args.include_sheet_overview is True
    assert args.resolve_master_items is True


def test_get_app_metadata_normalizes_repository_payload():
    client = QlikClient()

    with patch.object(client, "_repository_get_json", return_value={
        "id": "app-123",
        "name": "Sales Dashboard",
        "description": "Quarterly sales app",
        "published": True,
        "publishTime": "2026-03-17T10:00:00Z",
        "lastReloadTime": "2026-03-17T09:45:00Z",
        "createdDate": "2026-01-01T08:00:00Z",
        "modifiedDate": "2026-03-17T10:30:00Z",
        "fileSize": 4096,
        "stream": {"id": "stream-1", "name": "Finance"},
        "owner": {
            "id": "owner-1",
            "userId": "sa_engine",
            "userDirectory": "INTERNAL",
            "name": "Service Account",
        },
        "tags": [{"name": "sales"}, {"name": "executive"}],
    }):
        metadata = client.get_app_metadata("app-123")

    assert metadata == {
        "id": "app-123",
        "name": "Sales Dashboard",
        "description": "Quarterly sales app",
        "published": True,
        "publish_time": "2026-03-17T10:00:00Z",
        "last_reload_time": "2026-03-17T09:45:00Z",
        "created_at": "2026-01-01T08:00:00Z",
        "modified_at": "2026-03-17T10:30:00Z",
        "owner": {
            "id": "owner-1",
            "user_id": "sa_engine",
            "user_directory": "INTERNAL",
            "name": "Service Account",
        },
        "stream": {"id": "stream-1", "name": "Finance"},
        "tags": ["sales", "executive"],
        "file_size": 4096,
    }


def test_get_sheet_overviews_compacts_objects():
    client = QlikClient()

    with patch.object(client, "get_sheets", return_value={
        "sheets": [
            {"sheet_id": "sheet-1", "title": "Overview", "description": "Main", "rank": 1},
        ]
    }), patch.object(client, "get_sheet_objects", return_value={
        "objects": [
            {
                "object_id": "obj-1",
                "object_type": "barchart",
                "title": "Sales by Region",
                "subtitle": "FY26",
                "measures": [{"id": "m1"}],
                "dimensions": [{"id": "d1"}],
            },
            {
                "object_id": "obj-2",
                "object_type": "container",
                "title": "Tabbed KPIs",
                "is_container": True,
                "embedded_object_count": 3,
                "measures": [],
                "dimensions": [],
            },
        ]
    }) as get_sheet_objects:
        overview = client.get_sheet_overviews(resolve_master_items=False)

    get_sheet_objects.assert_called_once_with(
        sheet_id="sheet-1",
        include_properties=False,
        include_layout=True,
        include_data_definition=True,
        resolve_master_items=False,
    )
    assert overview["sheet_count"] == 1
    assert overview["object_count"] == 2
    assert overview["sheets"][0]["object_types"] == {"barchart": 1, "container": 1}
    assert overview["sheets"][0]["objects"][0]["measure_count"] == 1
    assert overview["sheets"][0]["objects"][1]["embedded_object_count"] == 3


@pytest.mark.asyncio
async def test_get_app_details_aggregates_repository_and_engine_data():
    with patch("src.tools.QlikClient") as mock_client_cls:
        client = mock_client_cls.return_value
        client.get_app_metadata.return_value = {
            "id": "app-123",
            "name": "Sales Dashboard",
            "description": "Quarterly sales app",
            "published": True,
        }
        client.connect.return_value = True
        client.get_fields.return_value = {
            "field_count": 2,
            "table_count": 1,
            "tables": ["Sales"],
            "fields": [
                {"name": "Region", "cardinal": 4, "is_numeric": False},
                {"name": "Sales", "cardinal": 100, "is_numeric": True},
            ],
        }
        client._summarize_field.side_effect = QlikClient._summarize_field
        client.get_measures.return_value = {
            "count": 1,
            "measures": [{
                "id": "measure-1",
                "title": "Total Sales",
                "label": "Total Sales",
                "expression": "Sum(Sales)",
                "tags": ["kpi"],
            }],
        }
        client._summarize_measure.side_effect = QlikClient._summarize_measure
        client.get_dimensions.return_value = {
            "dimension_count": 1,
            "dimensions": [{
                "dimension_id": "dimension-1",
                "title": "Region",
                "grouping": "N",
                "info": [{"qFieldDefs": ["Region"], "qLabel": "Region"}],
                "tags": ["geo"],
            }],
        }
        client._summarize_dimension.side_effect = QlikClient._summarize_dimension
        client.get_sheet_overviews.return_value = {
            "sheet_count": 1,
            "object_count": 2,
            "sheets": [{"sheet_id": "sheet-1", "title": "Overview", "object_count": 2, "objects": []}],
        }

        result = await get_app_details(app_id="app-123")

    assert result["app_metadata"]["name"] == "Sales Dashboard"
    assert result["data_model"]["field_count"] == 2
    assert result["data_model"]["fields"][1]["name"] == "Sales"
    assert result["master_items"]["measure_count"] == 1
    assert result["master_items"]["dimensions"][0]["field_definitions"] == ["Region"]
    assert result["sheets"]["sheet_count"] == 1
    client.get_sheet_overviews.assert_called_once_with(resolve_master_items=True)
    client.disconnect.assert_called_once()


@pytest.mark.asyncio
async def test_get_app_details_respects_section_flags():
    with patch("src.tools.QlikClient") as mock_client_cls:
        client = mock_client_cls.return_value
        client.get_app_metadata.return_value = {"id": "app-123", "name": "Sales Dashboard"}
        client.connect.return_value = True

        result = await get_app_details(
            app_id="app-123",
            include_fields=False,
            include_master_items=False,
            include_sheet_overview=False,
            resolve_master_items=False,
        )

    assert "data_model" not in result
    assert "master_items" not in result
    assert "sheets" not in result
    client.get_fields.assert_not_called()
    client.get_measures.assert_not_called()
    client.get_dimensions.assert_not_called()
    client.get_sheet_overviews.assert_not_called()
    client.disconnect.assert_called_once()


@pytest.mark.asyncio
async def test_handle_get_app_details_calls_tool():
    with patch("src.server.get_app_details") as get_app_details_mock:
        get_app_details_mock.return_value = {"app_id": "app-123", "app_metadata": {"name": "Sales Dashboard"}}

        result = await handle_get_app_details(
            GetAppDetailsArgs(
                app_id="app-123",
                include_fields=False,
                include_master_items=True,
                include_sheet_overview=False,
                resolve_master_items=False,
            )
        )

    get_app_details_mock.assert_called_once_with(
        app_id="app-123",
        include_fields=False,
        include_master_items=True,
        include_sheet_overview=False,
        resolve_master_items=False,
    )
    assert result["app_metadata"]["name"] == "Sales Dashboard"

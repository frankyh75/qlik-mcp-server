"""Tests for the insight tools."""

from unittest.mock import patch

import pytest

from src.qlik_client import QlikClient
from src.server import handle_get_app_details, handle_get_field_statistics, handle_get_hypercube_summary
from src.tools import (
    GetAppDetailsArgs,
    GetFieldStatisticsArgs,
    GetHypercubeSummaryArgs,
    get_app_details,
    get_field_statistics,
    get_hypercube_summary,
)


def test_get_app_details_args_strip_app_id():
    args = GetAppDetailsArgs(app_id="  app-123  ")

    assert args.app_id == "app-123"
    assert args.include_fields is True
    assert args.include_master_items is True
    assert args.include_sheet_overview is True
    assert args.resolve_master_items is True


def test_get_field_statistics_args_strip_app_id():
    args = GetFieldStatisticsArgs(app_id="  app-123  ")

    assert args.app_id == "app-123"
    assert args.show_system is True
    assert args.show_hidden is True
    assert args.show_derived_fields is True
    assert args.show_semantic is True
    assert args.show_implicit is True


def test_get_field_statistics_args_reject_whitespace_app_id():
    with pytest.raises(ValueError, match="app_id cannot be empty or whitespace"):
        GetFieldStatisticsArgs(app_id="   ")


def test_get_hypercube_summary_args_strip_ids():
    args = GetHypercubeSummaryArgs(app_id="  app-123  ", object_id="  obj-1  ")

    assert args.app_id == "app-123"
    assert args.object_id == "obj-1"
    assert args.max_data_rows == 1000


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


def test_get_hypercube_summary_helper_compacts_metadata_and_density():
    client = QlikClient()
    client.ws = object()
    client.app_handle = 1

    with patch.object(client, "_send_request", side_effect=[
        {"qReturn": {"qHandle": 99}},
        {
            "qLayout": {
                "qInfo": {"qType": "table"},
                "title": "Sales Table",
                "subtitle": "FY26",
                "qHyperCube": {
                    "qSize": {"qcy": 3, "qcx": 2},
                    "qMode": "S",
                    "qNoOfLeftDims": 1,
                    "qDimensionInfo": [{"qFallbackTitle": "Region", "qCardinal": 3}],
                    "qMeasureInfo": [{"qFallbackTitle": "Sales", "qMin": 10, "qMax": 30}],
                    "qDataPages": [{
                        "qMatrix": [
                            [{"qText": "North"}, {"qText": "10"}],
                            [{"qText": "South"}, {"qText": ""}],
                            [{"qText": "West"}, {"qIsNull": True}],
                        ],
                    }],
                },
            },
        },
    ]) as send_request:
        result = client.get_hypercube_summary("obj-1", max_data_rows=50)

    assert send_request.call_count == 2
    assert result["object_type"] == "table"
    assert result["hypercube"]["size"] == {"rows": 3, "columns": 2, "cells": 6}
    assert result["hypercube"]["dimension_count"] == 1
    assert result["hypercube"]["measure_count"] == 1
    assert result["statistics"] == {
        "total_rows": 3,
        "total_columns": 2,
        "total_cells": 6,
        "sampled": False,
        "sampled_row_count": 3,
        "sampled_cell_count": 6,
        "populated_cells": 4,
        "empty_cells": 2,
        "density": 0.6667,
    }


def test_get_field_statistics_helper_compacts_field_payload():
    client = QlikClient()

    with patch.object(client, "get_fields", return_value={
        "field_count": 2,
        "table_count": 3,
        "tables": ["Calendar", "Customers", "Sales"],
        "fields": [
            {
                "name": "Region",
                "cardinal": 4,
                "is_system": False,
                "is_hidden": False,
                "source_tables": ["Customers", "Sales"],
            },
            {
                "name": "$Table",
                "cardinal": 3,
                "is_system": True,
                "is_hidden": True,
                "source_tables": ["Sales"],
            },
        ],
    }) as get_fields:
        result = client.get_field_statistics(
            show_system=False,
            show_hidden=False,
            show_derived_fields=True,
            show_semantic=True,
            show_implicit=False,
        )

    get_fields.assert_called_once_with(
        show_system=False,
        show_hidden=False,
        show_derived_fields=True,
        show_semantic=True,
        show_src_tables=True,
        show_implicit=False,
    )
    assert result == {
        "field_count": 2,
        "table_count": 3,
        "tables": ["Calendar", "Customers", "Sales"],
        "fields": [
            {
                "name": "Region",
                "cardinal": 4,
                "table_count": 2,
                "is_system": False,
                "is_hidden": False,
            },
            {
                "name": "$Table",
                "cardinal": 3,
                "table_count": 1,
                "is_system": True,
                "is_hidden": True,
            },
        ],
    }


def test_get_field_statistics_helper_normalizes_source_tables():
    client = QlikClient()

    with patch.object(client, "get_fields", return_value={
        "field_count": 3,
        "table_count": 9,
        "tables": [" Sales ", "", "Sales", None, "Customers"],
        "fields": [
            {
                "name": "Region",
                "cardinal": 4,
                "is_system": False,
                "is_hidden": False,
                "source_tables": "Sales",
            },
            {
                "name": "CustomerID",
                "cardinal": 100,
                "is_system": False,
                "is_hidden": False,
                "source_tables": ["Customers", " Sales ", "", None, "Customers"],
            },
            {
                "name": "OrphanField",
                "cardinal": 0,
                "is_system": False,
                "is_hidden": False,
                "source_tables": None,
            },
        ],
    }):
        result = client.get_field_statistics()

    assert result["tables"] == ["Sales", "Customers"]
    assert result["fields"] == [
        {
            "name": "Region",
            "cardinal": 4,
            "table_count": 1,
            "is_system": False,
            "is_hidden": False,
        },
        {
            "name": "CustomerID",
            "cardinal": 100,
            "table_count": 2,
            "is_system": False,
            "is_hidden": False,
        },
        {
            "name": "OrphanField",
            "cardinal": 0,
            "table_count": 0,
            "is_system": False,
            "is_hidden": False,
        },
    ]


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
async def test_get_field_statistics_returns_compact_summary():
    with patch("src.tools.QlikClient") as mock_client_cls:
        client = mock_client_cls.return_value
        client.connect.return_value = True
        client.get_field_statistics.return_value = {
            "field_count": 2,
            "table_count": 2,
            "tables": ["Customers", "Sales"],
            "fields": [
                {
                    "name": "Region",
                    "cardinal": 4,
                    "table_count": 2,
                    "is_system": False,
                    "is_hidden": False,
                },
                {
                    "name": "$Table",
                    "cardinal": 2,
                    "table_count": 1,
                    "is_system": True,
                    "is_hidden": True,
                },
            ],
        }

        result = await get_field_statistics(
            app_id="app-123",
            show_system=False,
            show_hidden=False,
            show_derived_fields=True,
            show_semantic=True,
            show_implicit=False,
        )

    client.get_field_statistics.assert_called_once_with(
        show_system=False,
        show_hidden=False,
        show_derived_fields=True,
        show_semantic=True,
        show_implicit=False,
    )
    assert result["app_id"] == "app-123"
    assert result["field_count"] == 2
    assert result["fields"][0]["table_count"] == 2
    assert result["options"]["show_implicit"] is False
    client.disconnect.assert_called_once()


@pytest.mark.asyncio
async def test_get_field_statistics_returns_connect_error():
    with patch("src.tools.QlikClient") as mock_client_cls:
        client = mock_client_cls.return_value
        client.connect.return_value = False

        result = await get_field_statistics(app_id="app-123")

    assert result["error"] == "Failed to connect to Qlik Engine"
    assert result["app_id"] == "app-123"
    assert "timestamp" in result
    client.get_field_statistics.assert_not_called()
    client.disconnect.assert_called_once()


@pytest.mark.asyncio
async def test_get_field_statistics_returns_exception_error():
    with patch("src.tools.QlikClient") as mock_client_cls:
        client = mock_client_cls.return_value
        client.connect.return_value = True
        client.get_field_statistics.side_effect = RuntimeError("boom")

        result = await get_field_statistics(app_id="app-123")

    assert result["error"] == "boom"
    assert result["app_id"] == "app-123"
    assert "timestamp" in result
    client.disconnect.assert_called_once()


@pytest.mark.asyncio
async def test_get_hypercube_summary_returns_compact_summary():
    with patch("src.tools.QlikClient") as mock_client_cls:
        client = mock_client_cls.return_value
        client.connect.return_value = True
        client.get_hypercube_summary.return_value = {
            "object_id": "obj-1",
            "object_type": "table",
            "title": "Sales Table",
            "hypercube": {
                "size": {"rows": 20, "columns": 4, "cells": 80},
                "dimension_count": 2,
                "measure_count": 2,
            },
            "statistics": {
                "total_rows": 20,
                "total_columns": 4,
                "total_cells": 80,
                "sampled": False,
                "sampled_row_count": 20,
                "sampled_cell_count": 80,
                "populated_cells": 72,
                "empty_cells": 8,
                "density": 0.9,
            },
        }

        result = await get_hypercube_summary(
            app_id="app-123",
            object_id="obj-1",
            max_data_rows=250,
        )

    client.get_hypercube_summary.assert_called_once_with(object_id="obj-1", max_data_rows=250)
    assert result["app_id"] == "app-123"
    assert result["object_id"] == "obj-1"
    assert result["statistics"]["density"] == 0.9
    assert result["options"]["max_data_rows"] == 250
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


@pytest.mark.asyncio
async def test_handle_get_field_statistics_calls_tool():
    with patch("src.server.get_field_statistics") as get_field_statistics_mock:
        get_field_statistics_mock.return_value = {
            "app_id": "app-123",
            "field_count": 2,
            "fields": [{"name": "Region", "cardinal": 4, "table_count": 1}],
        }

        result = await handle_get_field_statistics(
            GetFieldStatisticsArgs(
                app_id="app-123",
                show_system=False,
                show_hidden=False,
                show_derived_fields=True,
                show_semantic=True,
                show_implicit=False,
            )
        )

    get_field_statistics_mock.assert_called_once_with(
        app_id="app-123",
        show_system=False,
        show_hidden=False,
        show_derived_fields=True,
        show_semantic=True,
        show_implicit=False,
    )
    assert result["field_count"] == 2


@pytest.mark.asyncio
async def test_handle_get_field_statistics_wraps_unexpected_error():
    with patch("src.server.get_field_statistics", side_effect=RuntimeError("unexpected")):
        result = await handle_get_field_statistics(GetFieldStatisticsArgs(app_id="app-123"))

    assert result == {
        "error": "Unexpected error: unexpected",
        "app_id": "app-123",
    }


@pytest.mark.asyncio
async def test_handle_get_hypercube_summary_calls_tool():
    with patch("src.server.get_hypercube_summary") as get_hypercube_summary_mock:
        get_hypercube_summary_mock.return_value = {
            "app_id": "app-123",
            "object_id": "obj-1",
            "statistics": {"total_rows": 20, "total_columns": 4, "density": 0.9},
        }

        result = await handle_get_hypercube_summary(
            GetHypercubeSummaryArgs(
                app_id="app-123",
                object_id="obj-1",
                max_data_rows=250,
            )
        )

    get_hypercube_summary_mock.assert_called_once_with(
        app_id="app-123",
        object_id="obj-1",
        max_data_rows=250,
    )
    assert result["statistics"]["density"] == 0.9

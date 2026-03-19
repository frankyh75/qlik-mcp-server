"""Simplified Qlik WebSocket client for measure retrieval"""

import json
import os
import ssl
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import websocket
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class QlikClient:
    """Comprehensive Qlik Engine API client for accessing all Qlik Sense application objects"""

    def __init__(self):
        """Initialize client with configuration from environment"""
        self.server_url = os.getenv("QLIK_SERVER_URL")
        self.server_port = os.getenv("QLIK_SERVER_PORT", "4747")
        self.repository_port = os.getenv("QLIK_REPOSITORY_PORT", "4242")
        self.user_directory = os.getenv("QLIK_USER_DIRECTORY", "INTERNAL")
        self.user_id = os.getenv("QLIK_USER_ID", "sa_engine")

        # Certificate paths
        self.cert_root = os.getenv("QLIK_CERT_ROOT", "certs/root.pem")
        self.cert_client = os.getenv("QLIK_CERT_CLIENT", "certs/client.pem")
        self.cert_key = os.getenv("QLIK_CERT_KEY", "certs/client_key.pem")

        # Timeout settings
        self.timeout = int(os.getenv("WEBSOCKET_TIMEOUT", "30"))
        self.recv_timeout = int(os.getenv("WEBSOCKET_RECV_TIMEOUT", "60"))

        # Connection state
        self.ws: websocket.WebSocket | None = None
        self.request_id = 0
        self.app_handle: int | None = None

    def _normalize_server_host(self) -> str:
        """Return the Qlik host without protocol or trailing slashes."""
        host = (self.server_url or "").strip()
        if host.startswith("https://"):
            host = host[len("https://"):]
        elif host.startswith("http://"):
            host = host[len("http://"):]
        return host.rstrip("/")

    def _get_auth_headers(self) -> dict[str, str]:
        """Build the common Qlik user header for Engine and Repository requests."""
        return {
            "X-Qlik-User": f"UserDirectory={self.user_directory}; UserId={self.user_id}",
        }

    def _build_ssl_context(self) -> ssl.SSLContext:
        """Build a certificate-authenticated SSL context."""
        context = ssl.create_default_context(cafile=self.cert_root)
        context.check_hostname = False
        context.load_cert_chain(certfile=self.cert_client, keyfile=self.cert_key)
        return context

    def _repository_get_json(
        self,
        path: str,
        query: dict[str, Any] | None = None,
    ) -> Any:
        """Execute a Repository API GET request and decode the JSON response."""
        host = self._normalize_server_host()
        if not host:
            raise ValueError("QLIK_SERVER_URL is not configured")

        query_string = f"?{urlencode(query)}" if query else ""
        url = f"https://{host}:{self.repository_port}{path}{query_string}"
        request = Request(url, headers=self._get_auth_headers(), method="GET")

        with urlopen(request, context=self._build_ssl_context(), timeout=self.timeout) as response:
            return json.loads(response.read().decode("utf-8"))

    def get_app_metadata(self, app_id: str) -> dict[str, Any]:
        """Fetch app metadata from the Qlik Repository API."""
        app = self._repository_get_json(f"/qrs/app/{app_id}")

        stream = app.get("stream") or {}
        owner = app.get("owner") or {}
        tags = [tag.get("name", "") for tag in app.get("tags", []) if isinstance(tag, dict)]

        return {
            "id": app.get("id", app_id),
            "name": app.get("name") or app.get("title") or "",
            "description": app.get("description") or "",
            "published": app.get("published", False),
            "publish_time": app.get("publishTime") or app.get("publishedDate"),
            "last_reload_time": app.get("lastReloadTime") or app.get("lastReloadTimeStamp"),
            "created_at": app.get("createdDate") or app.get("created"),
            "modified_at": app.get("modifiedDate") or app.get("modified"),
            "owner": {
                "id": owner.get("id"),
                "user_id": owner.get("userId"),
                "user_directory": owner.get("userDirectory"),
                "name": owner.get("name"),
            } if owner else None,
            "stream": {
                "id": stream.get("id"),
                "name": stream.get("name"),
            } if stream else None,
            "tags": [tag for tag in tags if tag],
            "file_size": app.get("fileSize"),
        }

    @staticmethod
    def _summarize_field(field: dict[str, Any]) -> dict[str, Any]:
        """Trim field payload to the metadata useful for app insights."""
        source_tables = QlikClient.normalize_source_tables(field.get("source_tables"))
        return {
            "name": field.get("name", ""),
            "cardinal": field.get("cardinal", 0),
            "is_system": field.get("is_system", False),
            "is_hidden": field.get("is_hidden", False),
            "is_semantic": field.get("is_semantic", False),
            "is_numeric": field.get("is_numeric", False),
            "source_tables": source_tables,
            "tags": field.get("tags", []),
        }

    @staticmethod
    def normalize_source_tables(source_tables: Any) -> list[str]:
        """Normalize table names to a unique, non-empty string list."""
        if isinstance(source_tables, str):
            raw_items = [source_tables]
        elif isinstance(source_tables, (list, tuple, set)):
            raw_items = list(source_tables)
        else:
            raw_items = []

        normalized = []
        seen = set()
        for table in raw_items:
            if not isinstance(table, str):
                continue
            table_name = table.strip()
            if not table_name or table_name in seen:
                continue
            seen.add(table_name)
            normalized.append(table_name)

        return normalized

    # backward-compatible alias
    _normalize_source_tables = normalize_source_tables

    @staticmethod
    def _summarize_field_statistics(field: dict[str, Any]) -> dict[str, Any]:
        """Trim field payload to the statistics useful for field-level insights."""
        source_tables = QlikClient.normalize_source_tables(field.get("source_tables"))
        return {
            "name": field.get("name", ""),
            "cardinal": field.get("cardinal", 0),
            "table_count": len(source_tables),
            "is_system": field.get("is_system", False),
            "is_hidden": field.get("is_hidden", False),
        }

    def get_field_statistics(
        self,
        show_system: bool = True,
        show_hidden: bool = True,
        show_derived_fields: bool = True,
        show_semantic: bool = True,
        show_implicit: bool = True,
    ) -> dict[str, Any]:
        """Return compact field-level statistics derived from the current app data model."""
        fields_result = self.get_fields(
            show_system=show_system,
            show_hidden=show_hidden,
            show_derived_fields=show_derived_fields,
            show_semantic=show_semantic,
            show_src_tables=True,
            show_implicit=show_implicit,
        )

        return {
            "fields": [
                self._summarize_field_statistics(field)
                for field in fields_result.get("fields", [])
            ],
            "field_count": fields_result.get("field_count", 0),
            "table_count": fields_result.get("table_count", 0),
            "tables": self.normalize_source_tables(fields_result.get("tables")),
        }

    @staticmethod
    def _cell_has_value(cell: dict[str, Any]) -> bool:
        """Return whether a hypercube cell should count as populated."""
        if not isinstance(cell, dict):
            return False
        if cell.get("qIsNull"):
            return False
        if cell.get("qText") not in (None, ""):
            return True
        if "qNum" in cell and cell.get("qNum") is not None:
            return True
        return False

    @classmethod
    def _compute_data_density(
        cls,
        data_pages: list[dict[str, Any]],
        total_columns: int,
    ) -> dict[str, Any]:
        """Summarize populated vs empty cells from hypercube data pages."""
        sampled_rows = 0
        sampled_cells = 0
        populated_cells = 0

        for page in data_pages:
            matrix = page.get("qMatrix", [])
            sampled_rows += len(matrix)
            for row in matrix:
                sampled_cells += total_columns
                populated_cells += sum(1 for cell in row[:total_columns] if cls._cell_has_value(cell))

        empty_cells = max(sampled_cells - populated_cells, 0)
        density = round(populated_cells / sampled_cells, 4) if sampled_cells else None

        return {
            "sampled_row_count": sampled_rows,
            "sampled_cell_count": sampled_cells,
            "populated_cells": populated_cells,
            "empty_cells": empty_cells,
            "density": density,
        }

    def get_hypercube_summary(self, object_id: str, max_data_rows: int = 1000) -> dict[str, Any]:
        """Return hypercube metadata and basic health statistics for an object."""
        if not self.ws or not self.app_handle:
            raise ConnectionError("Not connected to Qlik Engine")

        object_result = self._send_request("GetObject", self.app_handle, [object_id])
        if not object_result or "qReturn" not in object_result:
            raise ValueError(f"Failed to retrieve object: {object_id}")

        object_handle = object_result["qReturn"]["qHandle"]
        layout_result = self._send_request("GetLayout", object_handle)
        layout = layout_result.get("qLayout", layout_result) if layout_result else {}
        hypercube = layout.get("qHyperCube", {})

        if not hypercube:
            raise ValueError(f"Object does not expose qHyperCube metadata: {object_id}")

        size = hypercube.get("qSize", {})
        total_rows = size.get("qcy", 0)
        total_columns = size.get("qcx", 0)

        data_pages = hypercube.get("qDataPages", [])
        requested_rows = min(total_rows, max(max_data_rows, 0))
        if not data_pages and requested_rows and total_columns:
            data_result = self._send_request(
                "GetHyperCubeData",
                object_handle,
                ["/qHyperCubeDef", [{
                    "qTop": 0,
                    "qLeft": 0,
                    "qWidth": total_columns,
                    "qHeight": requested_rows,
                }]],
            )
            data_pages = data_result.get("qDataPages", [])

        density_stats = self._compute_data_density(data_pages, total_columns)
        total_cells = total_rows * total_columns

        return {
            "object_id": object_id,
            "object_type": layout.get("qInfo", {}).get("qType", ""),
            "title": layout.get("title", ""),
            "subtitle": layout.get("subtitle", ""),
            "hypercube": {
                "size": {
                    "rows": total_rows,
                    "columns": total_columns,
                    "cells": total_cells,
                },
                "mode": hypercube.get("qMode"),
                "no_of_left_dimensions": hypercube.get("qNoOfLeftDims", 0),
                "dimension_count": len(hypercube.get("qDimensionInfo", [])),
                "measure_count": len(hypercube.get("qMeasureInfo", [])),
                "dimensions": [
                    {
                        "title": item.get("qFallbackTitle", ""),
                        "cardinal": item.get("qCardinal"),
                    }
                    for item in hypercube.get("qDimensionInfo", [])
                ],
                "measures": [
                    {
                        "title": item.get("qFallbackTitle", ""),
                        "min": item.get("qMin"),
                        "max": item.get("qMax"),
                    }
                    for item in hypercube.get("qMeasureInfo", [])
                ],
            },
            "statistics": {
                "total_rows": total_rows,
                "total_columns": total_columns,
                "total_cells": total_cells,
                "sampled": density_stats["sampled_row_count"] < total_rows,
                **density_stats,
            },
        }

    @staticmethod
    def _summarize_measure(measure: dict[str, Any]) -> dict[str, Any]:
        """Trim master measure payload for overview responses."""
        return {
            "id": measure.get("id", ""),
            "title": measure.get("title", ""),
            "label": measure.get("label", ""),
            "description": measure.get("description", ""),
            "expression": measure.get("expression", ""),
            "tags": measure.get("tags", []),
        }

    @staticmethod
    def _summarize_dimension(dimension: dict[str, Any]) -> dict[str, Any]:
        """Trim master dimension payload for overview responses."""
        info = dimension.get("info", [])
        field_definitions = []
        labels = []
        for item in info:
            if not isinstance(item, dict):
                continue
            field_definitions.extend(item.get("qFieldDefs", []))
            label = item.get("qLabel")
            if label:
                labels.append(label)

        return {
            "id": dimension.get("dimension_id", ""),
            "title": dimension.get("title") or dimension.get("name", ""),
            "description": dimension.get("description", ""),
            "grouping": dimension.get("grouping", ""),
            "field_definitions": field_definitions,
            "labels": labels,
            "tags": dimension.get("tags", []),
        }

    @staticmethod
    def _summarize_sheet_object(obj: dict[str, Any]) -> dict[str, Any]:
        """Trim visualization payload to a compact overview."""
        return {
            "object_id": obj.get("object_id", ""),
            "object_type": obj.get("object_type", ""),
            "title": obj.get("title", ""),
            "subtitle": obj.get("subtitle", ""),
            "is_container": obj.get("is_container", False),
            "embedded_object_count": obj.get("embedded_object_count", 0),
            "measure_count": len(obj.get("measures", [])),
            "dimension_count": len(obj.get("dimensions", [])),
        }

    def get_sheet_overviews(self, resolve_master_items: bool = True) -> dict[str, Any]:
        """Return compact sheet and object summaries for the open app."""
        sheets_result = self.get_sheets(include_thumbnail=False, include_metadata=True)
        sheet_summaries = []
        total_objects = 0

        for sheet in sheets_result.get("sheets", []):
            objects_result = self.get_sheet_objects(
                sheet_id=sheet.get("sheet_id", ""),
                include_properties=False,
                include_layout=True,
                include_data_definition=True,
                resolve_master_items=resolve_master_items,
            )

            object_summaries = [
                self._summarize_sheet_object(obj)
                for obj in objects_result.get("objects", [])
            ]
            total_objects += len(object_summaries)

            object_types: dict[str, int] = {}
            for obj in object_summaries:
                object_type = obj.get("object_type") or "unknown"
                object_types[object_type] = object_types.get(object_type, 0) + 1

            sheet_summaries.append({
                "sheet_id": sheet.get("sheet_id", ""),
                "title": sheet.get("title", ""),
                "description": sheet.get("description", ""),
                "rank": sheet.get("rank", 0),
                "object_count": len(object_summaries),
                "object_types": object_types,
                "objects": object_summaries,
            })

        return {
            "sheet_count": len(sheet_summaries),
            "object_count": total_objects,
            "sheets": sheet_summaries,
        }

    def connect(self, app_id: str) -> bool:
        """Connect to Qlik Engine and open specified app"""
        try:
            # First try connecting to global context and using OpenDoc
            url = f"wss://{self.server_url}:{self.server_port}/app/"
            print(f"Connecting to: {url}")

            # Setup SSL context with certificates
            sslopt = {
                "cert_reqs": ssl.CERT_REQUIRED,
                "ca_certs": self.cert_root,
                "certfile": self.cert_client,
                "keyfile": self.cert_key,
                "check_hostname": False,
                "ssl_version": ssl.PROTOCOL_TLS,
            }

            # Setup headers
            headers = {
                "X-Qlik-User": f"UserDirectory={self.user_directory}; UserId={self.user_id}",
            }

            # Create WebSocket connection
            self.ws = websocket.create_connection(
                url,
                sslopt=sslopt,
                header=headers,
                timeout=self.timeout,
            )

            print("Connected to Qlik Engine")

            # Open the app using OpenDoc
            print(f"Opening app: {app_id}")
            result = self._send_request(
                "OpenDoc",
                -1,  # Global handle
                {"qDocName": app_id},
            )

            if result and "qReturn" in result and "qHandle" in result["qReturn"]:
                self.app_handle = result["qReturn"]["qHandle"]
                print(f"App opened with handle: {self.app_handle}")

                # Verify by getting app layout
                layout = self._send_request("GetAppLayout", self.app_handle)
                if layout:
                    app_title = layout.get("qTitle", app_id)
                    print(f"Successfully opened app: {app_title}")
                    return True

            print("Failed to open app")
            return False

        except Exception as e:
            print(f"Connection failed: {e}")
            return False

    def disconnect(self):
        """Close WebSocket connection"""
        if self.ws:
            self.ws.close()
            self.ws = None
            self.app_handle = None
            print("Disconnected from Qlik Engine")

    def connect_global(self) -> bool:
        """Connect to Qlik Engine global context (for listing apps)"""
        try:
            # Connect to global context (no specific app)
            url = f"wss://{self.server_url}:{self.server_port}/app/"
            print(f"Connecting to global context: {url}")

            # Setup SSL context with certificates
            sslopt = {
                "cert_reqs": ssl.CERT_REQUIRED,
                "ca_certs": self.cert_root,
                "certfile": self.cert_client,
                "keyfile": self.cert_key,
                "check_hostname": False,
                "ssl_version": ssl.PROTOCOL_TLS,
            }

            # Setup headers
            headers = {
                "X-Qlik-User": f"UserDirectory={self.user_directory}; UserId={self.user_id}",
            }

            # Create WebSocket connection
            self.ws = websocket.create_connection(
                url,
                sslopt=sslopt,
                header=headers,
                timeout=self.timeout,
            )

            print("Connected to Qlik Engine global context")
            return True

        except Exception as e:
            print(f"Global connection failed: {e}")
            return False

    def get_doc_list(self) -> dict[str, Any]:
        """Get list of all available applications"""
        if not self.ws:
            raise ConnectionError("Not connected to Qlik Engine")

        try:
            print("Fetching application list...")

            # Get document list using global handle (-1)
            result = self._send_request("GetDocList", -1)

            apps = []
            if result and "qDocList" in result:
                doc_list = result["qDocList"]
                print(f"Found {len(doc_list)} applications")

                for app in doc_list:
                    app_info = {
                        "app_id": app.get("qDocId", ""),
                        "name": app.get("qTitle", "Untitled"),
                        "last_reload_time": app.get("qLastReloadTime", ""),
                        "meta": app.get("qMeta", {}),
                        "doc_type": app.get("qDocType", ""),
                    }
                    apps.append(app_info)

            return {
                "applications": apps,
                "count": len(apps),
            }

        except Exception as e:
            print(f"Error fetching application list: {e}")
            raise

    def get_measures(self, include_expression: bool = True, include_tags: bool = True) -> dict[str, Any]:
        """Retrieve all measures from the current app"""
        if not self.ws or not self.app_handle:
            raise ConnectionError("Not connected to Qlik Engine")

        try:
            # Create MeasureList session object
            print("Creating MeasureList session object...")

            # Build qData paths based on options
            q_data = {
                "title": "/qMetaDef/title",
                "description": "/qMetaDef/description",
                "id": "/qInfo/qId",
            }

            if include_expression:
                q_data["expression"] = "/qMeasure/qDef"
                q_data["label"] = "/qMeasure/qLabel"

            if include_tags:
                q_data["tags"] = "/qMetaDef/tags"

            create_params = [
                {
                    "qInfo": {
                        "qType": "MeasureList",
                    },
                    "qMeasureListDef": {
                        "qType": "measure",
                        "qData": q_data,
                    },
                },
            ]

            create_result = self._send_request(
                "CreateSessionObject",
                self.app_handle,
                create_params,
            )

            if not create_result or "qReturn" not in create_result:
                raise ValueError("Failed to create MeasureList object")

            measure_list_handle = create_result["qReturn"]["qHandle"]
            print(f"Created MeasureList with handle: {measure_list_handle}")

            # Get layout containing measure data
            layout = self._send_request("GetLayout", measure_list_handle)
            # The actual data is nested under qLayout
            actual_layout = layout.get("qLayout", layout) if layout else {}

            # Extract measures from layout
            measures = []
            if actual_layout and "qMeasureList" in actual_layout:
                items = actual_layout["qMeasureList"].get("qItems", [])
                print(f"Processing {len(items)} measures...")
                for item in items:
                    # Parse both qData (custom paths) and standard qInfo/qMeta
                    q_data = item.get("qData", {})
                    q_info = item.get("qInfo", {})
                    q_meta = item.get("qMeta", {})

                    measure = {
                        "id": q_data.get("id", q_info.get("qId", "")),
                        "title": q_data.get("title", q_meta.get("title", "")),
                        "description": q_data.get("description", q_meta.get("description", "")),
                    }

                    if include_expression:
                        # Try to get expression from qData first, then fall back to qMeasure
                        expression_data = q_data.get("expression", {})
                        if isinstance(expression_data, dict):
                            measure["expression"] = expression_data.get("qDef", "") or expression_data.get("qExpr", "")
                        else:
                            # Fallback to standard qMeasure structure
                            q_measure = item.get("qMeasure", {})
                            measure["expression"] = (
                                q_measure.get("qDef", "") or
                                str(expression_data) if expression_data else ""
                            )

                        # Try to get label
                        label_data = q_data.get("label", {})
                        if isinstance(label_data, dict):
                            measure["label"] = label_data.get("qExpr", "")
                        else:
                            measure["label"] = (
                                item.get("qMeasure", {}).get("qLabel", "") or
                                str(label_data) if label_data else ""
                            )

                    if include_tags:
                        measure["tags"] = q_data.get("tags", [])

                    measures.append(measure)

            # Session object cleanup deferred to prevent connection issues
            # Session objects are automatically cleaned when connection closes
            print(f"Found {len(measures)} measures")

            return {
                "measures": measures,
                "count": len(measures),
            }

        except Exception as e:
            print(f"Error retrieving measures: {e}")
            raise

    def get_variables(
        self,
        include_definition: bool = True,
        include_tags: bool = True,
        show_reserved: bool = True,
        show_config: bool = True,
    ) -> dict[str, Any]:
        """Retrieve all variables from the current app"""
        if not self.ws or not self.app_handle:
            raise ConnectionError("Not connected to Qlik Engine")

        try:
            # Create VariableList session object
            print("Creating VariableList session object...")

            # Build qData paths based on options
            q_data = {
                "name": "/qName",
            }

            if include_definition:
                q_data["definition"] = "/qDefinition"

            if include_tags:
                q_data["tags"] = "/tags"

            create_params = [
                {
                    "qInfo": {
                        "qType": "VariableList",
                    },
                    "qVariableListDef": {
                        "qType": "variable",
                        "qShowReserved": show_reserved,
                        "qShowConfig": show_config,
                        "qData": q_data,
                    },
                },
            ]

            create_result = self._send_request(
                "CreateSessionObject",
                self.app_handle,
                create_params,
            )

            if not create_result or "qReturn" not in create_result:
                raise ValueError("Failed to create VariableList object")

            variable_list_handle = create_result["qReturn"]["qHandle"]
            print(f"Created VariableList with handle: {variable_list_handle}")

            # Get layout containing variable data
            layout = self._send_request("GetLayout", variable_list_handle)
            # The actual data is nested under qLayout
            actual_layout = layout.get("qLayout", layout) if layout else {}

            # Extract variables from layout
            variables = []
            if actual_layout and "qVariableList" in actual_layout:
                items = actual_layout["qVariableList"].get("qItems", [])
                print(f"Processing {len(items)} variables...")
                for item in items:
                    # Parse both qData (custom paths) and standard qInfo
                    q_data = item.get("qData", {})
                    q_info = item.get("qInfo", {})

                    variable = {
                        "name": q_data.get("name", q_info.get("qId", "")),
                    }

                    if include_definition:
                        variable["definition"] = q_data.get("definition", "")

                    if include_tags:
                        variable["tags"] = q_data.get("tags", [])

                    # Add additional metadata if available
                    if "qMeta" in item:
                        q_meta = item["qMeta"]
                        variable["is_reserved"] = q_meta.get("qIsReserved", False)
                        variable["is_config"] = q_meta.get("qIsConfig", False)

                    variables.append(variable)

            # Session object cleanup deferred to prevent connection issues
            # Session objects are automatically cleaned when connection closes
            print(f"Found {len(variables)} variables")

            return {
                "variables": variables,
                "count": len(variables),
            }

        except Exception as e:
            print(f"Error retrieving variables: {e}")
            raise

    def get_fields(
        self,
        show_system: bool = True,
        show_hidden: bool = True,
        show_derived_fields: bool = True,
        show_semantic: bool = True,
        show_src_tables: bool = True,
        show_implicit: bool = True,
    ) -> dict[str, Any]:
        """Retrieve all fields from the current app"""
        if not self.ws or not self.app_handle:
            raise ConnectionError("Not connected to Qlik Engine")

        try:
            # Create FieldList session object
            print("Creating FieldList session object...")

            create_params = [
                {
                    "qInfo": {
                        "qType": "FieldList",
                    },
                    "qFieldListDef": {
                        "qShowSystem": show_system,
                        "qShowHidden": show_hidden,
                        "qShowDerivedFields": show_derived_fields,
                        "qShowSemantic": show_semantic,
                        "qShowSrcTables": show_src_tables,
                        "qShowImplicit": show_implicit,
                    },
                },
            ]

            create_result = self._send_request(
                "CreateSessionObject",
                self.app_handle,
                create_params,
            )

            if not create_result or "qReturn" not in create_result:
                raise ValueError("Failed to create FieldList object")

            field_list_handle = create_result["qReturn"]["qHandle"]
            print(f"Created FieldList with handle: {field_list_handle}")

            # Get layout containing field data
            layout = self._send_request("GetLayout", field_list_handle)
            # The actual data is nested under qLayout
            actual_layout = layout.get("qLayout", layout) if layout else {}

            # Extract fields from layout
            fields = []
            tables = set()

            if actual_layout and "qFieldList" in actual_layout:
                items = actual_layout["qFieldList"].get("qItems", [])
                print(f"Processing {len(items)} fields...")

                for item in items:
                    field_name = item.get("qName", "")

                    field_info = {
                        "name": field_name,
                        "is_system": item.get("qIsSystem", False),
                        "is_hidden": item.get("qIsHidden", False),
                        "is_semantic": item.get("qIsSemantic", False),
                        "is_numeric": item.get("qIsNumeric", False),
                        "cardinal": item.get("qCardinal", 0),
                    }

                    # Add source table information if available
                    src_tables = item.get("qSrcTables", [])
                    if src_tables:
                        field_info["source_tables"] = src_tables
                        # Add tables to our set for summary
                        tables.update(src_tables)

                    # Add tags if available
                    tags = item.get("qTags", [])
                    if tags:
                        field_info["tags"] = tags

                    # Add field type information
                    if "qAndMode" in item:
                        field_info["and_mode"] = item["qAndMode"]

                    fields.append(field_info)

            # Convert tables set to sorted list for consistent output
            tables_list = sorted(list(tables))

            # Session object cleanup deferred to prevent connection issues
            # Session objects are automatically cleaned when connection closes
            print(f"Found {len(fields)} fields across {len(tables_list)} tables")

            return {
                "fields": fields,
                "field_count": len(fields),
                "tables": tables_list,
                "table_count": len(tables_list),
            }

        except Exception as e:
            print(f"Error retrieving fields: {e}")
            raise

    def get_sheets(
        self,
        include_thumbnail: bool = False,
        include_metadata: bool = True,
    ) -> dict[str, Any]:
        """Retrieve all sheets from the current app"""
        if not self.ws or not self.app_handle:
            raise ConnectionError("Not connected to Qlik Engine")

        try:
            print("Getting sheets using GetAllInfos...")

            # Get all objects in the app
            all_infos_result = self._send_request("GetAllInfos", self.app_handle)

            if not all_infos_result or "qInfos" not in all_infos_result:
                raise ValueError("Failed to get app objects")

            # Filter for sheets
            all_objects = all_infos_result["qInfos"]
            sheet_infos = [obj for obj in all_objects if obj.get("qType") == "sheet"]
            print(f"Found {len(sheet_infos)} sheets")

            sheets = []

            # Get detailed information for each sheet
            for sheet_info in sheet_infos:
                sheet_id = sheet_info.get("qId", "")
                if not sheet_id:
                    continue

                try:
                    # Get the sheet object to retrieve metadata
                    sheet_obj_result = self._send_request("GetObject", self.app_handle, [sheet_id])

                    if sheet_obj_result and "qReturn" in sheet_obj_result:
                        # Get the handle for the sheet object
                        sheet_handle = sheet_obj_result["qReturn"]["qHandle"]

                        # Get the layout from the sheet handle
                        layout_result = self._send_request("GetLayout", sheet_handle)

                        if layout_result and "qLayout" in layout_result:
                            layout = layout_result["qLayout"]
                            q_meta = layout.get("qMeta", {})

                            sheet_data = {
                                "sheet_id": sheet_id,
                                "title": q_meta.get("title", ""),
                                "description": q_meta.get("description", ""),
                                "rank": layout.get("rank", 0),
                            }

                            if include_thumbnail:
                                sheet_data["thumbnail"] = q_meta.get("thumbnail", "")

                            if include_metadata:
                                sheet_data["created"] = q_meta.get("createdDate", "")
                                sheet_data["modified"] = q_meta.get("modifiedDate", "")
                                sheet_data["published"] = q_meta.get("published", False)
                                sheet_data["approved"] = q_meta.get("approved", False)

                            sheets.append(sheet_data)
                        else:
                            raise ValueError("No layout data returned")
                    else:
                        raise ValueError("GetObject failed")

                except Exception as e:
                    print(f"Warning: Could not get metadata for sheet {sheet_id}: {e}")
                    # Add basic sheet info even if metadata retrieval fails
                    sheets.append({
                        "sheet_id": sheet_id,
                        "title": sheet_id,  # Use sheet_id as title fallback
                        "description": "",
                        "rank": 0,
                    })

            # Sort sheets by rank
            sheets.sort(key=lambda x: x.get("rank", 0))

            print(f"Successfully retrieved {len(sheets)} sheets")

            return {
                "sheets": sheets,
                "sheet_count": len(sheets),
            }

        except Exception as e:
            print(f"Error retrieving sheets: {e}")
            raise

    def get_sheet_objects(
        self,
        sheet_id: str,
        include_properties: bool = True,
        include_layout: bool = True,
        include_data_definition: bool = True,
        resolve_master_items: bool = True,
    ) -> dict[str, Any]:
        """Retrieve all visualization objects from a specific sheet, including container contents"""
        if not self.ws or not self.app_handle:
            raise ConnectionError("Not connected to Qlik Engine")

        try:
            # First get the sheet object itself
            print(f"Getting sheet object: {sheet_id}")
            sheet_result = self._send_request(
                "GetObject",
                self.app_handle,
                [sheet_id],
            )

            # print(f"GetObject result: {sheet_result}")  # Debug output

            if not sheet_result or "qReturn" not in sheet_result:
                raise ValueError(f"Failed to get sheet object: {sheet_id}")

            sheet_handle = sheet_result["qReturn"]["qHandle"]
            print(f"Got sheet with handle: {sheet_handle}")

            # Get sheet layout
            sheet_layout = self._send_request("GetLayout", sheet_handle)
            sheet_data = sheet_layout.get("qLayout", sheet_layout) if sheet_layout else {}

            sheet_title = ""
            if "qMeta" in sheet_data:
                sheet_title = sheet_data["qMeta"].get("title", "")

            # Get child objects (visualizations)
            child_infos = []
            if "qChildList" in sheet_data:
                child_list = sheet_data["qChildList"]
                if "qItems" in child_list:
                    child_infos = child_list["qItems"]

            print(f"Found {len(child_infos)} child objects")

            # Pre-fetch master items if needed for resolution
            master_measures_cache = {}
            master_dimensions_cache = {}
            if resolve_master_items:
                print("Pre-fetching master items for resolution...")
                master_measures_cache = self.get_master_measures_map()
                master_dimensions_cache = self.get_master_dimensions_map()

            # Process each visualization object
            objects = []
            for child_info in child_infos:
                obj_id = child_info.get("qInfo", {}).get("qId", "")
                obj_type = child_info.get("qInfo", {}).get("qType", "")

                obj_data = {
                    "object_id": obj_id,
                    "object_type": obj_type,
                }

                # Process ALL objects including containers
                try:
                    # Get the object
                    obj_result = self._send_request(
                        "GetObject",
                        self.app_handle,
                        [obj_id],
                    )

                    if obj_result and "qReturn" in obj_result:
                        obj_handle = obj_result["qReturn"]["qHandle"]

                        # Get object layout for detailed info
                        obj_layout = self._send_request("GetLayout", obj_handle)
                        obj_layout_data = obj_layout.get("qLayout", obj_layout) if obj_layout else {}

                        # Extract title and subtitle
                        if "title" in obj_layout_data:
                            obj_data["title"] = obj_layout_data["title"]
                        if "subtitle" in obj_layout_data:
                            obj_data["subtitle"] = obj_layout_data["subtitle"]

                        # Extract layout information
                        if include_layout and "qInfo" in obj_layout_data:
                            obj_data["layout"] = {
                                "x": child_info.get("qData", {}).get("col", 0),
                                "y": child_info.get("qData", {}).get("row", 0),
                                "width": child_info.get("qData", {}).get("colspan", 0),
                                "height": child_info.get("qData", {}).get("rowspan", 0),
                            }

                        # Check if this is a VizlibContainer or similar container
                        if obj_type.lower() in ["vizlibcontainer", "container", "qlik-tabbed-container"]:
                            print(f"Processing container object: {obj_id} (type: {obj_type})")
                            obj_data["is_container"] = True

                            # Get effective properties for the container
                            effective_props = self.get_effective_properties(obj_handle)

                            # Process container contents
                            container_objects = self._process_container_contents(
                                obj_handle,
                                obj_id,
                                effective_props,
                                include_properties,
                                include_layout,
                                include_data_definition,
                                resolve_master_items,
                                master_measures_cache,
                                master_dimensions_cache,
                            )

                            if container_objects:
                                obj_data["embedded_objects"] = container_objects
                                obj_data["embedded_object_count"] = len(container_objects)

                            # Store container structure if available
                            if effective_props:
                                obj_data["container_structure"] = self._extract_container_structure(effective_props)

                        # Process regular visualization objects
                        # Extract measures and dimensions
                        elif include_data_definition:
                            measures = []
                            dimensions = []

                            # Get measures from HyperCubeDef
                            if "qHyperCubeDef" in obj_layout_data:
                                hc_def = obj_layout_data["qHyperCubeDef"]

                                # Extract measures
                                if "qMeasures" in hc_def:
                                    for measure in hc_def["qMeasures"]:
                                        measure_data = self._process_measure(
                                            measure,
                                            resolve_master_items,
                                            master_measures_cache,
                                        )
                                        measures.append(measure_data)

                                # Extract dimensions
                                if "qDimensions" in hc_def:
                                    for dimension in hc_def["qDimensions"]:
                                        dimension_data = self._process_dimension(
                                            dimension,
                                            resolve_master_items,
                                            master_dimensions_cache,
                                        )
                                        dimensions.append(dimension_data)

                            if measures:
                                obj_data["measures"] = measures
                            if dimensions:
                                obj_data["dimensions"] = dimensions

                        # Extract properties if requested
                        if include_properties:
                            properties = {}

                            # Extract color settings
                            if "color" in obj_layout_data:
                                properties["color"] = obj_layout_data["color"]

                            # Extract other visualization-specific properties
                            if "qHyperCubeDef" in obj_layout_data:
                                hc_def = obj_layout_data["qHyperCubeDef"]
                                if "qInterColumnSortOrder" in hc_def:
                                    properties["sortOrder"] = hc_def["qInterColumnSortOrder"]

                            if properties:
                                obj_data["properties"] = properties

                except Exception as e:
                    print(f"Warning: Could not get details for object {obj_id}: {e}")
                    # Continue with basic info

                objects.append(obj_data)

            return {
                "sheet_title": sheet_title,
                "objects": objects,
                "object_count": len(objects),
            }

        except Exception as e:
            print(f"Error retrieving sheet objects: {e}")
            raise

    def _process_container_contents(
        self,
        container_handle: int,
        container_id: str,
        effective_props: dict[str, Any],
        include_properties: bool,
        include_layout: bool,
        include_data_definition: bool,
        resolve_master_items: bool,
        master_measures_cache: dict[str, dict[str, Any]],
        master_dimensions_cache: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Process and extract embedded objects from a container"""
        embedded_objects = []

        try:
            # Try to extract tabs/panels structure from effective properties
            tabs = []
            vizlib_container_objects = []

            if effective_props:
                # Check for VizlibContainer specific structure
                if "qProp" in effective_props:
                    qprop = effective_props.get("qProp", {})
                    if "containerObjects" in qprop:
                        vizlib_container_objects = qprop.get("containerObjects", [])

                # Look for common container structures
                if not vizlib_container_objects:
                    if "tabs" in effective_props:
                        tabs = effective_props.get("tabs", [])
                    elif "panels" in effective_props:
                        tabs = effective_props.get("panels", [])
                    elif "qProperty" in effective_props:
                        prop = effective_props.get("qProperty", {})
                        if "tabs" in prop:
                            tabs = prop.get("tabs", [])
                        elif "panels" in prop:
                            tabs = prop.get("panels", [])
                    elif "props" in effective_props:
                        props = effective_props.get("props", {})
                        if "tabs" in props:
                            tabs = props.get("tabs", [])

            # Process VizlibContainer objects if found
            if vizlib_container_objects:
                print(f"Found {len(vizlib_container_objects)} VizlibContainer tabs in container {container_id}")
                for tab_idx, container_obj in enumerate(vizlib_container_objects):
                    tab_label = container_obj.get("label", f"Tab_{tab_idx + 1}")
                    tab_id = container_obj.get("cId", f"{container_id}_tab_{tab_idx + 1}")

                    print(f"Processing VizlibContainer tab: {tab_label}")

                    # Extract master items from gridView
                    grid_view = container_obj.get("gridView", {})
                    master_items = grid_view.get("masterItems", [])

                    for master_item in master_items:
                        master_item_id = master_item.get("masterItemId")
                        if master_item_id:
                            print(f"  Found master item: {master_item_id}")
                            # Try to get the master item object
                            try:
                                obj_result = self._send_request(
                                    "GetObject",
                                    self.app_handle,
                                    [master_item_id],
                                )
                                if obj_result and "qReturn" in obj_result:
                                    obj_handle = obj_result["qReturn"]["qHandle"]
                                    if obj_handle:
                                        obj_layout = self._send_request("GetLayout", obj_handle)

                                        embedded_obj = self._create_object_from_layout(
                                            master_item_id,
                                            obj_layout,
                                            container_id,
                                            tab_label,
                                            include_properties,
                                            include_layout,
                                            include_data_definition,
                                            resolve_master_items,
                                            master_measures_cache,
                                            master_dimensions_cache,
                                        )
                                        if embedded_obj:
                                            embedded_obj["container_tab"] = tab_label
                                            embedded_obj["container_tab_id"] = tab_id
                                            embedded_obj["cell_label"] = master_item.get("label", "")
                                            embedded_objects.append(embedded_obj)
                            except Exception as e:
                                print(f"Could not get master item {master_item_id}: {e}")
            else:
                print(f"Found {len(tabs)} tabs/panels in container {container_id}")

            # Process each tab/panel
            for tab_idx, tab in enumerate(tabs):
                tab_objects = []

                # Extract tab metadata
                tab_label = tab.get("label", f"Tab_{tab_idx + 1}")
                tab_id = tab.get("id", f"{container_id}_tab_{tab_idx + 1}")

                # Look for embedded objects in the tab
                if "objects" in tab:
                    for obj in tab.get("objects", []):
                        embedded_obj = self._process_embedded_object(
                            obj,
                            container_id,
                            tab_label,
                            include_properties,
                            include_layout,
                            include_data_definition,
                            resolve_master_items,
                            master_measures_cache,
                            master_dimensions_cache,
                        )
                        if embedded_obj:
                            tab_objects.append(embedded_obj)

                # Also check for object references
                elif "objectId" in tab or "qObjectId" in tab:
                    obj_id = tab.get("objectId") or tab.get("qObjectId")
                    if obj_id:
                        # Try to get the referenced object
                        try:
                            obj_result = self._send_request(
                                "GetObject",
                                self.app_handle,
                                [obj_id],
                            )
                            if obj_result and "qReturn" in obj_result:
                                obj_handle = obj_result["qReturn"]["qHandle"]
                                obj_layout = self._send_request("GetLayout", obj_handle)

                                embedded_obj = self._create_object_from_layout(
                                    obj_id,
                                    obj_layout,
                                    container_id,
                                    tab_label,
                                    include_properties,
                                    include_layout,
                                    include_data_definition,
                                    resolve_master_items,
                                    master_measures_cache,
                                    master_dimensions_cache,
                                )
                                if embedded_obj:
                                    tab_objects.append(embedded_obj)
                        except Exception as e:
                            print(f"Could not get embedded object {obj_id}: {e}")

                # Add tab objects to main list
                for obj in tab_objects:
                    obj["container_tab"] = tab_label
                    obj["container_tab_id"] = tab_id
                    embedded_objects.append(obj)

            # If no tabs structure found, try to get child objects directly
            if not tabs and container_handle:
                try:
                    # Try GetChildInfos to see if there are child objects
                    child_info_result = self._send_request("GetChildInfos", container_handle)
                    if child_info_result:
                        for child in child_info_result:
                            child_id = child.get("qId", "")
                            if child_id:
                                try:
                                    obj_result = self._send_request(
                                        "GetObject",
                                        self.app_handle,
                                        [child_id],
                                    )
                                    if obj_result and "qReturn" in obj_result:
                                        obj_handle = obj_result["qReturn"]["qHandle"]
                                        obj_layout = self._send_request("GetLayout", obj_handle)

                                        embedded_obj = self._create_object_from_layout(
                                            child_id,
                                            obj_layout,
                                            container_id,
                                            "Main",
                                            include_properties,
                                            include_layout,
                                            include_data_definition,
                                            resolve_master_items,
                                            master_measures_cache,
                                            master_dimensions_cache,
                                        )
                                        if embedded_obj:
                                            embedded_obj["container_tab"] = "Main"
                                            embedded_objects.append(embedded_obj)
                                except Exception as e:
                                    print(f"Could not process child object {child_id}: {e}")
                except Exception:
                    pass  # GetChildInfos might not be available

            print(f"Extracted {len(embedded_objects)} embedded objects from container {container_id}")

        except Exception as e:
            print(f"Error processing container contents: {e}")

        return embedded_objects

    def _process_embedded_object(
        self,
        obj_data: dict[str, Any],
        container_id: str,
        tab_label: str,
        include_properties: bool,
        include_layout: bool,
        include_data_definition: bool,
        resolve_master_items: bool,
        master_measures_cache: dict[str, dict[str, Any]],
        master_dimensions_cache: dict[str, dict[str, Any]],
    ) -> dict[str, Any] | None:
        """Process an embedded object definition from container"""
        try:
            obj_id = obj_data.get("id") or obj_data.get("qId", "")
            obj_type = obj_data.get("type") or obj_data.get("qType", "")

            if not obj_id:
                return None

            embedded_obj = {
                "object_id": obj_id,
                "object_type": obj_type,
                "parent_container": container_id,
                "is_embedded": True,
            }

            # Try to get more details about the object
            try:
                obj_result = self._send_request(
                    "GetObject",
                    self.app_handle,
                    [obj_id],
                )
                if obj_result and "qReturn" in obj_result:
                    obj_handle = obj_result["qReturn"]["qHandle"]
                    obj_layout = self._send_request("GetLayout", obj_handle)

                    return self._create_object_from_layout(
                        obj_id,
                        obj_layout,
                        container_id,
                        tab_label,
                        include_properties,
                        include_layout,
                        include_data_definition,
                        resolve_master_items,
                        master_measures_cache,
                        master_dimensions_cache,
                    )
            except Exception:
                pass  # Continue with basic info if detailed fetch fails

            return embedded_obj

        except Exception as e:
            print(f"Error processing embedded object: {e}")
            return None

    def _create_object_from_layout(
        self,
        obj_id: str,
        obj_layout: dict[str, Any],
        container_id: str,
        tab_label: str,
        include_properties: bool,
        include_layout: bool,
        include_data_definition: bool,
        resolve_master_items: bool,
        master_measures_cache: dict[str, dict[str, Any]],
        master_dimensions_cache: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        """Create object data from layout information"""
        obj_layout_data = obj_layout.get("qLayout", obj_layout) if obj_layout else {}

        obj = {
            "object_id": obj_id,
            "object_type": obj_layout_data.get("qInfo", {}).get("qType", ""),
            "parent_container": container_id,
            "is_embedded": True,
        }

        # Extract title and subtitle
        if "title" in obj_layout_data:
            obj["title"] = obj_layout_data["title"]
        if "subtitle" in obj_layout_data:
            obj["subtitle"] = obj_layout_data["subtitle"]

        # Extract measures and dimensions if requested
        if include_data_definition:
            measures = []
            dimensions = []

            if "qHyperCubeDef" in obj_layout_data:
                hc_def = obj_layout_data["qHyperCubeDef"]

                # Extract measures
                if "qMeasures" in hc_def:
                    for measure in hc_def["qMeasures"]:
                        measure_data = self._process_measure(
                            measure,
                            resolve_master_items,
                            master_measures_cache,
                        )
                        measures.append(measure_data)

                # Extract dimensions
                if "qDimensions" in hc_def:
                    for dimension in hc_def["qDimensions"]:
                        dimension_data = self._process_dimension(
                            dimension,
                            resolve_master_items,
                            master_dimensions_cache,
                        )
                        dimensions.append(dimension_data)

            if measures:
                obj["measures"] = measures
            if dimensions:
                obj["dimensions"] = dimensions

        # Extract properties if requested
        if include_properties:
            properties = {}
            if "color" in obj_layout_data:
                properties["color"] = obj_layout_data["color"]
            if properties:
                obj["properties"] = properties

        return obj

    def _extract_container_structure(self, effective_props: dict[str, Any]) -> dict[str, Any]:
        """Extract and simplify container structure from effective properties"""
        structure = {
            "type": "container",
            "tabs": [],
        }

        try:
            tabs = []

            # Check for VizlibContainer specific structure
            if "qProp" in effective_props:
                qprop = effective_props.get("qProp", {})
                if "containerObjects" in qprop:
                    container_objects = qprop.get("containerObjects", [])
                    for container_obj in container_objects:
                        grid_view = container_obj.get("gridView", {})
                        master_items = grid_view.get("masterItems", [])
                        tab_info = {
                            "label": container_obj.get("label", ""),
                            "id": container_obj.get("cId", ""),
                            "object_count": len(master_items),
                            "master_items": [
                                item.get("masterItemId")
                                for item in master_items
                                if item.get("masterItemId")
                            ],
                        }
                        structure["tabs"].append(tab_info)

            # If no VizlibContainer structure, look for standard tabs/panels
            if not structure["tabs"]:
                if "tabs" in effective_props:
                    tabs = effective_props.get("tabs", [])
                elif "panels" in effective_props:
                    tabs = effective_props.get("panels", [])
                elif "qProperty" in effective_props:
                    prop = effective_props.get("qProperty", {})
                    if "tabs" in prop:
                        tabs = prop.get("tabs", [])
                    elif "panels" in prop:
                        tabs = prop.get("panels", [])

                for tab in tabs:
                    tab_info = {
                        "label": tab.get("label", ""),
                        "id": tab.get("id", ""),
                        "object_count": len(tab.get("objects", [])),
                    }
                    structure["tabs"].append(tab_info)

            structure["tab_count"] = len(structure["tabs"])

        except Exception as e:
            print(f"Error extracting container structure: {e}")

        return structure

    def _process_measure(
        self,
        measure: dict[str, Any],
        resolve_master_items: bool,
        master_measures_cache: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        """Process a measure definition, resolving master items if needed"""
        measure_def = measure.get("qDef", {})
        library_id = measure.get("qLibraryId", "")

        measure_data = {
            "label": measure_def.get("qLabel", ""),
            "expression": measure_def.get("qDef", ""),
        }

        # Check if this is a master measure reference
        if library_id and resolve_master_items:
            resolution = self.resolve_master_item_reference(
                library_id,
                "measure",
                master_measures_cache,
            )
            if resolution.get("resolved"):
                master_item = resolution.get("master_item", {})
                measure_data["library_id"] = library_id
                measure_data["is_master_item"] = True
                measure_data["resolved_expression"] = master_item.get("expression", "")
                measure_data["master_item_title"] = master_item.get("title", "")
                if not measure_data["label"]:
                    measure_data["label"] = master_item.get("label", "") or master_item.get("title", "")
                if not measure_data["expression"]:
                    measure_data["expression"] = master_item.get("expression", "")

        return measure_data

    def _process_dimension(
        self,
        dimension: dict[str, Any],
        resolve_master_items: bool,
        master_dimensions_cache: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        """Process a dimension definition, resolving master items if needed"""
        dim_def = dimension.get("qDef", {})
        library_id = dimension.get("qLibraryId", "")

        dimension_data = {
            "label": dim_def.get("qLabel", ""),
            "field": dim_def.get("qFieldDefs", [""])[0] if dim_def.get("qFieldDefs") else "",
        }

        # Check if this is a master dimension reference
        if library_id and resolve_master_items:
            resolution = self.resolve_master_item_reference(
                library_id,
                "dimension",
                master_dimensions_cache,
            )
            if resolution.get("resolved"):
                master_item = resolution.get("master_item", {})
                dimension_data["library_id"] = library_id
                dimension_data["is_master_item"] = True
                dimension_data["resolved_field_definitions"] = master_item.get("field_definitions", [])
                dimension_data["master_item_title"] = master_item.get("title", "")
                if not dimension_data["label"]:
                    labels = master_item.get("labels", [])
                    if labels:
                        dimension_data["label"] = labels[0]
                if not dimension_data["field"] and master_item.get("field_definitions"):
                    dimension_data["field"] = master_item["field_definitions"][0]

        return dimension_data

    def get_effective_properties(self, object_handle: int) -> dict[str, Any]:
        """Get effective properties of an object (especially useful for containers)"""
        if not self.ws:
            raise ConnectionError("Not connected to Qlik Engine")

        try:
            result = self._send_request("GetEffectiveProperties", object_handle)
            return result if result else {}
        except Exception as e:
            print(f"Error getting effective properties: {e}")
            return {}

    def get_master_measures_map(self) -> dict[str, dict[str, Any]]:
        """Get all master measures and return as a map keyed by ID"""
        if not self.ws or not self.app_handle:
            raise ConnectionError("Not connected to Qlik Engine")

        try:
            print("Fetching master measures for reference resolution...")
            measures_result = self.get_measures(include_expression=True, include_tags=False)
            measures_map = {}

            for measure in measures_result.get("measures", []):
                measure_id = measure.get("id")
                if measure_id:
                    measures_map[measure_id] = {
                        "expression": measure.get("expression", ""),
                        "label": measure.get("label", ""),
                        "title": measure.get("title", ""),
                        "description": measure.get("description", ""),
                    }

            print(f"Cached {len(measures_map)} master measures")
            return measures_map

        except Exception as e:
            print(f"Error fetching master measures map: {e}")
            return {}

    def get_master_dimensions_map(self) -> dict[str, dict[str, Any]]:
        """Get all master dimensions and return as a map keyed by ID"""
        if not self.ws or not self.app_handle:
            raise ConnectionError("Not connected to Qlik Engine")

        try:
            print("Fetching master dimensions for reference resolution...")
            dimensions_result = self.get_dimensions(
                include_title=True,
                include_tags=False,
                include_grouping=True,
                include_info=True,
            )
            dimensions_map = {}

            for dimension in dimensions_result.get("dimensions", []):
                dimension_id = dimension.get("id")
                if dimension_id:
                    # Extract field definitions and labels
                    dim_info = dimension.get("info", [])
                    field_defs = []
                    labels = []

                    for info_item in dim_info:
                        if isinstance(info_item, dict):
                            field_def = info_item.get("qFieldDefs", [])
                            if field_def:
                                field_defs.extend(field_def)
                            label = info_item.get("qLabel", "")
                            if label:
                                labels.append(label)

                    dimensions_map[dimension_id] = {
                        "field_definitions": field_defs,
                        "labels": labels,
                        "title": dimension.get("title", ""),
                        "grouping": dimension.get("grouping", ""),
                    }

            print(f"Cached {len(dimensions_map)} master dimensions")
            return dimensions_map

        except Exception as e:
            print(f"Error fetching master dimensions map: {e}")
            return {}

    def resolve_master_item_reference(
        self,
        library_id: str,
        item_type: str,
        master_items_cache: dict[str, dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Resolve a master item reference to its full definition

        Args:
            library_id: The qLibraryId reference
            item_type: Either 'measure' or 'dimension'
            master_items_cache: Optional pre-fetched cache of master items

        Returns:
            Dictionary with resolved expression/field definitions

        """
        if not library_id:
            return {}

        try:
            if item_type == "measure":
                if master_items_cache is None:
                    master_items_cache = self.get_master_measures_map()

                if library_id in master_items_cache:
                    return {
                        "resolved": True,
                        "master_item": master_items_cache[library_id],
                    }

            elif item_type == "dimension":
                if master_items_cache is None:
                    master_items_cache = self.get_master_dimensions_map()

                if library_id in master_items_cache:
                    return {
                        "resolved": True,
                        "master_item": master_items_cache[library_id],
                    }

            return {"resolved": False, "reason": "Master item not found"}

        except Exception as e:
            print(f"Error resolving master item {library_id}: {e}")
            return {"resolved": False, "reason": str(e)}

    def get_dimensions(
        self,
        include_title: bool = True,
        include_tags: bool = True,
        include_grouping: bool = True,
        include_info: bool = True,
    ) -> dict[str, Any]:
        """Retrieve all dimensions from the current app"""
        if not self.ws or not self.app_handle:
            raise ConnectionError("Not connected to Qlik Engine")

        try:
            print("Creating DimensionList session object...")

            # Build qData paths based on options
            q_data = {}

            if include_title:
                q_data["title"] = "/title"

            if include_tags:
                q_data["tags"] = "/tags"

            if include_grouping:
                q_data["grouping"] = "/qDim/qGrouping"

            if include_info:
                q_data["info"] = "/qDimInfos"

            create_params = [
                {
                    "qInfo": {
                        "qType": "DimensionList",
                    },
                    "qDimensionListDef": {
                        "qType": "dimension",
                        "qData": q_data,
                    },
                },
            ]

            create_result = self._send_request(
                "CreateSessionObject",
                self.app_handle,
                create_params,
            )

            if not create_result or "qReturn" not in create_result:
                raise ValueError("Failed to create DimensionList object")

            dimension_list_handle = create_result["qReturn"]["qHandle"]
            print(f"Created DimensionList with handle: {dimension_list_handle}")

            # Get layout containing dimension data
            layout = self._send_request("GetLayout", dimension_list_handle)
            # The actual data is nested under qLayout
            actual_layout = layout.get("qLayout", layout) if layout else {}

            # Extract dimensions from layout
            dimensions = []
            if actual_layout and "qDimensionList" in actual_layout:
                items = actual_layout["qDimensionList"].get("qItems", [])
                print(f"Processing {len(items)} dimensions...")
                for item in items:
                    # Parse both qData (custom paths) and standard qInfo
                    q_data = item.get("qData", {})
                    q_info = item.get("qInfo", {})
                    q_meta = item.get("qMeta", {})

                    dimension = {
                        "dimension_id": q_info.get("qId", ""),
                        "name": q_meta.get("title", q_info.get("qId", "")),
                    }

                    if include_title:
                        dimension["title"] = q_data.get("title", q_meta.get("title", ""))

                    if include_tags:
                        dimension["tags"] = q_data.get("tags", [])

                    if include_grouping:
                        dimension["grouping"] = q_data.get("grouping", "N")

                    if include_info:
                        dimension["info"] = q_data.get("info", [])

                    # Add additional metadata from qMeta
                    dimension["description"] = q_meta.get("description", "")
                    dimension["created"] = q_meta.get("createdDate", "")
                    dimension["modified"] = q_meta.get("modifiedDate", "")
                    dimension["published"] = q_meta.get("published", False)
                    dimension["approved"] = q_meta.get("approved", False)

                    dimensions.append(dimension)

            # Session object cleanup deferred to prevent connection issues
            # Session objects are automatically cleaned when connection closes
            print(f"Found {len(dimensions)} dimensions")

            return {
                "dimensions": dimensions,
                "dimension_count": len(dimensions),
            }

        except Exception as e:
            print(f"Error retrieving dimensions: {e}")
            raise

    def get_script(self) -> dict[str, Any]:
        """Retrieve the script from the current app"""
        if not self.ws or not self.app_handle:
            raise ConnectionError("Not connected to Qlik Engine")

        try:
            print("Getting app script...")

            # Call GetScript method on the app handle
            script_result = self._send_request("GetScript", self.app_handle)

            if not script_result:
                raise ValueError("Failed to get app script")

            print("Successfully retrieved app script")

            # The script content is typically in qScript
            script_content = script_result.get("qScript", "")

            return {
                "script": script_content,
                "script_length": len(script_content),
            }

        except Exception as e:
            print(f"Error retrieving script: {e}")
            raise

    def get_lineage(
        self,
        include_resident: bool = True,
        include_file_sources: bool = True,
        include_binary_sources: bool = True,
        include_inline_sources: bool = True,
    ) -> dict[str, Any]:
        """Retrieve data sources lineage from the current app"""
        if not self.ws or not self.app_handle:
            raise ConnectionError("Not connected to Qlik Engine")

        try:
            print("Getting app data sources lineage...")

            # Call GetLineage method on the app handle (no parameters needed)
            lineage_result = self._send_request("GetLineage", self.app_handle)

            if not lineage_result:
                raise ValueError("Failed to get app lineage")

            print("Successfully retrieved app lineage")

            # The lineage data is in qLineage
            lineage_data = lineage_result.get("qLineage", [])

            # Process and categorize the data sources
            data_sources = []
            categories = {
                "binary": [],
                "resident": [],
                "file": [],
                "inline": [],
                "other": [],
            }

            for item in lineage_data:
                discriminator = item.get("qDiscriminator", "")
                statement = item.get("qStatement", "")

                # Create data source object
                source = {
                    "discriminator": discriminator,
                    "statement": statement if statement else None,
                    "type": self._categorize_data_source(discriminator, statement),
                }

                # Apply filters based on include options
                source_type = source["type"]
                if (source_type == "binary" and include_binary_sources) or \
                   (source_type == "resident" and include_resident) or \
                   (source_type == "file" and include_file_sources) or \
                   (source_type == "inline" and include_inline_sources) or \
                   (source_type == "other"):

                    data_sources.append(source)
                    categories[source_type].append(source)

            print(f"Found {len(data_sources)} data sources")

            return {
                "data_sources": data_sources,
                "source_count": len(data_sources),
                "categories": {
                    "binary_count": len(categories["binary"]),
                    "resident_count": len(categories["resident"]),
                    "file_count": len(categories["file"]),
                    "inline_count": len(categories["inline"]),
                    "other_count": len(categories["other"]),
                },
                "by_category": categories,
            }

        except Exception as e:
            print(f"Error retrieving lineage: {e}")
            raise

    def _categorize_data_source(self, discriminator: str, statement: str = None) -> str:
        """Categorize data source based on discriminator and statement"""
        discriminator_lower = discriminator.lower()

        # Check for binary sources
        if statement and statement.lower() == "binary":
            return "binary"

        # Check for resident sources
        if discriminator_lower.startswith("resident "):
            return "resident"

        # Check for inline sources
        if discriminator_lower.startswith("inline"):
            return "inline"

        # Check for file sources (paths, URLs, etc.)
        if any(indicator in discriminator_lower for indicator in [
            "\\", "/", ".", "lib://", "http://", "https://", "ftp://", ".txt", ".csv", ".xlsx", ".qvd",
        ]):
            return "file"

        # Everything else
        return "other"

    # ============================================
    # WRITE Methods
    # ============================================

    @staticmethod
    def _build_dimension_def(dimension: dict[str, Any]) -> dict[str, Any]:
        """Build a Qlik hypercube dimension definition from structured input."""
        field = dimension.get("field")
        library_id = dimension.get("library_id")
        label = dimension.get("label") or field or library_id or "Dimension"

        q_def: dict[str, Any] = {
            "qLabel": label,
            "qNullSuppression": False,
        }

        if library_id:
            q_def["qLibraryId"] = library_id
        else:
            if not field:
                raise ValueError("Dimension requires either field or library_id")
            q_def["qDef"] = {
                "qFieldDefs": [field],
                "qFieldLabels": [label],
            }

        if dimension.get("sort_by_ascii") not in (None, 0):
            q_def["qDef"] = q_def.get("qDef", {})
            q_def["qDef"]["qSortCriterias"] = [{
                "qSortByAscii": int(dimension["sort_by_ascii"]),
            }]

        return q_def

    @staticmethod
    def _build_measure_def(measure: dict[str, Any]) -> dict[str, Any]:
        """Build a Qlik hypercube measure definition from structured input."""
        expression = measure.get("expression")
        library_id = measure.get("library_id")
        label = measure.get("label") or expression or library_id or "Measure"

        q_def: dict[str, Any] = {
            "qLabel": label,
        }

        if library_id:
            q_def["qLibraryId"] = library_id
        else:
            if not expression:
                raise ValueError("Measure requires either expression or library_id")
            q_def["qDef"] = expression

        if measure.get("number_format"):
            q_def["qNumFormat"] = {
                "qType": "U",
                "qnDec": 10,
                "qUseThou": 1,
                "qFmt": measure["number_format"],
            }

        return q_def

    @staticmethod
    def _default_sort_order(dimensions: list[dict[str, Any]] | None = None, measures: list[dict[str, Any]] | None = None) -> list[int]:
        """Use a consistent dimensions-first, measures-last sort order."""
        return list(range(len(dimensions or []) + len(measures or [])))

    @classmethod
    def build_hypercube_def(
        cls,
        dimensions: list[dict[str, Any]] | None = None,
        measures: list[dict[str, Any]] | None = None,
        sort_order: list[int] | None = None,
        column_order: list[int] | None = None,
        initial_rows: int = 100,
        initial_columns: int | None = None,
        suppress_zero: bool = False,
        suppress_missing: bool = False,
    ) -> dict[str, Any]:
        """Build a structured qHyperCubeDef from dimensions and measures."""
        q_dimensions = [cls._build_dimension_def(dimension) for dimension in (dimensions or [])]
        q_measures = [cls._build_measure_def(measure) for measure in (measures or [])]
        column_count = initial_columns or max(len(q_dimensions) + len(q_measures), 1)

        hypercube = {
            "qDimensions": q_dimensions,
            "qMeasures": q_measures,
            "qInitialDataFetch": [{
                "qTop": 0,
                "qLeft": 0,
                "qHeight": initial_rows,
                "qWidth": column_count,
            }],
            "qSuppressZero": suppress_zero,
            "qSuppressMissing": suppress_missing,
        }

        hypercube["qInterColumnSortOrder"] = (
            sort_order if sort_order is not None else cls._default_sort_order(dimensions, measures)
        )

        if column_order is not None:
            hypercube["columnOrder"] = column_order

        return hypercube

    def create_visualization(
        self,
        object_type: str,
        title: str,
        dimensions: list[dict[str, Any]] | None = None,
        measures: list[dict[str, Any]] | None = None,
        visualization_properties: dict[str, Any] | None = None,
        object_id: str | None = None,
    ) -> dict[str, Any]:
        """Create a visualization backed by a structured hypercube definition."""
        viz_props = dict(visualization_properties or {})
        properties = {
            "qHyperCubeDef": self.build_hypercube_def(
                dimensions=dimensions,
                measures=measures,
                sort_order=viz_props.pop("sort_order", None),
                column_order=viz_props.pop("column_order", None),
                initial_rows=viz_props.pop("initial_rows", 100),
                initial_columns=viz_props.pop("initial_columns", None),
                suppress_zero=viz_props.pop("suppress_zero", False),
                suppress_missing=viz_props.pop("suppress_missing", False),
            ),
        }
        properties.update(viz_props)

        return self.create_object(
            object_type=object_type,
            title=title,
            properties=properties,
            object_id=object_id,
        )

    @classmethod
    def build_bar_chart_properties(
        cls,
        dimensions: list[dict[str, Any]],
        measures: list[dict[str, Any]],
        orientation: str = "vertical",
        stacked: bool = False,
        show_legend: bool = True,
    ) -> dict[str, Any]:
        return {
            "qHyperCubeDef": cls.build_hypercube_def(dimensions=dimensions, measures=measures),
            "orientation": orientation,
            "barGrouping": "stacked" if stacked else "grouped",
            "legend": {"show": show_legend},
        }

    @classmethod
    def build_line_chart_properties(
        cls,
        dimensions: list[dict[str, Any]],
        measures: list[dict[str, Any]],
        show_markers: bool = True,
    ) -> dict[str, Any]:
        return {
            "qHyperCubeDef": cls.build_hypercube_def(dimensions=dimensions, measures=measures),
            "dataPoint": {"show": show_markers},
            "legend": {"show": True},
        }

    @classmethod
    def build_kpi_properties(
        cls,
        dimensions: list[dict[str, Any]] | None = None,
        measures: list[dict[str, Any]] | None = None,
        subtitle: str = "",
    ) -> dict[str, Any]:
        properties: dict[str, Any] = {
            "qHyperCubeDef": cls.build_hypercube_def(
                dimensions=dimensions,
                measures=measures,
                initial_rows=1,
                initial_columns=max(len(dimensions or []) + len(measures or []), 1),
            ),
        }
        if subtitle:
            properties["subtitle"] = subtitle
        return properties

    @classmethod
    def build_table_properties(
        cls,
        dimensions: list[dict[str, Any]] | None = None,
        measures: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        return {
            "qHyperCubeDef": cls.build_hypercube_def(dimensions=dimensions, measures=measures),
        }

    def create_bar_chart(
        self,
        title: str,
        dimensions: list[dict[str, Any]],
        measures: list[dict[str, Any]],
        orientation: str = "vertical",
        stacked: bool = False,
        show_legend: bool = True,
        object_id: str | None = None,
    ) -> dict[str, Any]:
        """Create a bar chart visualization."""
        return self.create_object(
            object_type="barchart",
            title=title,
            properties=self.build_bar_chart_properties(
                dimensions=dimensions,
                measures=measures,
                orientation=orientation,
                stacked=stacked,
                show_legend=show_legend,
            ),
            object_id=object_id,
        )

    def create_line_chart(
        self,
        title: str,
        dimensions: list[dict[str, Any]],
        measures: list[dict[str, Any]],
        show_markers: bool = True,
        object_id: str | None = None,
    ) -> dict[str, Any]:
        """Create a line chart visualization."""
        return self.create_object(
            object_type="linechart",
            title=title,
            properties=self.build_line_chart_properties(
                dimensions=dimensions,
                measures=measures,
                show_markers=show_markers,
            ),
            object_id=object_id,
        )

    def create_kpi(
        self,
        title: str,
        dimensions: list[dict[str, Any]] | None = None,
        measures: list[dict[str, Any]] | None = None,
        subtitle: str = "",
        object_id: str | None = None,
    ) -> dict[str, Any]:
        """Create a KPI visualization."""
        return self.create_object(
            object_type="kpi",
            title=title,
            properties=self.build_kpi_properties(
                dimensions=dimensions,
                measures=measures,
                subtitle=subtitle,
            ),
            object_id=object_id,
        )

    def create_table(
        self,
        title: str,
        dimensions: list[dict[str, Any]] | None = None,
        measures: list[dict[str, Any]] | None = None,
        object_id: str | None = None,
    ) -> dict[str, Any]:
        """Create a straight table visualization."""
        return self.create_object(
            object_type="table",
            title=title,
            properties=self.build_table_properties(
                dimensions=dimensions,
                measures=measures,
            ),
            object_id=object_id,
        )

    def create_measure(
        self,
        title: str,
        expression: str,
        description: str = "",
        label: str = "",
        tags: list[str] | None = None,
        measure_id: str | None = None,
    ) -> dict[str, Any]:
        """Create a new master measure in the app."""
        if not self.app_handle:
            raise ConnectionError("Not connected to an app")

        q_prop = {
            "qInfo": {"qType": "measure"},
            "qMeasure": {
                "qLabel": label or title,
                "qDef": expression,
                "qGrouping": "N",
                "qExpressions": [],
                "qActiveExpression": 0,
            },
            "qMetaDef": {
                "title": title,
                "description": description,
                "tags": tags or [],
            },
        }

        if measure_id:
            q_prop["qInfo"]["qId"] = measure_id

        result = self._send_request("CreateMeasure", self.app_handle, {"qProp": q_prop})

        if result and "qReturn" in result:
            return {
                "success": True,
                "measure_id": result["qReturn"].get("qGenericId"),
                "title": title,
                "expression": expression,
            }

        return {"success": False, "error": "Failed to create measure"}

    def update_measure(
        self,
        measure_id: str,
        title: str | None = None,
        expression: str | None = None,
        description: str | None = None,
        label: str | None = None,
        tags: list[str] | None = None,
    ) -> dict[str, Any]:
        """Update an existing master measure in the app."""

        if not self.app_handle:
            raise ConnectionError("Not connected to an app")

        q_prop: dict[str, Any] = {
            "qInfo": {
                "qType": "measure",
                "qId": measure_id,
            },
            "qMeasure": {
                "qGrouping": "N",
                "qExpressions": [],
                "qActiveExpression": 0,
            },
            "qMetaDef": {},
        }

        if expression is not None:
            q_prop["qMeasure"]["qDef"] = expression

        if label is not None:
            q_prop["qMeasure"]["qLabel"] = label

        if title is not None:
            q_prop["qMetaDef"]["title"] = title

        if description is not None:
            q_prop["qMetaDef"]["description"] = description

        if tags is not None:
            q_prop["qMetaDef"]["tags"] = tags

        result = self._send_request("UpdateMeasure", self.app_handle, {"qProp": q_prop})

        if result and "qReturn" in result:
            return {
                "success": True,
                "measure_id": measure_id,
            }

        return {"success": False, "error": "Failed to update measure"}

    def delete_measure(self, measure_id: str) -> dict[str, Any]:
        """Delete a master measure from the app."""

        if not self.app_handle:
            raise ConnectionError("Not connected to an app")

        q_prop = {
            "qInfo": {
                "qType": "measure",
                "qId": measure_id,
            }
        }

        result = self._send_request("DestroyMeasure", self.app_handle, {"qProp": q_prop})

        if result and "qReturn" in result:
            return {"success": True, "measure_id": measure_id}

        return {"success": False, "error": "Failed to delete measure"}

    def create_variable(
        self,
        name: str,
        definition: str = "",
        comment: str = "",
        variable_id: str | None = None,
    ) -> dict[str, Any]:
        """Create a new variable in the app.

        Args:
            name: Variable name (case sensitive)
            definition: Variable value or expression
            comment: Optional comment/description
            variable_id: Optional custom ID

        Returns:
            Dict with created variable info
        """
        if not self.app_handle:
            raise ConnectionError("Not connected to an app")

        q_prop = {
            "qInfo": {"qType": "variable"},
            "qName": name,
            "qComment": comment,
        }

        if definition:
            q_prop["qDefinition"] = definition

        if variable_id:
            q_prop["qInfo"]["qId"] = variable_id

        result = self._send_request("CreateVariableEx", self.app_handle, {"qProp": q_prop})

        if result and "qReturn" in result:
            return {
                "success": True,
                "variable_name": name,
                "definition": definition,
            }

        return {"success": False, "error": "Failed to create variable"}

    def update_variable(
        self,
        name: str,
        definition: str | None = None,
    ) -> dict[str, Any]:
        """Update a variable definition in the app."""

        if not self.app_handle:
            raise ConnectionError("Not connected to an app")

        params: dict[str, Any] = {
            "qName": name,
        }

        if definition is not None:
            params["qValue"] = definition

        result = self._send_request("SetVariable", self.app_handle, params)

        if result and "qReturn" in result:
            return {"success": True, "variable_name": name}

        return {"success": False, "error": "Failed to update variable"}

    def delete_variable(self, name: str) -> dict[str, Any]:
        """Delete a variable from the app."""

        if not self.app_handle:
            raise ConnectionError("Not connected to an app")

        result = self._send_request("DestroyVariable", self.app_handle, {"qName": name})

        if result and "qReturn" in result:
            return {"success": True, "variable_name": name}

        return {"success": False, "error": "Failed to delete variable"}

    def create_dimension(
        self,
        title: str,
        field_def: str | list[str],
        description: str = "",
        tags: list[str] | None = None,
        grouping: str = "N",
        dimension_id: str | None = None,
    ) -> dict[str, Any]:
        """Create a new master dimension in the app.

        Args:
            title: Dimension title/name
            field_def: Field name(s) - can be string or list
            description: Optional description
            tags: Optional list of tags
            grouping: Grouping type: "N" (None), "H" (Drill-down)
            dimension_id: Optional custom ID

        Returns:
            Dict with created dimension info
        """
        if not self.app_handle:
            raise ConnectionError("Not connected to an app")

        if isinstance(field_def, str):
            field_defs = [field_def]
            field_labels = [title]
        else:
            field_defs = field_def
            field_labels = field_def

        q_prop = {
            "qInfo": {"qType": "dimension"},
            "qDim": {
                "qGrouping": grouping,
                "qFieldDefs": field_defs,
                "qFieldLabels": field_labels,
                "qLabelExpression": "",
            },
            "qMetaDef": {
                "title": title,
                "description": description,
                "tags": tags or [],
            },
        }

        if dimension_id:
            q_prop["qInfo"]["qId"] = dimension_id

        result = self._send_request("CreateDimension", self.app_handle, {"qProp": q_prop})

        if result and "qReturn" in result:
            return {
                "success": True,
                "dimension_id": result["qReturn"].get("qGenericId"),
                "title": title,
                "field_defs": field_defs,
            }

        return {"success": False, "error": "Failed to create dimension"}

    def update_dimension(
        self,
        dimension_id: str,
        title: str | None = None,
        field_def: str | list[str] | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        grouping: str | None = None,
    ) -> dict[str, Any]:
        """Update a master dimension in the app."""

        if not self.app_handle:
            raise ConnectionError("Not connected to an app")

        q_prop: dict[str, Any] = {
            "qInfo": {
                "qType": "dimension",
                "qId": dimension_id,
            },
            "qDim": {},
            "qMetaDef": {},
        }

        if field_def is not None:
            if isinstance(field_def, str):
                q_prop["qDim"]["qFieldDefs"] = [field_def]
                q_prop["qDim"]["qFieldLabels"] = [field_def]
            else:
                q_prop["qDim"]["qFieldDefs"] = field_def
                q_prop["qDim"]["qFieldLabels"] = field_def

        if grouping is not None:
            q_prop["qDim"]["qGrouping"] = grouping

        if title is not None:
            q_prop["qMetaDef"]["title"] = title

        if description is not None:
            q_prop["qMetaDef"]["description"] = description

        if tags is not None:
            q_prop["qMetaDef"]["tags"] = tags

        result = self._send_request("UpdateDimension", self.app_handle, {"qProp": q_prop})

        if result and "qReturn" in result:
            return {"success": True, "dimension_id": dimension_id}

        return {"success": False, "error": "Failed to update dimension"}

    def delete_dimension(self, dimension_id: str) -> dict[str, Any]:
        """Delete a master dimension from the app."""

        if not self.app_handle:
            raise ConnectionError("Not connected to an app")

        q_prop = {
            "qInfo": {
                "qType": "dimension",
                "qId": dimension_id,
            }
        }

        result = self._send_request("DestroyDimension", self.app_handle, {"qProp": q_prop})

        if result and "qReturn" in result:
            return {"success": True, "dimension_id": dimension_id}

        return {"success": False, "error": "Failed to delete dimension"}

    def create_object(
        self,
        object_type: str,
        title: str,
        properties: dict[str, Any] | None = None,
        object_id: str | None = None,
    ) -> dict[str, Any]:
        """Create a generic object (sheet, chart, table, etc.) in the app.

        Args:
            object_type: Type of object (sheet, barchart, linechart, table, etc.)
            title: Object title
            properties: Additional object-specific properties
            object_id: Optional custom ID

        Returns:
            Dict with created object info
        """
        if not self.app_handle:
            raise ConnectionError("Not connected to an app")

        q_prop = {
            "qInfo": {"qType": object_type},
            "qMetaDef": {"title": title},
        }

        if properties:
            q_prop.update(properties)

        if object_id:
            q_prop["qInfo"]["qId"] = object_id

        result = self._send_request("CreateObject", self.app_handle, {"qProp": q_prop})

        if result and "qReturn" in result:
            return {
                "success": True,
                "object_id": result["qReturn"].get("qGenericId"),
                "object_type": object_type,
                "title": title,
            }

        return {"success": False, "error": "Failed to create object"}

    def create_sheet(
        self,
        title: str,
        description: str = "",
        sheet_id: str | None = None,
    ) -> dict[str, Any]:
        """Create a new sheet in the app.

        Args:
            title: Sheet title
            description: Optional description
            sheet_id: Optional custom ID

        Returns:
            Dict with created sheet info
        """
        properties = {
            "rank": 0,
            "columns": 24,
            "rows": 12,
        }

        if description:
            properties["qMetaDef"]["description"] = description

        return self.create_object("sheet", title, properties, sheet_id)

    def add_object_to_sheet(
        self,
        sheet_id: str,
        object_id: str,
        col: int = 0,
        row: int = 0,
        colspan: int = 6,
        rowspan: int = 6,
    ) -> dict[str, Any]:
        """Place an existing object onto a specific sheet."""

        if not self.app_handle:
            raise ConnectionError("Not connected to an app")

        sheet_result = self._send_request("GetObject", self.app_handle, [sheet_id])
        if not sheet_result or "qReturn" not in sheet_result:
            return {"success": False, "error": f"Sheet {sheet_id} not found"}

        sheet_handle = sheet_result["qReturn"].get("qHandle")
        layout_result = self._send_request("GetLayout", sheet_handle)
        layout = layout_result.get("qLayout", layout_result) if layout_result else {}
        child_list = layout.get("qChildList", {}).get("qItems", [])

        # Prevent duplicate placements
        if any(item.get("qInfo", {}).get("qId") == object_id for item in child_list):
            return {"success": False, "error": "Object already on sheet"}

        object_info_result = self._send_request("GetObject", self.app_handle, [object_id])
        if not object_info_result or "qReturn" not in object_info_result:
            return {"success": False, "error": f"Object {object_id} not found"}

        object_info = object_info_result["qReturn"].get("qInfo", {})
        new_item = {
            "qInfo": object_info,
            "qData": {
                "col": col,
                "row": row,
                "colspan": colspan,
                "rowspan": rowspan,
            },
        }

        child_list.append(new_item)
        patches = [
            {
                "qPath": "/qChildList/qItems",
                "qOp": "replace",
                "qValue": json.dumps(child_list),
            }
        ]

        self._send_request("ApplyPatches", sheet_handle, {"qPatches": patches})

        return {
            "success": True,
            "sheet_id": sheet_id,
            "object_id": object_id,
            "col": col,
            "row": row,
            "colspan": colspan,
            "rowspan": rowspan,
        }

    def get_sheet_layout(self, sheet_id: str) -> dict[str, Any]:
        """Read the current sheet grid layout and child object positions."""

        if not self.app_handle:
            raise ConnectionError("Not connected to an app")

        sheet_result = self._send_request("GetObject", self.app_handle, [sheet_id])
        if not sheet_result or "qReturn" not in sheet_result:
            return {"success": False, "error": f"Sheet {sheet_id} not found"}

        sheet_handle = sheet_result["qReturn"].get("qHandle")
        layout_result = self._send_request("GetLayout", sheet_handle)
        layout = layout_result.get("qLayout", layout_result) if layout_result else {}
        q_meta = layout.get("qMeta", {})
        child_items = layout.get("qChildList", {}).get("qItems", [])

        objects = []
        for index, item in enumerate(child_items):
            q_info = item.get("qInfo", {})
            q_data = item.get("qData", {})
            object_id = q_info.get("qId", "")

            obj_data = {
                "object_id": object_id,
                "object_type": q_info.get("qType", ""),
                "column": q_data.get("col", 0),
                "row": q_data.get("row", 0),
                "colspan": q_data.get("colspan", 0),
                "rowspan": q_data.get("rowspan", 0),
                "index": index,
            }

            if object_id:
                try:
                    object_result = self._send_request("GetObject", self.app_handle, [object_id])
                    if object_result and "qReturn" in object_result:
                        object_handle = object_result["qReturn"].get("qHandle")
                        object_layout_result = self._send_request("GetLayout", object_handle)
                        object_layout = (
                            object_layout_result.get("qLayout", object_layout_result)
                            if object_layout_result else {}
                        )
                        if "title" in object_layout:
                            obj_data["title"] = object_layout["title"]
                        if "subtitle" in object_layout:
                            obj_data["subtitle"] = object_layout["subtitle"]
                except Exception as exc:
                    print(f"Warning: Could not enrich layout info for object {object_id}: {exc}")

            objects.append(obj_data)

        return {
            "success": True,
            "sheet_id": sheet_id,
            "sheet_title": q_meta.get("title", ""),
            "description": q_meta.get("description", ""),
            "rank": layout.get("rank", 0),
            "columns": layout.get("columns", 24),
            "rows": layout.get("rows", 12),
            "objects": objects,
            "object_count": len(objects),
        }

    def reposition_sheet_object(
        self,
        sheet_id: str,
        object_id: str,
        col: int,
        row: int,
        colspan: int,
        rowspan: int,
    ) -> dict[str, Any]:
        """Move or resize an existing object placement on a sheet."""

        if not self.app_handle:
            raise ConnectionError("Not connected to an app")

        sheet_result = self._send_request("GetObject", self.app_handle, [sheet_id])
        if not sheet_result or "qReturn" not in sheet_result:
            return {"success": False, "error": f"Sheet {sheet_id} not found"}

        sheet_handle = sheet_result["qReturn"].get("qHandle")
        layout_result = self._send_request("GetLayout", sheet_handle)
        layout = layout_result.get("qLayout", layout_result) if layout_result else {}
        child_items = layout.get("qChildList", {}).get("qItems", [])

        updated = False
        previous_position = None

        for item in child_items:
            q_info = item.get("qInfo", {})
            if q_info.get("qId") != object_id:
                continue

            q_data = item.setdefault("qData", {})
            previous_position = {
                "col": q_data.get("col", 0),
                "row": q_data.get("row", 0),
                "colspan": q_data.get("colspan", 0),
                "rowspan": q_data.get("rowspan", 0),
            }
            q_data["col"] = col
            q_data["row"] = row
            q_data["colspan"] = colspan
            q_data["rowspan"] = rowspan
            updated = True
            break

        if not updated:
            return {
                "success": False,
                "error": f"Object {object_id} is not placed on sheet {sheet_id}",
            }

        patches = [
            {
                "qPath": "/qChildList/qItems",
                "qOp": "replace",
                "qValue": json.dumps(child_items),
            }
        ]
        self._send_request("ApplyPatches", sheet_handle, {"qPatches": patches})

        return {
            "success": True,
            "sheet_id": sheet_id,
            "object_id": object_id,
            "previous_position": previous_position,
            "col": col,
            "row": row,
            "colspan": colspan,
            "rowspan": rowspan,
        }

    def set_script(self, script: str) -> dict[str, Any]:
        """Set the data load script for the app.

        Args:
            script: Complete load script content

        Returns:
            Dict with success status
        """
        if not self.app_handle:
            raise ConnectionError("Not connected to an app")

        self._send_request("SetScript", self.app_handle, {"qScript": script})

        return {"success": True, "script_length": len(script)}

    def save_app(self) -> dict[str, Any]:
        """Save the app.

        Returns:
            Dict with success status
        """
        if not self.app_handle:
            raise ConnectionError("Not connected to an app")

        self._send_request("Save", self.app_handle)

        return {"success": True}

    def do_reload(self) -> dict[str, Any]:
        """Reload the app data.

        Returns:
            Dict with success status
        """
        if not self.app_handle:
            raise ConnectionError("Not connected to an app")

        result = self._send_request("DoReload", self.app_handle)

        if result and "qReturn" in result:
            return {"success": result["qReturn"]}

        return {"success": False}

    def _send_request(self, method: str, handle: int = -1, params: Any | None = None) -> dict[str, Any]:
        """Send JSON-RPC request and wait for response"""
        if not self.ws:
            raise ConnectionError("WebSocket is not connected")

        self.request_id += 1

        # Handle params based on method type
        if method == "CreateSessionObject" and isinstance(params, list):
            # CreateSessionObject expects array params
            request = {
                "jsonrpc": "2.0",
                "id": self.request_id,
                "method": method,
                "handle": handle,
                "params": params,
            }
        elif method == "GetObject" and isinstance(params, list):
            # GetObject expects array params directly
            request = {
                "jsonrpc": "2.0",
                "id": self.request_id,
                "method": method,
                "handle": handle,
                "params": params,
            }
        elif method == "OpenDoc" and isinstance(params, dict) and "qDocName" in params:
            # OpenDoc expects array with just the doc name
            request = {
                "jsonrpc": "2.0",
                "id": self.request_id,
                "method": method,
                "handle": handle,
                "params": [params["qDocName"]],
            }
        else:
            request = {
                "jsonrpc": "2.0",
                "id": self.request_id,
                "method": method,
                "handle": handle,
                "params": params if params is not None else {},
            }

        # Send request
        request_json = json.dumps(request)
        self.ws.send(request_json)

        # Set receive timeout
        if hasattr(self.ws, "sock") and self.ws.sock:
            self.ws.sock.settimeout(self.recv_timeout)

        # Wait for response
        while True:
            response_str = self.ws.recv()
            response = json.loads(response_str)

            # Skip connection messages
            if response.get("method") == "OnConnected":
                continue

            # Check for errors
            if "error" in response:
                error = response["error"]
                raise Exception(f"Engine API Error: {error.get('message', 'Unknown error')}")

            # Check if this is our response
            if response.get("id") == self.request_id:
                return response.get("result", {})


def test_connection():
    """Test function to verify Qlik connection and measure retrieval"""
    client = QlikClient()

    # Test with a sample app ID (replace with actual app ID)
    test_app_id = "12345678-abcd-1234-efgh-123456789abc"

    try:
        if client.connect(test_app_id):
            print("\n✅ Connection successful!")

            # Get measures
            result = client.get_measures(include_expression=True, include_tags=True)

            print(f"\n📊 Found {result['count']} measures:")
            for measure in result["measures"][:5]:  # Show first 5
                print(f"  - {measure['title']} (ID: {measure['id']})")
                if measure.get("expression"):
                    print(f"    Expression: {measure['expression'][:50]}...")

            client.disconnect()
            return True
        print("❌ Connection failed")
        return False

    except Exception as e:
        print(f"❌ Test failed: {e}")
        client.disconnect()
        return False


if __name__ == "__main__":
    # Run test when module is executed directly
    test_connection()

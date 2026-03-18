# Qlik MCP Server

![Python](https://img.shields.io/badge/python-3.10%2B-blue) ![License](https://img.shields.io/badge/license-MIT-green) ![MCP](https://img.shields.io/badge/MCP-compatible-brightgreen)

## Overview

The Qlik MCP Server exposes Model Context Protocol tools so AI assistants (Claude, Cursor, VS Code, etc.) can explore Qlik Sense apps without a GUI. It connects directly to the Qlik Engine over secured WebSockets, resolves master items, inspects VizlibContainer layouts, analyzes scripts, and exposes object-level metadata through 9+ MCP tools.

- 🔌 Certificate-backed WebSocket access with configurable timeouts
- 📚 Catalogs apps, sheets, objects, measures, variables, dimensions, scripts, and data-lineage
- 🧠 Adds script analysis, binary-load detection, and master-item resolution for deterministic tooling
- 🧪 Built-in FastMCP server offers Pydantic validation, automatic schemas, and extensive error handling
- 🚀 Designed for AI-first workflows: natural-language MCP clients can now inspect or modify dashboards

## Installation

### 1. Quick Start

```bash
git clone https://github.com/arthurfantaci/qlik-mcp-server.git
cd qlik-mcp-server
curl -LsSf https://astral.sh/uv/install.sh | sh  # install UV if needed
uv sync  # create the virtual environment and install dependencies
```

### 2. Configuration

```bash
cp .env.example .env
# edit .env with your QLIK_* values (URL, cert paths, user directory)
```

Put the Qlik certificates into `certs/`:

```
certs/
├── root.pem
├── client.pem
└── client_key.pem
```

Refer to [docs/CERTIFICATES.md](docs/CERTIFICATES.md) for exact requirements.

### 3. Testing & Quality

Run the unit/integration suites via UV:

```bash
uv run pytest         # run all tests
uv run pytest -m unit # fast unit tests without a Qlik server
uv run pytest -m integration # requires live Qlik connection
uv run ruff check     # lint checks
uv run ruff format    # fix formatting
```

## Additional Resources

### Tools

| Area | Description |
|------|-------------|
| Applications & Metadata | `list_qlik_applications`, `get_app_details` (coming soon) |
| Measures & Variables | `get_app_measures`, `get_app_variables` |
| Data Model | `get_app_fields`, `get_app_dimensions` |
| Dashboards | `get_app_sheets`, `get_sheet_objects`, `get_sheet_layout`, `add_object_to_sheet` |
| Scripts & Sources | `get_app_script`, `get_app_data_sources`, `set_script`, `reload_app` |

Refer to [docs/API_REFERENCE.md](docs/API_REFERENCE.md) for detailed payloads and examples.

### Documentation & Examples

- Certificate guide: [docs/CERTIFICATES.md](docs/CERTIFICATES.md)
- Script tool usage: [docs/SCRIPT_TOOL_USAGE.md](docs/SCRIPT_TOOL_USAGE.md)
- Troubleshooting: [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)
- Configuration samples: [examples/README.md](examples/README.md)

### Support & Security

- Keep certificates and `.env` files out of version control
- Use environment variables for all sensitive settings
- Need help? Review the Troubleshooting guide or check server logs for detailed errors

## License

MIT. Use in line with your Qlik Sense enterprise agreements.

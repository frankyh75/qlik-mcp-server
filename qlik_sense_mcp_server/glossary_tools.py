"""Qlik Cloud glossary helpers for MCP tooling."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

try:
    import httpx
except ModuleNotFoundError:
    class _HttpxMissing:
        class Client:
            def __init__(self, *args, **kwargs):
                raise RuntimeError("httpx not installed")
    httpx = _HttpxMissing

DEFAULT_HTTP_TIMEOUT = 10.0


def _unwrap_payload(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        if "glossaries" in payload:
            return payload["glossaries"]
        if "data" in payload:
            data = payload["data"]
            if isinstance(data, list):
                return data
        if "items" in payload:
            return payload["items"]
    return []


class QlikGlossaryClient:
    """Client for calling Qlik Cloud glossary REST APIs."""

    def __init__(
        self,
        server_url: str,
        client: Optional[httpx.Client] = None,
        timeout: float = DEFAULT_HTTP_TIMEOUT,
    ):
        self.base_url = server_url.rstrip("/")
        self.client = client or httpx.Client(timeout=timeout, headers={"Accept": "application/json"})

    def _build_url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def list_glossaries(self) -> List[Dict[str, Any]]:
        resp = self.client.get(self._build_url("/api/v1/glossaries"))
        resp.raise_for_status()
        return _unwrap_payload(resp.json())

    def list_terms(self, glossary_id: str) -> List[Dict[str, Any]]:
        resp = self.client.get(self._build_url(f"/api/v1/glossaries/{glossary_id}/entries"))
        resp.raise_for_status()
        return _unwrap_payload(resp.json())

    def create_term(
        self,
        glossary_id: str,
        term: str,
        definition: str,
        synonyms: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "term": term,
            "definition": definition,
        }
        if synonyms:
            payload["synonyms"] = synonyms
        resp = self.client.post(
            self._build_url(f"/api/v1/glossaries/{glossary_id}/entries"), json=payload
        )
        resp.raise_for_status()
        return resp.json()

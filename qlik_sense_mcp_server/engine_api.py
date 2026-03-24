"""Minimal stub for Qlik Engine API client used in tests."""
from __future__ import annotations

from typing import Any, Dict, List, Optional


class QlikEngineAPI:
    def open_doc_safe(self, app_id: str, no_data: bool = True) -> Dict[str, Any]:
        raise NotImplementedError

    def send_request(self, method: str, params: Optional[List[Any]] = None, handle: int = -1) -> Dict[str, Any]:
        raise NotImplementedError

    def close_doc(self, handle: int) -> None:
        raise NotImplementedError

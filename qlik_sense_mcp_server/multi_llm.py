"""Multi-LLM HTTP transport helper for MCP."""
from __future__ import annotations

from typing import Dict, List, Optional

try:
    import httpx
except ModuleNotFoundError:  # pragma: no cover - env-specific
    httpx = None


class MultiLLMTransport:
    """Dispatch prompts to multiple LLM endpoints via HTTP."""

    def __init__(
        self,
        endpoints: Dict[str, str],
        api_keys: Dict[str, str],
        timeout: float = 10.0,
        client: Optional["httpx.Client"] = None,
    ) -> None:
        if httpx is None and client is None:
            raise RuntimeError("httpx is required for MultiLLMTransport")
        self.endpoints = endpoints
        self.api_keys = api_keys
        self.client = client or httpx.Client(timeout=timeout, headers={"Content-Type": "application/json"})

    def dispatch(self, provider: str, model: str, prompt: str) -> Dict[str, str]:
        """Send the prompt to the configured provider endpoint."""
        url = self.endpoints.get(provider)
        if url is None:
            raise KeyError(f"Provider {provider} is unknown")
        api_key = self.api_keys.get(provider)
        headers = self.client.headers.copy()
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        response = self.client.post(url, json={"model": model, "prompt": prompt}, headers=headers)
        response.raise_for_status()
        data = response.json()
        return {"provider": provider, "text": data.get("text", "")}

    def broadcast(self, model: str, prompt: str) -> List[Dict[str, str]]:
        """Send the prompt to all configured providers and gather responses."""
        results = []
        for provider in self.endpoints:
            results.append(self.dispatch(provider, model, prompt))
        return results

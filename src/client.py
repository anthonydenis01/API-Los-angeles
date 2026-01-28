from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from src.config import Settings


@dataclass
class SignalClient:
    settings: Settings
    session: requests.Session
    logger: logging.Logger

    @classmethod
    def from_settings(cls, settings: Settings, logger: logging.Logger) -> "SignalClient":
        session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST"),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        if settings.headers:
            session.headers.update(settings.headers)

        cookies = _load_cookies(settings)
        if cookies:
            session.cookies.update(cookies)

        return cls(settings=settings, session=session, logger=logger)

    def fetch_json(self, endpoint: str, payload: dict[str, Any] | None) -> Any:
        url = self._resolve_url(endpoint)
        method = "POST" if payload is not None else "GET"
        self.logger.info("Fetching %s via %s", url, method)
        response = self.session.request(
            method=method,
            url=url,
            json=payload,
            timeout=self.settings.timeout_seconds,
        )
        if response.status_code >= 400:
            self.logger.error(
                "Request failed for %s with status %s: %s",
                url,
                response.status_code,
                response.text,
            )
            response.raise_for_status()
        try:
            return response.json()
        except json.JSONDecodeError as exc:
            raise ValueError(f"Response from {url} was not valid JSON") from exc

    def _resolve_url(self, endpoint: str) -> str:
        if endpoint.startswith("http"):
            return endpoint
        if not self.settings.base_url:
            raise ValueError("SIGNAL_BASE_URL is required for relative endpoints.")
        return urljoin(self.settings.base_url.rstrip("/") + "/", endpoint.lstrip("/"))


def _load_cookies(settings: Settings) -> dict[str, Any] | None:
    if settings.cookies_json:
        return settings.cookies_json
    if settings.cookies_path:
        path = Path(settings.cookies_path)
        if not path.exists():
            raise FileNotFoundError(f"Cookie file not found: {path}")
        return json.loads(path.read_text())
    return None

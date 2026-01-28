from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Iterable

from dotenv import load_dotenv


def _parse_json_env(name: str, default: dict[str, Any] | None = None) -> dict[str, Any] | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Environment variable {name} must be valid JSON.") from exc


def _parse_list_env(name: str, default: Iterable[str]) -> list[str]:
    value = os.getenv(name)
    if not value:
        return list(default)
    return [item.strip() for item in value.split(",") if item.strip()]


@dataclass(frozen=True)
class Settings:
    base_url: str
    weekly_volumes_url: str
    containers_at_terminal_url: str
    outgated_metrics_url: str
    berth_url: str
    weekly_volumes_payload: dict[str, Any] | None
    containers_at_terminal_payload: dict[str, Any] | None
    outgated_metrics_payload: dict[str, Any] | None
    berth_payload: dict[str, Any] | None
    headers: dict[str, Any] | None
    cookies_json: dict[str, Any] | None
    cookies_path: str | None
    timeout_seconds: int
    log_level: str
    database_url: str
    db_schema: str
    from_date: str | None
    to_date: str | None
    congested_buckets: list[str]
    slow_outgate_buckets: list[str]
    volume_pressure_high: float
    volume_pressure_low: float
    terminal_loaded_high: float
    terminal_empty_high: float
    outgate_slow_high: float
    berth_high_hours: float
    berth_top_n: int

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        base_url = os.getenv("SIGNAL_BASE_URL", "").strip()
        weekly_volumes_url = os.getenv("SIGNAL_WEEKLY_VOLUMES_URL", "").strip()
        containers_at_terminal_url = os.getenv("SIGNAL_CONTAINERS_AT_TERMINAL_URL", "").strip()
        outgated_metrics_url = os.getenv("SIGNAL_OUTGATED_METRICS_URL", "").strip()
        berth_url = os.getenv("SIGNAL_BERTH_URL", "").strip()
        missing = [
            name
            for name, value in {
                "SIGNAL_WEEKLY_VOLUMES_URL": weekly_volumes_url,
                "SIGNAL_CONTAINERS_AT_TERMINAL_URL": containers_at_terminal_url,
                "SIGNAL_OUTGATED_METRICS_URL": outgated_metrics_url,
                "SIGNAL_BERTH_URL": berth_url,
            }.items()
            if not value
        ]
        if missing:
            raise ValueError(f"Missing required endpoint env vars: {', '.join(missing)}")

        database_url = os.getenv("DATABASE_URL", "").strip()
        if not database_url:
            raise ValueError("DATABASE_URL must be set to write KPI tables.")

        return cls(
            base_url=base_url,
            weekly_volumes_url=weekly_volumes_url,
            containers_at_terminal_url=containers_at_terminal_url,
            outgated_metrics_url=outgated_metrics_url,
            berth_url=berth_url,
            weekly_volumes_payload=_parse_json_env("SIGNAL_WEEKLY_VOLUMES_PAYLOAD"),
            containers_at_terminal_payload=_parse_json_env("SIGNAL_CONTAINERS_AT_TERMINAL_PAYLOAD"),
            outgated_metrics_payload=_parse_json_env("SIGNAL_OUTGATED_METRICS_PAYLOAD"),
            berth_payload=_parse_json_env("SIGNAL_BERTH_PAYLOAD"),
            headers=_parse_json_env("SIGNAL_HEADERS_JSON"),
            cookies_json=_parse_json_env("SIGNAL_COOKIES_JSON"),
            cookies_path=os.getenv("SIGNAL_COOKIES_PATH"),
            timeout_seconds=int(os.getenv("SIGNAL_TIMEOUT_SECONDS", "30")),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            database_url=database_url,
            db_schema=os.getenv("DB_SCHEMA", "public"),
            from_date=os.getenv("FROM_DATE"),
            to_date=os.getenv("TO_DATE"),
            congested_buckets=_parse_list_env(
                "CONGESTED_BUCKETS", ["9-12 Days", "13+ Days"]
            ),
            slow_outgate_buckets=_parse_list_env(
                "SLOW_OUTGATE_BUCKETS", ["5-8 Days", "9-12 Days", "13+ Days"]
            ),
            volume_pressure_high=float(os.getenv("VOLUME_PRESSURE_HIGH", "1.15")),
            volume_pressure_low=float(os.getenv("VOLUME_PRESSURE_LOW", "0.90")),
            terminal_loaded_high=float(os.getenv("TERMINAL_LOADED_HIGH", "0.25")),
            terminal_empty_high=float(os.getenv("TERMINAL_EMPTY_HIGH", "0.50")),
            outgate_slow_high=float(os.getenv("OUTGATE_SLOW_HIGH", "0.40")),
            berth_high_hours=float(os.getenv("BERTH_HIGH_HOURS", "24")),
            berth_top_n=int(os.getenv("BERTH_TOP_N", "5")),
        )

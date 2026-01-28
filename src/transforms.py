from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable

import pandas as pd


class DataShapeError(ValueError):
    pass


def _extract_list(payload: Any, keys: Iterable[str]) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in keys:
            if key in payload and isinstance(payload[key], list):
                return payload[key]
    raise DataShapeError(f"Expected list payload with keys {list(keys)}")


def _get_required_key(item: dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in item and item[key] is not None:
            return item[key]
    raise DataShapeError(f"Missing required keys {list(keys)} in item {item}")


def _parse_date(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, (int, float)):
        return datetime.utcfromtimestamp(value).date().isoformat()
    if isinstance(value, str):
        return pd.to_datetime(value).date().isoformat()
    raise DataShapeError(f"Unable to parse date value: {value}")


def parse_weekly_volumes(payload: Any) -> pd.DataFrame:
    items = _extract_list(payload, ["weeklyVolumesComparison", "data", "items"])
    rows = []
    for item in items:
        week_start = _parse_date(
            _get_required_key(
                item,
                [
                    "weekStartDate",
                    "week_start_date",
                    "week",
                    "startDate",
                    "date",
                ],
            )
        )
        inbound_full = _get_required_key(
            item,
            [
                "inboundFullContainers",
                "inbound_full_containers",
                "inboundFullTeu",
                "inbound_full_teu",
                "inboundFullTEU",
            ],
        )
        rows.append({"week_start_date": week_start, "inbound_full_containers": inbound_full})
    df = pd.DataFrame(rows)
    if df.empty:
        raise DataShapeError("Weekly volume payload returned no rows.")
    df["inbound_full_containers"] = pd.to_numeric(
        df["inbound_full_containers"], errors="coerce"
    )
    if df["inbound_full_containers"].isna().any():
        raise DataShapeError("Weekly volume payload contained non-numeric inbound values.")
    return df


def parse_terminal_containers(payload: Any) -> pd.DataFrame:
    items = _extract_list(payload, ["ContainersAtTerminalData", "data", "items"])
    rows = []
    for item in items:
        load_type = _get_required_key(item, ["loadType", "load_type", "status"]).strip()
        bucket = _get_required_key(item, ["bucket", "agingBucket", "ageBucket", "aging_bucket"]).strip()
        containers = _get_required_key(item, ["containers", "containerCount", "value", "count"])
        rows.append(
            {
                "load_type": load_type,
                "bucket": bucket,
                "containers": containers,
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        raise DataShapeError("Terminal container payload returned no rows.")
    df["containers"] = pd.to_numeric(df["containers"], errors="coerce")
    if df["containers"].isna().any():
        raise DataShapeError("Terminal container payload contained non-numeric values.")
    return df


def parse_outgate_metrics(payload: Any) -> pd.DataFrame:
    items = _extract_list(payload, ["FetchOutgatedContainerMetricsData", "data", "items"])
    rows = []
    for item in items:
        status = _get_required_key(item, ["status", "containerStatus", "loadType"]).strip()
        bucket = _get_required_key(item, ["bucket", "agingBucket", "ageBucket", "aging_bucket"]).strip()
        containers = _get_required_key(item, ["containers", "containerCount", "value", "count"])
        rows.append(
            {
                "status": status,
                "bucket": bucket,
                "containers": containers,
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        raise DataShapeError("Outgate metrics payload returned no rows.")
    df["containers"] = pd.to_numeric(df["containers"], errors="coerce")
    if df["containers"].isna().any():
        raise DataShapeError("Outgate metrics payload contained non-numeric values.")
    return df


def parse_berth_data(payload: Any) -> pd.DataFrame:
    items = _extract_list(payload, ["FetchQuickviewDashboardBerthData", "vessels", "data", "items"])
    rows = []
    for item in items:
        vessel = _get_required_key(item, ["vessel", "vesselName", "name"]).strip()
        hours = _get_required_key(
            item,
            ["timeAtBerthHours", "hoursAtBerth", "time_at_berth_hours", "hours"],
        )
        terminal = item.get("terminal") or item.get("terminalName")
        rows.append(
            {
                "vessel": vessel,
                "time_at_berth_hours": hours,
                "terminal": terminal,
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        raise DataShapeError("Berth payload returned no rows.")
    df["time_at_berth_hours"] = pd.to_numeric(df["time_at_berth_hours"], errors="coerce")
    if df["time_at_berth_hours"].isna().any():
        raise DataShapeError("Berth payload contained non-numeric time values.")
    return df

from __future__ import annotations

from datetime import datetime

import pandas as pd


def build_weekly_volume_pressure(
    weekly_df: pd.DataFrame,
    extraction_ts_utc: datetime,
    from_date: str | None,
    to_date: str | None,
    high_threshold: float,
    low_threshold: float,
) -> pd.DataFrame:
    df = weekly_df.copy()
    df["week_start_date"] = pd.to_datetime(df["week_start_date"])
    df = df.sort_values("week_start_date")
    df["rolling_4w_avg_teu"] = (
        df["inbound_full_containers"].rolling(window=4, min_periods=1).mean()
    )
    df["volume_pressure_index"] = df["inbound_full_containers"] / df["rolling_4w_avg_teu"]
    df["flag"] = "NORMAL"
    df.loc[df["volume_pressure_index"] >= high_threshold, "flag"] = "HIGH"
    df.loc[df["volume_pressure_index"] <= low_threshold, "flag"] = "LOW"
    df["extraction_ts_utc"] = extraction_ts_utc
    if from_date:
        df["from_date"] = from_date
    if to_date:
        df["to_date"] = to_date
    df["week_start_date"] = df["week_start_date"].dt.date.astype(str)
    return df


def build_terminal_congestion(
    terminal_df: pd.DataFrame,
    extraction_ts_utc: datetime,
    from_date: str | None,
    to_date: str | None,
    congested_buckets: list[str],
    loaded_high: float,
    empty_high: float,
) -> pd.DataFrame:
    df = terminal_df.copy()
    grouped = df.groupby("load_type", dropna=False)
    rows = []
    for load_type, subset in grouped:
        total = subset["containers"].sum()
        congested = subset[subset["bucket"].isin(congested_buckets)]["containers"].sum()
        congested_pct = congested / total if total else 0.0
        flag = "NORMAL"
        if load_type.lower() == "loaded" and congested_pct >= loaded_high:
            flag = "HIGH"
        if load_type.lower() == "empty" and congested_pct >= empty_high:
            flag = "HIGH"
        rows.append(
            {
                "load_type": load_type,
                "total_containers": total,
                "congested_containers": congested,
                "congested_pct": congested_pct,
                "flag": flag,
            }
        )
    result = pd.DataFrame(rows)
    if result.empty:
        raise ValueError("Terminal congestion KPI produced no rows.")
    result["extraction_ts_utc"] = extraction_ts_utc
    if from_date:
        result["from_date"] = from_date
    if to_date:
        result["to_date"] = to_date
    return result


def build_outgate_stress_by_status(
    outgate_df: pd.DataFrame,
    extraction_ts_utc: datetime,
    from_date: str | None,
    to_date: str | None,
    slow_buckets: list[str],
    slow_high: float,
) -> pd.DataFrame:
    df = outgate_df.copy()
    grouped = df.groupby("status", dropna=False)
    rows = []
    for status, subset in grouped:
        total = subset["containers"].sum()
        slow = subset[subset["bucket"].isin(slow_buckets)]["containers"].sum()
        slow_pct = slow / total if total else 0.0
        flag = "HIGH" if slow_pct >= slow_high else "NORMAL"
        rows.append(
            {
                "status": status,
                "total_containers": total,
                "slow_containers": slow,
                "slow_pct": slow_pct,
                "flag": flag,
            }
        )
    result = pd.DataFrame(rows)
    if result.empty:
        raise ValueError("Outgate stress KPI produced no rows.")
    result["extraction_ts_utc"] = extraction_ts_utc
    if from_date:
        result["from_date"] = from_date
    if to_date:
        result["to_date"] = to_date
    return result


def build_berth_snapshot(
    berth_df: pd.DataFrame,
    extraction_ts_utc: datetime,
    from_date: str | None,
    to_date: str | None,
    berth_high_hours: float,
    top_n: int,
) -> pd.DataFrame:
    df = berth_df.copy()
    avg_time = df["time_at_berth_hours"].mean()
    flag = "HIGH" if avg_time >= berth_high_hours else "NORMAL"
    summary = pd.DataFrame(
        [
            {
                "record_type": "summary",
                "vessel": None,
                "terminal": None,
                "time_at_berth_hours": None,
                "avg_time_at_berth_hours": avg_time,
                "flag": flag,
            }
        ]
    )

    vessels = (
        df.sort_values("time_at_berth_hours", ascending=False)
        .head(top_n)
        .assign(
            record_type="vessel",
            avg_time_at_berth_hours=avg_time,
            flag=flag,
        )
    )

    result = pd.concat([summary, vessels], ignore_index=True)
    result["extraction_ts_utc"] = extraction_ts_utc
    if from_date:
        result["from_date"] = from_date
    if to_date:
        result["to_date"] = to_date
    return result


def build_health_summary(
    volume_df: pd.DataFrame,
    terminal_df: pd.DataFrame,
    outgate_df: pd.DataFrame,
    berth_df: pd.DataFrame,
    extraction_ts_utc: datetime,
) -> pd.DataFrame:
    latest_volume = volume_df.sort_values("week_start_date").iloc[-1]
    loaded_flag = _flag_for_load_type(terminal_df, "loaded")
    empty_flag = _flag_for_load_type(terminal_df, "empty")
    high_statuses = outgate_df[outgate_df["flag"] == "HIGH"]["status"].tolist()
    berth_flag = berth_df[berth_df["record_type"] == "summary"]["flag"].iloc[0]

    row = {
        "volume_pressure_flag": latest_volume["flag"],
        "terminal_congestion_loaded_flag": loaded_flag,
        "terminal_congestion_empty_flag": empty_flag,
        "outgate_stress_high_statuses": ", ".join(high_statuses) if high_statuses else "NONE",
        "berth_flag": berth_flag,
        "extraction_ts_utc": extraction_ts_utc,
    }
    return pd.DataFrame([row])


def _flag_for_load_type(df: pd.DataFrame, load_type: str) -> str:
    subset = df[df["load_type"].str.lower() == load_type]
    if subset.empty:
        return "UNKNOWN"
    return subset.iloc[0]["flag"]

from __future__ import annotations

import logging
from datetime import datetime, timezone

from src.client import SignalClient
from src.config import Settings
from src.db import create_db_engine, write_tables
from src.kpis import (
    build_berth_snapshot,
    build_health_summary,
    build_outgate_stress_by_status,
    build_terminal_congestion,
    build_weekly_volume_pressure,
)
from src.transforms import (
    parse_berth_data,
    parse_outgate_metrics,
    parse_terminal_containers,
    parse_weekly_volumes,
)


def configure_logging(level: str) -> logging.Logger:
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    return logging.getLogger("signal_pipeline")


def run_pipeline() -> None:
    settings = Settings.from_env()
    logger = configure_logging(settings.log_level)
    logger.info("Starting Signal KPI pipeline")

    client = SignalClient.from_settings(settings, logger)
    extraction_ts_utc = datetime.now(timezone.utc)

    weekly_payload = client.fetch_json(
        settings.weekly_volumes_url, settings.weekly_volumes_payload
    )
    terminal_payload = client.fetch_json(
        settings.containers_at_terminal_url, settings.containers_at_terminal_payload
    )
    outgate_payload = client.fetch_json(
        settings.outgated_metrics_url, settings.outgated_metrics_payload
    )
    berth_payload = client.fetch_json(settings.berth_url, settings.berth_payload)

    weekly_df = parse_weekly_volumes(weekly_payload)
    terminal_df = parse_terminal_containers(terminal_payload)
    outgate_df = parse_outgate_metrics(outgate_payload)
    berth_df = parse_berth_data(berth_payload)

    kpi_weekly = build_weekly_volume_pressure(
        weekly_df,
        extraction_ts_utc,
        settings.from_date,
        settings.to_date,
        settings.volume_pressure_high,
        settings.volume_pressure_low,
    )
    kpi_terminal = build_terminal_congestion(
        terminal_df,
        extraction_ts_utc,
        settings.from_date,
        settings.to_date,
        settings.congested_buckets,
        settings.terminal_loaded_high,
        settings.terminal_empty_high,
    )
    kpi_outgate = build_outgate_stress_by_status(
        outgate_df,
        extraction_ts_utc,
        settings.from_date,
        settings.to_date,
        settings.slow_outgate_buckets,
        settings.outgate_slow_high,
    )
    kpi_berth = build_berth_snapshot(
        berth_df,
        extraction_ts_utc,
        settings.from_date,
        settings.to_date,
        settings.berth_high_hours,
        settings.berth_top_n,
    )
    kpi_health = build_health_summary(
        kpi_weekly, kpi_terminal, kpi_outgate, kpi_berth, extraction_ts_utc
    )

    engine = create_db_engine(settings.database_url)
    tables = {
        "kpi_weekly_volume_pressure": kpi_weekly,
        "kpi_terminal_congestion": kpi_terminal,
        "kpi_outgate_stress_by_status": kpi_outgate,
        "kpi_berth_snapshot": kpi_berth,
        "kpi_health_summary": kpi_health,
    }
    write_tables(engine, settings.db_schema, tables)
    logger.info("KPI tables written to schema %s", settings.db_schema)


if __name__ == "__main__":
    run_pipeline()

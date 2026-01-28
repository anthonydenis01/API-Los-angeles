from __future__ import annotations

from typing import Mapping

import pandas as pd
from sqlalchemy import create_engine


def create_db_engine(database_url: str):
    return create_engine(database_url)


def write_tables(
    engine,
    schema: str,
    tables: Mapping[str, pd.DataFrame],
) -> None:
    for table_name, df in tables.items():
        df.to_sql(table_name, engine, schema=schema, if_exists="replace", index=False)

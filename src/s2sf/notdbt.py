# Not dbt
import pkgutil
from typing import Optional

from .seafowl import SEAFOWL_SCHEMA, emit_value, query_seafowl

TO_BUILD = [
    "all_datasets.sql",
    "daily_diff.sql",
    "monthly_diff.sql",
    "weekly_diff.sql",
]


def build_models(seafowl: str, access_token: Optional[str]) -> None:
    for model in TO_BUILD:
        print(f"Building {model}")
        table_name = model.split(".")[0]

        model_p = pkgutil.get_data("s2sf.sql", model)
        if not model_p:
            raise ValueError(f"Model file {model} not found!")
        model_sql = model_p.decode()

        table_exists = query_seafowl(
            seafowl,
            f"SELECT 1 AS exists FROM information_schema.tables "
            f"WHERE table_schema = {emit_value(SEAFOWL_SCHEMA)} AND table_name = {emit_value(table_name)}",
        )

        if table_exists:
            query = f"DROP TABLE {SEAFOWL_SCHEMA}.{table_name}; "
        else:
            query = ""

        query += (
            f"\nCREATE TABLE {SEAFOWL_SCHEMA}.{table_name} AS (\n" + model_sql + ");"
        )

        query_seafowl(seafowl, query, access_token)
        print(f"Building {model} done")

    print("All done")

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import duckdb

from backend.app.config import Settings


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class DuckDBManager:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def connect(self) -> duckdb.DuckDBPyConnection:
        connection = duckdb.connect(database=self.settings.duckdb_database)
        try:
            connection.execute(f"SET statement_timeout='{self.settings.query_timeout_seconds}s'")
        except duckdb.Error:
            pass
        return connection

    @contextmanager
    def session(self) -> Iterator[duckdb.DuckDBPyConnection]:
        connection = self.connect()
        try:
            yield connection
        finally:
            connection.close()

    def register_csv_view(
        self,
        connection: duckdb.DuckDBPyConnection,
        csv_path: Path,
        view_name: str = "dataset_view",
    ) -> None:
        safe_path = quote_literal(str(csv_path))
        connection.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW {quote_identifier(view_name)} AS
            SELECT * FROM read_csv_auto(
                {safe_path},
                SAMPLE_SIZE=-1,
                HEADER=TRUE
            )
            """
        )

    def ping(self) -> bool:
        with self.session() as connection:
            result = connection.execute("SELECT 1").fetchone()
            return bool(result and result[0] == 1)

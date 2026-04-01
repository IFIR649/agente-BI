from __future__ import annotations

import re
import sqlite3
import threading
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from typing import Callable

from backend.app.models.telemetry import FXRateRecord


_BANXICO_SOURCE = "Banxico FIX"


class BanxicoFxResolver:
    def __init__(
        self,
        *,
        db_path: Path,
        service_url: str,
        timeout_seconds: int = 10,
        fetcher: Callable[[date], str] | None = None,
    ) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.service_url = service_url
        self.timeout_seconds = timeout_seconds
        self._fetcher = fetcher or self._fetch_html
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _init_db(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS fx_rates (
                    fx_date TEXT PRIMARY KEY,
                    usd_to_mxn_rate REAL NOT NULL,
                    source TEXT NOT NULL,
                    fetched_at TEXT NOT NULL
                )
                """
            )
            connection.execute("CREATE INDEX IF NOT EXISTS idx_fx_rates_fx_date ON fx_rates(fx_date)")
            connection.commit()

    def resolve(self, requested_date: date) -> FXRateRecord:
        cached = self._get_cached_exact(requested_date)
        if cached is not None:
            return cached

        prior_cached = self._get_latest_on_or_before(requested_date)
        if prior_cached is not None:
            return prior_cached

        for offset in range(0, 8):
            candidate = requested_date - timedelta(days=offset)
            cached_candidate = self._get_cached_exact(candidate)
            if cached_candidate is not None:
                return cached_candidate

            try:
                html = self._fetcher(candidate)
            except Exception:
                continue

            parsed = self._parse_fix_html(html)
            if parsed is None:
                continue

            fx_date, usd_to_mxn_rate = parsed
            record = FXRateRecord(
                fx_date=fx_date,
                usd_to_mxn_rate=usd_to_mxn_rate,
                fx_source=_BANXICO_SOURCE,
            )
            self._persist(record)
            return record

        prior = self._get_latest_on_or_before(requested_date)
        if prior is not None:
            return prior

        return FXRateRecord(
            fx_date=requested_date,
            usd_to_mxn_rate=0.0,
            fx_source=_BANXICO_SOURCE,
        )

    def _fetch_html(self, requested_date: date) -> str:
        formatted = requested_date.strftime("%d/%m/%Y")
        payload = urllib.parse.urlencode(
            {
                "idioma": "sp",
                "fechaInicial": formatted,
                "fechaFinal": formatted,
                "salida": "HTML",
            }
        ).encode()
        request = urllib.request.Request(
            self.service_url,
            data=payload,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
            return response.read().decode("utf-8", errors="ignore")

    def _parse_fix_html(self, html: str) -> tuple[date, float] | None:
        # Banxico renders the FIX determination as the first numeric cell after the date.
        cells = re.findall(r'<td class="renglon(?:Par|Non)">\s*([^<]+?)\s*</td>', html, flags=re.IGNORECASE)
        if len(cells) < 2:
            return None

        raw_date = unescape(cells[0]).strip()
        raw_rate = unescape(cells[1]).strip().replace(",", "")
        try:
            fx_date = datetime.strptime(raw_date, "%d/%m/%Y").date()
            usd_to_mxn_rate = float(raw_rate)
        except ValueError:
            return None
        return fx_date, usd_to_mxn_rate

    def _get_cached_exact(self, requested_date: date) -> FXRateRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT fx_date, usd_to_mxn_rate, source FROM fx_rates WHERE fx_date = ?",
                (requested_date.isoformat(),),
            ).fetchone()
        return self._row_to_record(row)

    def _get_latest_on_or_before(self, requested_date: date) -> FXRateRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT fx_date, usd_to_mxn_rate, source
                FROM fx_rates
                WHERE fx_date <= ?
                ORDER BY fx_date DESC
                LIMIT 1
                """,
                (requested_date.isoformat(),),
            ).fetchone()
        return self._row_to_record(row)

    def _persist(self, record: FXRateRecord) -> None:
        with self._lock:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO fx_rates (fx_date, usd_to_mxn_rate, source, fetched_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(fx_date) DO UPDATE SET
                        usd_to_mxn_rate = excluded.usd_to_mxn_rate,
                        source = excluded.source,
                        fetched_at = excluded.fetched_at
                    """,
                    (
                        record.fx_date.isoformat(),
                        record.usd_to_mxn_rate,
                        record.fx_source,
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )
                connection.commit()

    def _row_to_record(self, row: sqlite3.Row | None) -> FXRateRecord | None:
        if row is None:
            return None
        return FXRateRecord(
            fx_date=date.fromisoformat(row["fx_date"]),
            usd_to_mxn_rate=float(row["usd_to_mxn_rate"] or 0.0),
            fx_source=row["source"] or _BANXICO_SOURCE,
        )

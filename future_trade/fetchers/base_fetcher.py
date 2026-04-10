"""
Base fetcher with common functionality for future_trade API calls.

Includes rate limiting (0.5s per call), retry logic with exponential backoff,
and database write operations. Also handles date conversion and NaN/None cleaning
for safe DB writes.
"""
import math
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Callable, Optional

import pandas as pd
import tushare as ts
from rich.console import Console

from config.postgres_config import get_settings
from db import get_connection, get_engine

console = Console()


class BaseFetcher(ABC):
    """Base class for all future_trade data fetchers."""

    _last_call_time: float = 0.0
    _rate_limit_wait: float = 12.0  # Fallback wait when rate limited (s)

    def __init__(self):
        self.settings = get_settings()
        # Initialize Pro API using provided token
        self.pro = ts.pro_api(self.settings.tushare_token)
        # API interval can be overridden by settings; default to 0.5s per call
        self.api_interval = getattr(self.settings, "api_interval", 0.5)

    @property
    @abstractmethod
    def table_name(self) -> str:
        """Name of the target database table."""
        pass

    @property
    @abstractmethod
    def api_name(self) -> str:
        """Name of the Tushare API to call."""
        pass

    @property
    def fields(self) -> str | None:
        return None

    @property
    def date_columns(self) -> list[str]:
        return []

    def _rate_limit(self) -> None:
        """Ensure API calls respect rate limits."""
        elapsed = time.time() - BaseFetcher._last_call_time
        if elapsed < self.api_interval:
            time.sleep(self.api_interval - elapsed)
        BaseFetcher._last_call_time = time.time()

    def _call_api(self, **kwargs) -> pd.DataFrame:
        """Call Tushare API with rate limiting and smart retry."""
        max_retries = 5
        retry_count = 0

        while retry_count < max_retries:
            self._rate_limit()
            api_func = getattr(self.pro, self.api_name)
            if self.fields:
                kwargs["fields"] = self.fields
            try:
                df = api_func(**kwargs)
                return df if df is not None else pd.DataFrame()
            except Exception as e:
                error_msg = str(e)
                # Retry on rate-limiting related messages
                if "每分钟最多访问" in error_msg or "rate" in error_msg.lower():
                    retry_count += 1
                    if retry_count < max_retries:
                        time.sleep(self._rate_limit_wait)
                        continue
                # Exponential backoff for other errors
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = min(2 ** retry_count, 30)
                    time.sleep(wait_time)
                    continue
                console.print(f"[red]API Error ({self.api_name}): {e}[/red]")
                raise
        return pd.DataFrame()

    def fetch_by_date(self, trade_date: str | None = None, start_date: str | None = None, end_date: str | None = None, **extra_kwargs) -> pd.DataFrame:
        """Fetch data by date or date range."""
        kwargs = {**extra_kwargs}
        if trade_date:
            kwargs["trade_date"] = trade_date
        if start_date:
            kwargs["start_date"] = start_date
        if end_date:
            kwargs["end_date"] = end_date
        df = self._call_api(**kwargs)
        if not df.empty and self.date_columns:
            df = self._convert_dates(df)
        return df

    def fetch_by_code(self, ts_code: str, start_date: str | None = None, end_date: str | None = None, **extra_kwargs) -> pd.DataFrame:
        """Fetch data for a specific contract code."""
        kwargs = {"ts_code": ts_code, **extra_kwargs}
        if start_date:
            kwargs["start_date"] = start_date
        if end_date:
            kwargs["end_date"] = end_date
        df = self._call_api(**kwargs)
        if not df.empty and self.date_columns:
            df = self._convert_dates(df)
        return df

    def fetch_by_exchange(self, exchange: str, **kwargs) -> pd.DataFrame:
        """Fetch data for a specific exchange."""
        df = self._call_api(exchange=exchange, **kwargs)
        if not df.empty and self.date_columns:
            df = self._convert_dates(df)
        return df

    def fetch_all_by_date_range(self, start_date: str, end_date: str, progress_callback: Optional[Callable[[int, int, str], None]] = None, skip_weekends: bool = True, **extra_kwargs) -> pd.DataFrame:
        """Fetch data day-by-day within a date range."""
        start = datetime.strptime(start_date, "%Y%m%d")
        end = datetime.strptime(end_date, "%Y%m%d")
        all_data: list[pd.DataFrame] = []
        current = start
        total_days = (end - start).days + 1
        processed = 0

        while current <= end:
            if skip_weekends and current.weekday() >= 5:
                current += timedelta(days=1)
                processed += 1
                continue
            date_str = current.strftime("%Y%m%d")
            try:
                df = self.fetch_by_date(trade_date=date_str, **extra_kwargs)
                if not df.empty:
                    all_data.append(df)
            except Exception as e:
                console.print(f"[yellow]Warning: Failed to fetch {date_str}: {e}[/yellow]")
            processed += 1
            if progress_callback:
                progress_callback(processed, total_days, date_str)
            current += timedelta(days=1)

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()

    def save_to_db(self, df: pd.DataFrame, if_exists: str = "append") -> int:
        """Save DataFrame to database table."""
        if df.empty:
            return 0
        df = self._clean_for_db(df)
        engine = get_engine()
        df.to_sql(self.table_name, engine, if_exists=if_exists, index=False, method="multi")
        return len(df)

    def upsert_to_db(self, df: pd.DataFrame, conflict_columns: list[str]) -> int:
        """Upsert DataFrame into database (insert or update on conflict)."""
        if df.empty:
            return 0
        columns = df.columns.tolist()
        col_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        conflict_str = ", ".join(conflict_columns)
        update_set_parts = [f"{col} = EXCLUDED.{col}" for col in columns if col not in conflict_columns]
        update_str = ", ".join(update_set_parts) if update_set_parts else None
        conflict_action = f"DO UPDATE SET {update_str}" if update_str else "DO NOTHING"
        sql = f"INSERT INTO {self.table_name} ({col_str}) VALUES ({placeholders}) ON CONFLICT ({conflict_str}) {conflict_action}"
        df = self._clean_for_db(df)
        with get_connection() as conn:
            with conn.cursor() as cur:
                rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
                cur.executemany(sql, rows)
        return len(df)

    def get_latest_date(self, date_column: str = "trade_date") -> str | None:
        """Get the latest trade_date in the table."""
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT MAX({date_column}) FROM {self.table_name}")
                result = cur.fetchone()
                if result and result[0]:
                    if hasattr(result[0], 'strftime'):
                        return result[0].strftime("%Y%m%d")
                    return str(result[0])
        return None

    def get_row_count(self) -> int:
        """Get total row count of the table."""
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {self.table_name}")
                result = cur.fetchone()
                return result[0] if result else 0

    def _convert_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert date string columns to date type."""
        for col in self.date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format="%Y%m%d", errors="coerce").dt.date
        return df

    @staticmethod
    def _clean_for_db(df: pd.DataFrame) -> pd.DataFrame:
        """Clean DataFrame for database insertion."""
        import numpy as np
        df = df.copy()
        for col in df.columns:
            df[col] = df[col].apply(
                lambda x: None if x is pd.NaT or x is pd.NA
                or (isinstance(x, float) and (math.isnan(x) or np.isnan(x)))
                else x
            )
        return df

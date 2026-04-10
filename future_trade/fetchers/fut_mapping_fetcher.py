"""Futures dominant/continuous contract mapping fetcher."""
import pandas as pd
from fetchers.base_fetcher import BaseFetcher


class FutMappingFetcher(BaseFetcher):
    """Fetcher for futures dominant/continuous contract mapping."""

    @property
    def table_name(self) -> str:
        return "fut_mapping"

    @property
    def api_name(self) -> str:
        return "fut_mapping"

    @property
    def date_columns(self) -> list[str]:
        return ["trade_date"]

    def sync_by_code(self, ts_code: str) -> int:
        """Sync mapping for a specific continuous contract."""
        df = self.fetch(ts_code=ts_code)
        if df.empty:
            return 0
        return self.upsert_to_db(df, ["ts_code", "trade_date"])

    def sync_by_date(self, trade_date: str) -> int:
        """Sync mapping for a specific date."""
        df = self.fetch(trade_date=trade_date)
        if df.empty:
            return 0
        return self.upsert_to_db(df, ["ts_code", "trade_date"])

    def fetch(self, ts_code: str | None = None, trade_date: str | None = None, start_date: str | None = None, end_date: str | None = None) -> pd.DataFrame:
        """Fetch mapping data."""
        kwargs = {}
        if ts_code:
            kwargs["ts_code"] = ts_code
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

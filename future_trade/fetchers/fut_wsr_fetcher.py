"""Futures warehouse receipts data fetcher."""
import pandas as pd
from fetchers.base_fetcher import BaseFetcher


class FutWsrFetcher(BaseFetcher):
    """Fetcher for futures warehouse receipts."""

    @property
    def table_name(self) -> str:
        return "fut_wsr"

    @property
    def api_name(self) -> str:
        return "fut_wsr"

    @property
    def date_columns(self) -> list[str]:
        return ["trade_date"]

    def sync_by_date(self, trade_date: str) -> int:
        """Sync WSR for a specific date."""
        df = self.fetch(trade_date=trade_date)
        if df.empty:
            return 0
        return self.upsert_to_db(df, ["trade_date", "symbol", "warehouse"])

    def fetch(self, trade_date: str | None = None, start_date: str | None = None, end_date: str | None = None, exchange: str | None = None, symbol: str | None = None) -> pd.DataFrame:
        """Fetch WSR data."""
        kwargs = {}
        if trade_date:
            kwargs["trade_date"] = trade_date
        if start_date:
            kwargs["start_date"] = start_date
        if end_date:
            kwargs["end_date"] = end_date
        if exchange:
            kwargs["exchange"] = exchange
        if symbol:
            kwargs["symbol"] = symbol
        df = self._call_api(**kwargs)
        if not df.empty and self.date_columns:
            df = self._convert_dates(df)
        return df

"""Futures daily OHLCV data fetcher."""
import pandas as pd
from rich.console import Console
from fetchers.base_fetcher import BaseFetcher

console = Console()


class FutDailyFetcher(BaseFetcher):
    """Fetcher for futures daily price data."""

    @property
    def table_name(self) -> str:
        return "fut_daily"

    @property
    def api_name(self) -> str:
        return "fut_daily"

    @property
    def date_columns(self) -> list[str]:
        return ["trade_date"]

    def sync_by_date(self, trade_date: str) -> int:
        """Sync one day's data for all contracts."""
        df = self.fetch_by_date(trade_date=trade_date)
        if df.empty:
            return 0
        return self.upsert_to_db(df, ["ts_code", "trade_date"])

    def sync_date_range(self, start_date: str, end_date: str, trade_dates: list[str] | None = None) -> int:
        """Sync daily data for a date range."""
        total = 0
        for i, td in enumerate(trade_dates or []):
            df = self.fetch_by_date(trade_date=td)
            if not df.empty:
                total += self.upsert_to_db(df, ["ts_code", "trade_date"])
            if (i + 1) % 50 == 0:
                console.print(f"  [dim]Progress: {i+1}/{len(trade_dates)} days[/dim]")
        return total

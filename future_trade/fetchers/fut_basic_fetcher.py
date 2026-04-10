"""Futures contract basic info fetcher."""
import pandas as pd
from fetchers.base_fetcher import BaseFetcher
from config.postgres_config import get_settings


class FutBasicFetcher(BaseFetcher):
    """Fetcher for futures contract basic information."""

    @property
    def table_name(self) -> str:
        return "fut_basic"

    @property
    def api_name(self) -> str:
        return "fut_basic"

    @property
    def date_columns(self) -> list[str]:
        return ["list_date", "delist_date", "last_ddate"]

    def fetch(self, exchange: str, fut_type: str | None = None) -> pd.DataFrame:
        kwargs = {"exchange": exchange}
        if fut_type:
            kwargs["fut_type"] = fut_type
        df = self._call_api(**kwargs)
        if not df.empty:
            df = self._convert_dates(df)
        return df

    def fetch_all_exchanges(self, fut_type: str | None = None) -> pd.DataFrame:
        all_data = []
        for exchange in get_settings().exchanges:
            try:
                df = self.fetch(exchange=exchange, fut_type=fut_type)
                if not df.empty:
                    all_data.append(df)
            except Exception as e:
                from rich.console import Console
                Console().print(f"[yellow]Warning: Failed to fetch {exchange}: {e}[/yellow]")
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()

    def sync(self, fut_type: str | None = None) -> int:
        """Sync all exchanges' contract info to database."""
        df = self.fetch_all_exchanges(fut_type=fut_type)
        if df.empty:
            return 0
        return self.save_to_db(df, if_exists="replace")

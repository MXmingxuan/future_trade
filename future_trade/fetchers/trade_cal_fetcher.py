"""Trade calendar fetcher."""
import pandas as pd
from fetchers.base_fetcher import BaseFetcher


class TradeCalFetcher(BaseFetcher):
    """Fetcher for trade calendars."""

    @property
    def table_name(self) -> str:
        return "trade_cal"

    @property
    def api_name(self) -> str:
        return "trade_cal"

    @property
    def date_columns(self) -> list[str]:
        return ["cal_date", "pretrade_date"]

    def get_trade_dates(self, exchange: str = "DCE", start_date: str | None = None, end_date: str | None = None) -> list[str]:
        """Get list of trading dates for an exchange."""
        kwargs = {"exchange": exchange}
        if start_date:
            kwargs["start_date"] = start_date
        if end_date:
            kwargs["end_date"] = end_date
        df = self.fetch(**kwargs)
        if df.empty:
            return []
        if "is_open" in df.columns:
            df = df[df["is_open"] == 1]
        return sorted(df["cal_date"].astype(str).tolist())

    def fetch(self, exchange: str, start_date: str | None = None, end_date: str | None = None) -> pd.DataFrame:
        """Fetch trade calendar data for an exchange."""
        kwargs = {"exchange": exchange}
        if start_date:
            kwargs["start_date"] = start_date
        if end_date:
            kwargs["end_date"] = end_date
        df = self._call_api(**kwargs)
        if not df.empty and self.date_columns:
            df = self._convert_dates(df)
        return df

    def sync(self) -> int:
        """Sync trade calendars for all exchanges."""
        from config.postgres_config import get_settings
        all_data = []
        for exchange in get_settings().exchanges:
            try:
                df = self.fetch(exchange=exchange)
                if not df.empty:
                    all_data.append(df)
            except Exception as e:
                from rich.console import Console
                Console().print(f"[yellow]Warning: Failed to fetch {exchange}: {e}[/yellow]")
        if all_data:
            combined = pd.concat(all_data, ignore_index=True)
            combined = self._convert_dates(combined)
            return self.save_to_db(combined, if_exists="replace")
        return 0

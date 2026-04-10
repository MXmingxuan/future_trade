"""
Tushare Data Sync Script for future_trade.

Usage:
    python scripts/sync_tushare.py                    # Incremental sync (default)
    python scripts/sync_tushare.py --full             # Full sync from 2010
    python scripts/sync_tushare.py --full --start 20250101  # Full from specific date
    python scripts/sync_tushare.py --table fut_daily # Sync specific table only
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
from datetime import datetime
from rich.console import Console
from rich.table import Table
from fetchers import (
    FutBasicFetcher,
    TradeCalFetcher,
    FutDailyFetcher,
    FutMappingFetcher,
    FutWsrFetcher,
)

console = Console()


class IncrementalSync:
    """Daily incremental data sync - syncs from last date to today."""

    def __init__(self):
        self.today = datetime.now().strftime("%Y%m%d")

    def _get_last_date(self, table_name: str, date_col: str = "trade_date") -> str | None:
        from db import get_connection
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT MAX({date_col}) FROM {table_name}")
                result = cur.fetchone()
                if result and result[0]:
                    if hasattr(result[0], 'strftime'):
                        return result[0].strftime("%Y%m%d")
                    return str(result[0])
        return None

    def sync_fut_basic(self) -> int:
        console.print("[cyan]? Refreshing contract info...[/cyan]")
        fetcher = FutBasicFetcher()
        count = fetcher.sync()
        console.print(f"  [OK] fut_basic: {count} contracts")
        return count

    def sync_trade_cal(self) -> int:
        console.print("[cyan]? Updating trade calendars...[/cyan]")
        fetcher = TradeCalFetcher()
        count = fetcher.sync()
        console.print(f"  [OK] trade_cal: {count} records")
        return count

    def sync_fut_daily(self) -> int:
        console.print("[cyan][DATA] Syncing daily OHLCV...[/cyan]")
        last_date = self._get_last_date("fut_daily")
        if not last_date:
            console.print("  [yellow]No existing data. Run --full first.[/yellow]")
            return 0
        console.print(f"  Last sync: {last_date} ? {self.today}")
        cal_fetcher = TradeCalFetcher()
        trade_dates = cal_fetcher.get_trade_dates(exchange="DCE", start_date=last_date, end_date=self.today)
        if trade_dates and trade_dates[0] == last_date:
            trade_dates = trade_dates[1:]
        if not trade_dates:
            console.print("  [dim]No new trading dates.[/dim]")
            return 0
        fetcher = FutDailyFetcher()
        total = 0
        for td in trade_dates:
            try:
                count = fetcher.sync_by_date(td)
                total += count
            except Exception as e:
                console.print(f"  [yellow]Warning: {td} failed: {e}[/yellow]")
        console.print(f"  [OK] fut_daily: +{total} rows ({len(trade_dates)} days)")
        return total

    def sync_fut_mapping(self) -> int:
        console.print("[cyan][LINK] Updating contract mappings...[/cyan]")
        last_date = self._get_last_date("fut_mapping")
        if not last_date:
            console.print("  [yellow]No existing data.[/yellow]")
            return 0
        cal_fetcher = TradeCalFetcher()
        trade_dates = cal_fetcher.get_trade_dates(exchange="DCE", start_date=last_date, end_date=self.today)
        if trade_dates and trade_dates[0] == last_date:
            trade_dates = trade_dates[1:]
        if not trade_dates:
            console.print("  [dim]No new trading dates.[/dim]")
            return 0
        fetcher = FutMappingFetcher()
        total = 0
        for td in trade_dates:
            try:
                count = fetcher.sync_by_date(td)
                total += count
            except Exception as e:
                console.print(f"  [yellow]Warning: {td} failed: {e}[/yellow]")
        console.print(f"  [OK] fut_mapping: +{total} rows")
        return total

    def sync_fut_wsr(self) -> int:
        console.print("[cyan]? Updating warehouse receipts...[/cyan]")
        last_date = self._get_last_date("fut_wsr")
        if not last_date:
            console.print("  [yellow]No existing data.[/yellow]")
            return 0
        cal_fetcher = TradeCalFetcher()
        trade_dates = cal_fetcher.get_trade_dates(exchange="SHFE", start_date=last_date, end_date=self.today)
        if trade_dates and trade_dates[0] == last_date:
            trade_dates = trade_dates[1:]
        if not trade_dates:
            console.print("  [dim]No new trading dates.[/dim]")
            return 0
        fetcher = FutWsrFetcher()
        total = 0
        for td in trade_dates:
            try:
                count = fetcher.sync_by_date(td)
                total += count
            except Exception as e:
                console.print(f"  [yellow]Warning: {td} failed: {e}[/yellow]")
        console.print(f"  [OK] fut_wsr: +{total} rows")
        return total

    def run(self, table: str | None = None) -> None:
        console.print(f"[bold green][SYNC] Daily Sync ? {self.today}[/bold green]")
        started_at = datetime.now()
        results = {}
        steps = {
            "fut_basic": lambda: self.sync_fut_basic(),
            "trade_cal": lambda: self.sync_trade_cal(),
            "fut_daily": lambda: self.sync_fut_daily(),
            "fut_mapping": lambda: self.sync_fut_mapping(),
            "fut_wsr": lambda: self.sync_fut_wsr(),
        }
        try:
            if table:
                if table not in steps:
                    console.print(f"[red][FAIL] Unknown table: {table}[/red]")
                    return
                results[table] = steps[table]()
            else:
                for tbl, step_func in steps.items():
                    results[tbl] = step_func()
        except KeyboardInterrupt:
            console.print("\n[yellow]? Sync interrupted.[/yellow]")
        except Exception as e:
            console.print(f"\n[red][FAIL] Sync failed: {e}[/red]")
        elapsed = (datetime.now() - started_at).total_seconds()
        console.print("\n[bold green]??? Daily Sync Summary ???[/bold green]")
        table_out = Table(show_header=True)
        table_out.add_column("Table", style="cyan")
        table_out.add_column("New Rows", justify="right")
        for tbl, count in results.items():
            table_out.add_row(tbl, f"+{count:,}")
        console.print(table_out)
        console.print(f"Elapsed: {elapsed:.1f}s")


class FullSync:
    """Full historical data sync."""

    def __init__(self):
        from config.postgres_config import get_settings
        self.settings = get_settings()

    def sync_fut_basic(self) -> int:
        console.print("\n[bold cyan]? Step 1: Syncing futures contract info...[/bold cyan]")
        fetcher = FutBasicFetcher()
        count = fetcher.sync()
        console.print(f"  [OK] fut_basic: {count} contracts")
        return count

    def sync_trade_cal(self) -> int:
        console.print("\n[bold cyan]? Step 2: Syncing trade calendars...[/bold cyan]")
        fetcher = TradeCalFetcher()
        count = fetcher.sync()
        console.print(f"  [OK] trade_cal: {count} records")
        return count

    def sync_fut_daily(self, start_date: str | None = None, end_date: str | None = None) -> int:
        console.print("\n[bold cyan][DATA] Step 3: Syncing daily OHLCV data...[/bold cyan]")
        if not start_date:
            start_date = self.settings.sync_start_date
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
        console.print(f"  Date range: {start_date} ? {end_date}")
        cal_fetcher = TradeCalFetcher()
        trade_dates = cal_fetcher.get_trade_dates(exchange="DCE", start_date=start_date, end_date=end_date)
        if not trade_dates:
            console.print("  [yellow]No trading dates found.[/yellow]")
            return 0
        console.print(f"  Trading dates to sync: {len(trade_dates)}")
        fetcher = FutDailyFetcher()
        total = 0
        for i, td in enumerate(trade_dates):
            try:
                count = fetcher.sync_by_date(td)
                total += count
            except Exception as e:
                console.print(f"  [yellow]Warning: {td} failed: {e}[/yellow]")
            if (i + 1) % 100 == 0 or (i + 1) == len(trade_dates):
                console.print(f"  [dim]Progress: {i+1}/{len(trade_dates)} days, {total} total rows[/dim]")
        console.print(f"  [OK] fut_daily: {total} rows")
        return total

    def sync_fut_mapping(self) -> int:
        console.print("\n[bold cyan][LINK] Step 4: Syncing contract mappings...[/bold cyan]")
        basic_fetcher = FutBasicFetcher()
        df_basic = basic_fetcher.fetch_all_exchanges(fut_type="2")
        if df_basic.empty:
            console.print("  [yellow]No continuous contracts found.[/yellow]")
            return 0
        codes = df_basic["ts_code"].unique().tolist()
        console.print(f"  Continuous contracts: {len(codes)}")
        fetcher = FutMappingFetcher()
        total = 0
        for i, code in enumerate(codes):
            try:
                count = fetcher.sync_by_code(code)
                total += count
            except Exception as e:
                console.print(f"  [yellow]Warning: {code} failed: {e}[/yellow]")
            if (i + 1) % 20 == 0:
                console.print(f"  [dim]Progress: {i+1}/{len(codes)}[/dim]")
        console.print(f"  [OK] fut_mapping: {total} rows")
        return total

    def sync_fut_wsr(self, start_date: str | None = None, end_date: str | None = None) -> int:
        console.print("\n[bold cyan]? Step 5: Syncing warehouse receipts...[/bold cyan]")
        if not start_date:
            start_date = self.settings.sync_start_date
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
        cal_fetcher = TradeCalFetcher()
        trade_dates = cal_fetcher.get_trade_dates(exchange="SHFE", start_date=start_date, end_date=end_date)
        fetcher = FutWsrFetcher()
        total = 0
        for i, td in enumerate(trade_dates):
            try:
                count = fetcher.sync_by_date(td)
                total += count
            except Exception as e:
                console.print(f"  [yellow]Warning: {td} failed: {e}[/yellow]")
            if (i + 1) % 100 == 0 or (i + 1) == len(trade_dates):
                console.print(f"  [dim]Progress: {i+1}/{len(trade_dates)}[/dim]")
        console.print(f"  [OK] fut_wsr: {total} rows")
        return total

    def run(self, start_date: str | None = None, end_date: str | None = None, table: str | None = None) -> None:
        console.print("[bold green]? Full Sync[/bold green]")
        if table:
            console.print(f"Target table: [bold yellow]{table}[/bold yellow]")
        console.print(f"Start date: {start_date or self.settings.sync_start_date}")
        console.print(f"End date: {end_date or datetime.now().strftime('%Y%m%d')}")
        started_at = datetime.now()
        results = {}
        steps = {
            "fut_basic": lambda: self.sync_fut_basic(),
            "trade_cal": lambda: self.sync_trade_cal(),
            "fut_daily": lambda: self.sync_fut_daily(start_date, end_date),
            "fut_mapping": lambda: self.sync_fut_mapping(),
            "fut_wsr": lambda: self.sync_fut_wsr(start_date, end_date),
        }
        try:
            if table:
                if table not in steps:
                    console.print(f"[red][FAIL] Unknown table: {table}[/red]")
                    return
                results[table] = steps[table]()
            else:
                for tbl, step_func in steps.items():
                    results[tbl] = step_func()
        except KeyboardInterrupt:
            console.print("\n[yellow]? Sync interrupted by user.[/yellow]")
        except Exception as e:
            console.print(f"\n[red][FAIL] Sync failed: {e}[/red]")
        elapsed = (datetime.now() - started_at).total_seconds()
        console.print("\n[bold green]??? Sync Summary ???[/bold green]")
        table_out = Table(show_header=True)
        table_out.add_column("Table", style="cyan")
        table_out.add_column("Rows", justify="right")
        for tbl, count in results.items():
            table_out.add_row(tbl, f"{count:,}")
        table_out.add_row("[bold]Total[/bold]", f"[bold]{sum(results.values()):,}[/bold]")
        console.print(table_out)
        console.print(f"\nElapsed: {elapsed:.1f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="future_trade Tushare Data Sync")
    parser.add_argument("--full", action="store_true", help="Run full sync instead of incremental")
    parser.add_argument("--start", default=None, help="Start date (YYYYMMDD)")
    parser.add_argument("--end", default=None, help="End date (YYYYMMDD)")
    parser.add_argument("--table", default=None, help="Sync specific table only")
    args = parser.parse_args()

    if args.full:
        FullSync().run(start_date=args.start, end_date=args.end, table=args.table)
    else:
        IncrementalSync().run(table=args.table)

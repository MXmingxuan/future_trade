"""
Sync announcements from East Money for specified stock list.
Usage:
    python scripts/sync_notices.py
    python scripts/sync_notices.py --stock 000301
    python scripts/sync_notices.py --start 20250101
    python scripts/sync_notices.py --analyze      # sync then analyze recent notices
    python scripts/sync_notices.py --analyze --timeseries  # also run time-series analysis
"""
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import requests

from config.postgres_config import get_settings
from fetchers.notice_fetcher import (
    fetch_notice_list,
    fetch_notice_detail,
    parse_notice,
    upsert_notice,
)
from db import get_connection


STOCK_LIST = [
    {"code": "000301", "name": "东方盛虹"},
    {"code": "002493", "name": "荣盛石化"},
    {"code": "000703", "name": "恒逸石化"},
    {"code": "600346", "name": "恒力石化"},
    {"code": "601233", "name": "桐昆股份"},
    {"code": "603225", "name": "新凤鸣"},
    {"code": "301216", "name": "万凯新材"},
    {"code": "600370", "name": "三房巷"},
]

DEFAULT_START = "20250101"


def get_latest_date(stock_code: str) -> str | None:
    """Get the latest notice_date already in DB for this stock."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(notice_date) FROM announcements WHERE stock_code = %s",
                (stock_code,),
            )
            row = cur.fetchone()
    if row and row[0]:
        return row[0].strftime("%Y%m%d")
    return None


def get_latest_art_code(stock_code: str) -> str | None:
    """Get the latest art_code already in DB for this stock (for dedup)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT art_code FROM announcements WHERE stock_code = %s ORDER BY notice_date DESC, art_code DESC LIMIT 1",
                (stock_code,),
            )
            row = cur.fetchone()
    return row[0] if row else None


def sync_stock(stock_code: str, start_date: str, end_date: str):
    """Sync all announcements for one stock from start_date to end_date."""
    print(f"\n=== Syncing {stock_code}  {start_date} -> {end_date} ===")

    latest_in_db = get_latest_date(stock_code)
    if latest_in_db:
        print(f"  DB already has data up to {latest_in_db}")
        # Start from the day after latest
        from datetime import timedelta
        start = (datetime.strptime(latest_in_db, "%Y%m%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        start = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"

    # Fetch list (f_node=0 means all categories)
    notices, total = fetch_notice_list(stock_code, start, end_date, f_node=0, s_node=0)
    print(f"  API returned {len(notices)} notices (total hits: {total})")

    if not notices:
        print("  No new notices")
        return

    # Process each notice - fetch full text and upsert
    new_count = 0
    skip_count = 0

    for i, n in enumerate(notices):
        art_code = n.get("art_code", "")
        notice_date = n.get("notice_date", "")[:10]

        # Skip if already in DB (by art_code)
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM announcements WHERE art_code = %s", (art_code,))
                exists = cur.fetchone() is not None

        if exists:
            skip_count += 1
            continue

        # Parse metadata
        parsed = parse_notice(n)

        # Fetch full text
        time.sleep(0.3)  # Be polite
        detail = fetch_notice_detail(art_code)
        full_text = None
        attach_url = None
        if detail:
            full_text = detail.get("notice_content")
            attach_url = detail.get("attach_url")

        # Upsert
        upsert_notice(parsed, full_text=full_text, attach_url=attach_url)
        new_count += 1

        if (i + 1) % 10 == 0:
            print(f"  Processed {i+1}/{len(notices)}: +{new_count} new, {skip_count} skipped")

    print(f"  Done: {new_count} new, {skip_count} skipped")

    # Summary
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*), MIN(notice_date), MAX(notice_date) FROM announcements WHERE stock_code = %s",
                (stock_code,),
            )
            row = cur.fetchone()
            if row:
                cnt, mn, mx = row
                print(f"  DB now: {cnt} rows, {mn} to {mx}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Sync East Money announcements")
    parser.add_argument("--stock", default=None, help="Stock code to sync (default: all in STOCK_LIST)")
    parser.add_argument("--start", default=DEFAULT_START, help="Start date YYYYMMDD (default: 20250101)")
    parser.add_argument("--end", default=None, help="End date YYYYMMDD (default: today)")
    parser.add_argument("--analyze", action="store_true", help="Run single-notice analysis after sync")
    parser.add_argument(
        "--timeseries",
        action="store_true",
        help="Also run time-series analysis (implies --analyze)",
    )
    args = parser.parse_args()

    end_date = args.end or datetime.now().strftime("%Y%m%d")
    end_date_fmt = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"

    stocks = STOCK_LIST
    if args.stock:
        stocks = [s for s in STOCK_LIST if s["code"] == args.stock]
        if not stocks:
            print(f"Stock {args.stock} not in STOCK_LIST")
            sys.exit(1)

    for stock in stocks:
        try:
            sync_stock(stock["code"], args.start, end_date_fmt)
        except Exception as e:
            print(f"  Error syncing {stock['code']}: {e}")

    print("\n=== All sync complete ===")

    # 自动分析
    if args.analyze or args.timeseries:
        from services.notice_analyzer import (
            analyze_recent_notices,
            analyze_time_series,
        )

        target_stock = args.stock  # None means all stocks
        print(f"\n=== Running notice analysis (stock={target_stock or 'all'}) ===")
        results = analyze_recent_notices(days=30, stock_code=target_stock, limit=50)
        analyzed = [r for r in results if r.get("analysis")]
        print(f"  Analyzed {len(analyzed)} notices")

        if args.timeseries:
            print("\n=== Running time-series analysis ===")
            ts_result = analyze_time_series(days=30, stock_code=target_stock)
            print(f"  Overall trend: {ts_result.get('overall_trend')}")
            print(f"  Price outlook: {ts_result.get('price_outlook')}")
            print(f"  Supply direction: {ts_result.get('supply_direction')}")
            print(f"  Demand direction: {ts_result.get('demand_direction')}")


if __name__ == "__main__":
    main()

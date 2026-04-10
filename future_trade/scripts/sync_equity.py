"""
Fast bulk sync for equity data from Tushare.
Writes to future_trade database (equity tables already synced from fcta).

Usage:
    python scripts/sync_equity.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import time
from datetime import datetime, timedelta

import pandas as pd
import tushare as ts

from config.postgres_config import get_settings

INDEX_CODE_MAP = {
    "HS300": "000300.SH",
    "ZZ800": "000906.SH",
    "ZZ1000": "000852.SH",
}


def _to_date(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], format="%Y%m%d", errors="coerce").dt.date
    return out


def _latest_date(table: str, col: str):
    from db import get_connection
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX({col}) FROM {table}")
            row = cur.fetchone()
    if row and row[0]:
        return row[0].strftime("%Y%m%d")
    return None


def _upsert(table: str, df: pd.DataFrame, conflict_cols: list) -> int:
    if df.empty:
        return 0
    from db import get_connection
    cleaned = df.astype(object).where(pd.notna(df), None)
    columns = list(cleaned.columns)
    placeholders = ", ".join(["%s"] * len(columns))
    col_str = ", ".join(columns)
    conflict = ", ".join(conflict_cols)
    update_cols = [c for c in columns if c not in conflict_cols]
    if update_cols:
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
        conflict_action = f"DO UPDATE SET {updates}"
    else:
        conflict_action = "DO NOTHING"
    sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders}) ON CONFLICT ({conflict}) {conflict_action}"
    rows = [tuple(row) for row in cleaned.itertuples(index=False, name=None)]
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
    return len(rows)


def _rate_limit(interval: float) -> None:
    time.sleep(interval)


def sync_eq_daily_bulk(start_date: str, end_date: str, interval: float = 0.6) -> int:
    pro = ts.pro_api(get_settings().tushare_token)
    print(f"  Fetching eq_daily {start_date} -> {end_date} ...")
    df = pro.daily(start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        print("    (no data)")
        return 0
    df = _to_date(df, ["trade_date"])
    keep = ["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"]
    df = df[[c for c in keep if c in df.columns]]
    total = _upsert("eq_daily", df, ["ts_code", "trade_date"])
    print(f"    upserted {total} rows")
    return total


def sync_adj_factor_bulk(start_date: str, end_date: str, interval: float = 0.6) -> int:
    pro = ts.pro_api(get_settings().tushare_token)
    print(f"  Fetching eq_adj_factor {start_date} -> {end_date} ...")
    df = pro.adj_factor(start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        print("    (no data)")
        return 0
    df = _to_date(df, ["trade_date"])
    keep = ["ts_code", "trade_date", "adj_factor"]
    df = df[[c for c in keep if c in df.columns]]
    total = _upsert("eq_adj_factor", df, ["ts_code", "trade_date"])
    print(f"    upserted {total} rows")
    return total


def sync_index_members_bulk(start_date: str, end_date: str, interval: float = 0.6) -> int:
    pro = ts.pro_api(get_settings().tushare_token)
    total = 0
    for alias, index_code in INDEX_CODE_MAP.items():
        print(f"  Fetching index members {alias} ({index_code}) ...")
        df = pro.index_weight(index_code=index_code, start_date=start_date, end_date=end_date)
        _rate_limit(interval)
        if df is None or df.empty:
            print(f"    (no data for {alias})")
            continue
        keep = ["index_code", "trade_date", "con_code", "weight"]
        df = df[[c for c in keep if c in df.columns]].copy()
        if "index_code" not in df.columns:
            df["index_code"] = index_code
        df = _to_date(df, ["trade_date"])
        n = _upsert("eq_index_members", df, ["index_code", "trade_date", "con_code"])
        total += n
        print(f"    upserted {n} rows for {alias}")
    return total


def sync_daily_features_bulk(start_date: str, end_date: str, interval: float = 0.6) -> int:
    from db import get_connection
    pro = ts.pro_api(get_settings().tushare_token)
    print(f"  Fetching eq_daily_features {start_date} -> {end_date} ...")

    with get_connection() as conn:
        basic_df = pd.read_sql("SELECT ts_code, list_date FROM eq_basic", conn)
    if basic_df.empty:
        print("    eq_basic empty, skipping features")
        return 0

    with get_connection() as conn:
        trade_dates = pd.read_sql(
            "SELECT cal_date FROM eq_trade_cal WHERE exchange='SSE' AND is_open=1 AND cal_date >= %(start)s AND cal_date <= %(end)s ORDER BY cal_date",
            conn,
            params={"start": start_date, "end": end_date},
        )
    if trade_dates.empty:
        print("    no open trade dates in range")
        return 0

    with get_connection() as conn:
        st_intervals = pd.read_sql(
            "SELECT ts_code, start_date, end_date FROM eq_namechange WHERE name LIKE '%%ST%%' OR name LIKE '%%*ST%%'",
            conn,
        )
    if not st_intervals.empty:
        st_intervals["start_date"] = pd.to_datetime(st_intervals["start_date"], errors="coerce")
        st_intervals["end_date"] = pd.to_datetime(st_intervals["end_date"], errors="coerce")

    total = 0
    for _, row in trade_dates.iterrows():
        td = pd.to_datetime(row["cal_date"]).strftime("%Y%m%d")
        td_date = pd.to_datetime(row["cal_date"])

        _rate_limit(interval)
        daily_basic = pro.daily_basic(trade_date=td, fields="ts_code,trade_date,total_mv,circ_mv")
        if daily_basic is None or daily_basic.empty:
            continue
        daily_basic = _to_date(daily_basic, ["trade_date"])

        merged = daily_basic.merge(basic_df[["ts_code", "list_date"]], on="ts_code", how="left")
        merged["trade_date_ts"] = pd.to_datetime(merged["trade_date"])
        merged["list_days"] = (merged["trade_date_ts"] - pd.to_datetime(merged["list_date"], errors="coerce")).dt.days

        with get_connection() as conn:
            amount_df = pd.read_sql("SELECT ts_code, trade_date, amount FROM eq_daily WHERE trade_date = %(td)s", conn, params={"td": td})
        merged = merged.merge(amount_df, on=["ts_code", "trade_date"], how="left")

        if st_intervals.empty:
            merged["is_st"] = False
        else:
            st_today = st_intervals[
                (st_intervals["start_date"].isna() | (st_intervals["start_date"] <= td_date))
                & (st_intervals["end_date"].isna() | (st_intervals["end_date"] >= td_date))
            ]["ts_code"].astype(str)
            st_set = set(st_today.tolist())
            merged["is_st"] = merged["ts_code"].astype(str).isin(st_set)

        merged["list_days"] = pd.to_numeric(merged["list_days"], errors="coerce")
        merged.loc[(merged["list_days"] < -2147483648) | (merged["list_days"] > 2147483647), "list_days"] = None
        out = merged[["ts_code", "trade_date", "is_st", "list_days", "amount", "total_mv", "circ_mv"]].copy()
        n = _upsert("eq_daily_features", out, ["ts_code", "trade_date"])
        total += n

    print(f"    upserted {total} rows across {len(trade_dates)} dates")
    return total


def main():
    settings = get_settings()
    interval = 60.0 / settings.api_rate_limit

    last_daily = _latest_date("eq_daily", "trade_date")
    if last_daily is None:
        start_date = settings.sync_start_date
    else:
        start_date = (pd.to_datetime(last_daily) + timedelta(days=1)).strftime("%Y%m%d")

    today_str = datetime.now().strftime("%Y%m%d")
    end_date = today_str

    if start_date > end_date:
        print(f"\n=== Bulk Sync: already up to date (last_daily={_latest_date('eq_daily', 'trade_date')}) ===\n")
        return

    print(f"\n=== Bulk Sync: {start_date} -> {end_date} ===\n")

    pro = ts.pro_api(settings.tushare_token)
    print("Fetching eq_trade_cal ...")
    cal_df = pro.trade_cal(exchange="SSE", start_date=start_date, end_date=end_date, fields="exchange,cal_date,is_open,pretrade_date")
    if cal_df is not None and not cal_df.empty:
        cal_df = _to_date(cal_df, ["cal_date", "pretrade_date"])
        _upsert("eq_trade_cal", cal_df, ["exchange", "cal_date"])
        print(f"  upserted {len(cal_df)} cal rows")
    _rate_limit(interval)

    print("\nRefreshing eq_basic ...")
    for status in ["L", "D", "P"]:
        _rate_limit(interval)
        df = pro.stock_basic(exchange="", list_status=status, fields="ts_code,symbol,name,area,industry,market,list_date,delist_date,list_status")
        if df is None or df.empty:
            continue
        df = _to_date(df, ["list_date", "delist_date"])
        _upsert("eq_basic", df, ["ts_code"])
    print("  eq_basic refreshed")

    print()
    sync_eq_daily_bulk(start_date, end_date, interval)
    sync_adj_factor_bulk(start_date, end_date, interval)
    sync_index_members_bulk(start_date, end_date, interval)
    sync_daily_features_bulk(start_date, end_date, interval)

    print("\n=== Sync Complete ===")
    tables = ["eq_daily", "eq_adj_factor", "eq_daily_features", "eq_index_members"]
    from db import get_connection
    with get_connection() as conn:
        for tbl in tables:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*), MIN(trade_date), MAX(trade_date) FROM {tbl}")
                cnt, mn, mx = cur.fetchone()
                print(f"  {tbl}: {cnt:,} rows  {mn} -> {mx}")
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), MIN(cal_date), MAX(cal_date) FROM eq_trade_cal")
            cnt, mn, mx = cur.fetchone()
            print(f"  eq_trade_cal: {cnt:,} rows  {mn} -> {mx}")


if __name__ == "__main__":
    main()

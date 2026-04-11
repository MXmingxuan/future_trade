#!/usr/bin/env python3
"""
PTA 数据同步脚本 → PostgreSQL

从 Tushare 下载 PTA 期货和仓单数据，写入 PostgreSQL price_factor_daily 表。

数据范围：2025-01-01 至今
运行方式：
    python scripts/sync_pta_to_postgres.py
"""
import sys
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Optional
import time

sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
import tushare as ts
from tqdm import tqdm

# ============ 配置 ============
from config.postgres_config import get_settings
from config.tushare_config import TUSHARE_TOKEN

COMMODITY_ID = 1          # PTA
COMMODITY_CODE = "PTA"
EXCHANGE = "CZCE"        # 郑商所
PTA_SYMBOL = "PTA"       # PTA 品种代码（注意：fut_wsr 用品种代码 "PTA"，不能用合约代码 "TA"）
START_DATE = "20250101"
END_DATE = date.today().strftime("%Y%m%d")

INSERT_SQL = """
INSERT INTO price_factor_daily (
    commodity_id, trade_date,
    fut_close, fut_settle, main_contract, near_contract,
    basis_main, basis_near, basis_rate,
    basis_5d_change, basis_20d_change, basis_percentile_60,
    warehouse_receipt, wr_change, wr_5d_change, wr_percentile_60,
    time_spread, back_or_contango
) VALUES (
    %(commodity_id)s, %(trade_date)s,
    %(fut_close)s, %(fut_settle)s, %(main_contract)s, %(near_contract)s,
    %(basis_main)s, %(basis_near)s, %(basis_rate)s,
    %(basis_5d_change)s, %(basis_20d_change)s, %(basis_percentile_60)s,
    %(warehouse_receipt)s, %(wr_change)s, %(wr_5d_change)s, %(wr_percentile_60)s,
    %(time_spread)s, %(back_or_contango)s
) ON CONFLICT (commodity_id, trade_date) DO UPDATE SET
    fut_close = EXCLUDED.fut_close,
    fut_settle = EXCLUDED.fut_settle,
    main_contract = EXCLUDED.main_contract,
    near_contract = EXCLUDED.near_contract,
    basis_main = EXCLUDED.basis_main,
    basis_near = EXCLUDED.basis_near,
    basis_rate = EXCLUDED.basis_rate,
    warehouse_receipt = EXCLUDED.warehouse_receipt,
    wr_change = EXCLUDED.wr_change,
    time_spread = EXCLUDED.time_spread,
    back_or_contango = EXCLUDED.back_or_contango;
"""


def get_pro():
    return ts.pro_api(TUSHARE_TOKEN)


def get_trade_dates(pro, start: str, end: str) -> list[str]:
    """获取交易日列表"""
    df = pro.trade_cal(exchange="SSE", start_date=start, end_date=end)
    if df is None or df.empty:
        return []
    return sorted(df[df["is_open"] == 1]["cal_date"].tolist())


def get_active_pta_contracts(pro, trade_date: str) -> tuple[Optional[str], Optional[str]]:
    """
    获取某日期的主力合约和近月合约代码。
    TA.ZCE → 主力合约（mapping_ts_code）
    TAL.ZCE → 近月合约（mapping_ts_code）
    """
    try:
        df = pro.fut_mapping(trade_date=trade_date, exchange=EXCHANGE)
        if df is None or df.empty:
            return None, None
        # 筛选 TA 品种（TA.ZCE=主力，TAL.ZCE=近月）
        pta = df[df["ts_code"].isin(["TA.ZCE", "TAL.ZCE"])]
        if pta.empty:
            return None, None
        row = pta.set_index("ts_code")
        main = row.at["TA.ZCE", "mapping_ts_code"] if "TA.ZCE" in row.index else None
        near = row.at["TAL.ZCE", "mapping_ts_code"] if "TAL.ZCE" in row.index else None
        return main, near
    except Exception:
        return None, None


def get_fut_daily(pro, ts_code: str, start: str, end: str) -> dict:
    """获取合约日行情"""
    try:
        df = pro.fut_daily(ts_code=ts_code, start_date=start, end_date=end)
        if df is None or df.empty:
            return {}
        result = {}
        for _, row in df.iterrows():
            td = datetime.strptime(str(row["trade_date"]), "%Y%m%d").date()
            result[td] = {
                "close": row.get("close"),
                "settle": row.get("settle"),
                "vol": row.get("vol"),
            }
        return result
    except Exception:
        return {}


def get_pta_warehouse_receipts(pro, start: str, end: str) -> dict:
    """
    获取 PTA 仓单数据，按日期汇总。
    返回: {trade_date: {"wr": total_warehouse_receipt, "wr_chg": change}}
    """
    try:
        df = pro.fut_wsr(symbol=PTA_SYMBOL, start_date=start, end_date=end)
        if df is None or df.empty:
            return {}
        result = {}
        for _, row in df.iterrows():
            td = datetime.strptime(str(row["trade_date"]), "%Y%m%d").date()
            vol = row.get("vol") or 0
            vol_chg = row.get("vol_chg") or 0
            if td in result:
                result[td]["wr"] += vol
                result[td]["wr_chg"] += vol_chg
            else:
                result[td] = {"wr": vol, "wr_chg": vol_chg}
        return result
    except Exception as e:
        print(f"  ⚠️  仓单数据获取失败: {e}")
        return {}


def get_nearby_contract(dts: list[str], n: int = 1) -> str:
    """从日期列表中找第n近的月份（格式 TA2505 → 202505）"""
    if not dts:
        return ""
    # 找最近到期的合约
    return sorted(dts)[min(n - 1, len(dts) - 1)]


def compute_near_contract(pro, trade_date: str, main_ts_code: str) -> Optional[str]:
    """找到近月合约（主力的下一个主力合约）"""
    try:
        df = pro.fut_mapping(trade_date=trade_date, exchange=EXCHANGE)
        if df is None or df.empty:
            return None
        pta = df[df["symbol"].str.startswith(PTA_SYMBOL)].copy()
        if pta.empty:
            return None
        # 按合约代码排序（TA2505, TA2506...）
        pta["month"] = pta["ts_code"].str.extract(r"TA(\d+)")
        pta = pta.sort_values("month")
        codes = pta["ts_code"].tolist()
        if main_ts_code in codes:
            idx = codes.index(main_ts_code)
            if idx + 1 < len(codes):
                return codes[idx + 1]
        return codes[0] if codes else None
    except:
        return None


def build_price_factors(
    pro,
    trade_dates: list[str],
    fut_daily_by_code: dict,
    wr_data: dict,
) -> list[dict]:
    """为每个交易日构建 price_factor_daily 记录"""
    records = []

    for i, td_str in enumerate(tqdm(trade_dates, desc="计算因子")):
        td = datetime.strptime(td_str, "%Y%m%d").date()

        # 找主力合约
        main_code, near_code = get_active_pta_contracts(pro, td_str)
        if main_code is None:
            continue

        # 近月合约
        if near_code is None:
            near_code = compute_near_contract(pro, td_str, main_code)

        main_data = (fut_daily_by_code.get(main_code) or {}).get(td, {})
        near_data = (fut_daily_by_code.get(near_code) or {}).get(td, {}) if near_code else {}

        fut_close = main_data.get("close")
        fut_settle = main_data.get("settle")
        near_close = near_data.get("close")

        # time_spread = 主力 - 近月（正数说明现货紧张=back）
        time_spread = None
        back_or_contango = None
        if fut_close is not None and near_close is not None:
            time_spread = round(fut_close - near_close, 4)
            back_or_contango = "back" if time_spread < 0 else "contango"

        # 仓单数据
        wr_info = wr_data.get(td, {})
        warehouse_receipt = wr_info.get("wr")
        wr_chg = wr_info.get("wr_chg")

        # 5日/20日基差变化（需要历史数据，这里简化处理）
        basis_5d_change = None
        basis_20d_change = None
        basis_percentile_60 = None
        wr_5d_change = None
        wr_percentile_60 = None

        record = {
            "commodity_id": COMMODITY_ID,
            "trade_date": td,
            "fut_close": fut_close,
            "fut_settle": fut_settle,
            "main_contract": main_code,
            "near_contract": near_code,
            "basis_main": None,      # 需要现货价格，待补充
            "basis_near": None,
            "basis_rate": None,
            "basis_5d_change": basis_5d_change,
            "basis_20d_change": basis_20d_change,
            "basis_percentile_60": basis_percentile_60,
            "warehouse_receipt": warehouse_receipt,
            "wr_change": wr_chg,
            "wr_5d_change": wr_5d_change,
            "wr_percentile_60": wr_percentile_60,
            "time_spread": time_spread,
            "back_or_contango": back_or_contango,
        }
        records.append(record)

    return records


def save_to_postgres(records: list[dict]):
    """批量写入 PostgreSQL"""
    settings = get_settings()
    conn = psycopg2.connect(
        host=settings.pg_host,
        port=settings.pg_port,
        database=settings.pg_database,
        user=settings.pg_user,
        password=settings.pg_password,
    )
    cur = conn.cursor()

    count = 0
    for rec in records:
        try:
            cur.execute(INSERT_SQL, rec)
            count += 1
        except Exception as e:
            print(f"  ⚠️  插入失败 {rec['trade_date']}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ 写入 {count} 条记录到 PostgreSQL")
    return count


def main():
    print("=" * 60)
    print(f"📥 PTA 数据同步 → PostgreSQL")
    print(f"   品种：PTA（郑商所）")
    print(f"   范围：{START_DATE} → {END_DATE}")
    print("=" * 60)

    pro = get_pro()

    # Step 1: 获取交易日列表
    print("\n[1/5] 获取交易日列表...")
    trade_dates = get_trade_dates(pro, START_DATE, END_DATE)
    print(f"  交易日共 {len(trade_dates)} 天")

    # Step 2: 批量获取所有活跃 PTA 合约的日行情
    print("\n[2/5] 获取主力/近月合约代码映射...")
    all_codes = set()
    code_map = {}  # {trade_date: (main, near)}
    for td_str in tqdm(trade_dates, desc="获取合约映射"):
        main, near = get_active_pta_contracts(pro, td_str)
        code_map[td_str] = (main, near)
        if main:
            all_codes.add(main)
        if near:
            all_codes.add(near)
        time.sleep(0.05)  # 避免请求过快

    print(f"  涉及合约数：{len(all_codes)}")

    # Step 3: 获取所有合约的日行情
    print("\n[3/5] 获取合约日行情...")
    fut_daily_by_code = {}
    for code in tqdm(list(all_codes), desc="获取行情"):
        data = get_fut_daily(pro, code, START_DATE, END_DATE)
        if data:
            fut_daily_by_code[code] = data
        time.sleep(0.05)

    # Step 4: 获取仓单数据
    print("\n[4/5] 获取 PTA 仓单数据...")
    wr_data = get_pta_warehouse_receipts(pro, START_DATE, END_DATE)
    print(f"  仓单记录：{len(wr_data)} 天")

    # Step 5: 计算因子并写入
    print("\n[5/5] 计算因子并写入 PostgreSQL...")
    records = build_price_factors(pro, trade_dates, fut_daily_by_code, wr_data)
    print(f"  生成记录：{len(records)} 条")
    if records:
        save_to_postgres(records)

    print("\n" + "=" * 60)
    print("✅ PTA 数据同步完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()

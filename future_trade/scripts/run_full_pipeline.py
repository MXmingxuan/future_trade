#!/usr/bin/env python3
"""
PTA 品种日频状态分析全量 Pipeline

数据库上线后运行此脚本，执行完整流程：
1. 同步公告（东方财富）→ 抽取事件 → 写入 event_fact
2. 获取最新数值因子（Tushare）→ Agent A 分析
3. 事件时序聚合 → Agent C 分析
4. 综合翻译 → Agent D → 日频状态卡
5. 推送 Feishu

Usage:
    python scripts/run_full_pipeline.py
    python scripts/run_full_pipeline.py --date 2026-04-10
    python scripts/run_full_pipeline.py --stock 000301
    python scripts/run_full_pipeline.py --skip-sync  # 跳过公告同步，只跑分析
"""
import sys
import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from future_trade.services.daily_report import generate_daily_state, format_daily_report
from future_trade.services.market_state_engine import build_numeric_input, sync_analyze_numeric_state
from future_trade.services.trend_engine import sync_analyze_event_trend
from future_trade.services.trade_translator import sync_translate_to_trade
from future_trade.services.llm_client import LLMClient

DEFAULT_COMMODITY = "PTA"
DEFAULT_WINDOW_DAYS = 30


def run_sync_and_extract(stock_code: str | None = None, days: int = 30):
    """
    步骤1：同步公告 → 抽取事件 → 写入 event_fact
    （数据库上线后运行）
    """
    print("\n=== 步骤1：同步公告并抽取事件 ===")
    try:
        from services.notice_analyzer import analyze_recent_notices

        results = analyze_recent_notices(days=days, stock_code=stock_code, limit=50)
        analyzed = [r for r in results if r.get("analysis")]
        print(f"  ✅ 分析了 {len(analyzed)} 条公告")
        return analyzed
    except Exception as e:
        print(f"  ⚠️  同步/分析失败（DB可能离线）: {e}")
        return []


def fetch_numeric_data(commodity_code: str = "PTA", trade_date: date | None = None) -> dict:
    """
    步骤2：获取最新数值因子（数据库上线后从 price_factor_daily 读取）
    目前返回模拟数据，真实场景从 DB 查询
    """
    print("\n=== 步骤2：获取数值因子 ===")
    td = trade_date or date.today()

    try:
        # 真实：从数据库 price_factor_daily 读取
        from db.connection import get_connection

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT spot_price, fut_close, basis_main, basis_5d_change,
                           basis_20d_change, basis_percentile_60,
                           warehouse_receipt, wr_change, wr_5d_change,
                           wr_percentile_60, time_spread, back_or_contango
                    FROM price_factor_daily
                    WHERE commodity_id = (SELECT id FROM commodity_master WHERE commodity_code = %s)
                      AND trade_date = %s
                    """,
                    (commodity_code, td),
                )
                row = cur.fetchone()
                if row:
                    cols = [
                        "spot_price", "fut_close", "basis_main", "basis_5d_change",
                        "basis_20d_change", "basis_percentile_60",
                        "warehouse_receipt", "wr_change", "wr_5d_change",
                        "wr_percentile_60", "time_spread", "back_or_contango",
                    ]
                    data = dict(zip(cols, row))
                    print(f"  ✅ 从DB读取 {commodity_code} @{td}")
                    return data

        print(f"  ⚠️ DB无 {commodity_code} @{td} 的数值因子数据，使用模拟数据")
        return _mock_numeric_data(td)

    except Exception as e:
        print(f"  ⚠️  DB查询失败（DB可能离线）: {e}")
        print(f"  ℹ️  使用模拟数值因子演示")
        return _mock_numeric_data(td)


def _mock_numeric_data(td: date) -> dict:
    """演示用模拟数据"""
    return {
        "spot_price": 5800,
        "fut_close": 5750,
        "basis_main": 50,
        "basis_5d_change": 10,
        "basis_20d_change": -5,
        "basis_percentile_60": 0.65,
        "warehouse_receipt": 48000,
        "wr_change": 3000,
        "wr_5d_change": 8000,
        "wr_percentile_60": 0.72,
        "time_spread": 20,
        "back_or_contango": "back",
    }


def fetch_recent_events(
    commodity_code: str = "PTA",
    days: int = 30,
    stock_code: str | None = None,
) -> list[dict]:
    """
    步骤3：从 event_fact 表读取近期事件
    """
    try:
        from db.connection import get_connection

        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        with get_connection() as conn:
            with conn.cursor() as cur:
                if stock_code:
                    cur.execute(
                        """
                        SELECT company, event_type, supply_impact, demand_impact,
                               capacity_mtpa, effective_date, announcement_date,
                               confidence, evidence_text, time_horizon
                        FROM event_fact
                        WHERE commodity_id = (SELECT id FROM commodity_master WHERE commodity_code = %s)
                          AND (announcement_date >= %s OR effective_date >= %s)
                          AND (announcement_date IS NOT NULL OR effective_date IS NOT NULL)
                        ORDER BY COALESCE(effective_date, announcement_date) DESC
                        LIMIT 50
                        """,
                        (commodity_code, cutoff, cutoff),
                    )
                else:
                    cur.execute(
                        """
                        SELECT company, event_type, supply_impact, demand_impact,
                               capacity_mtpa, effective_date, announcement_date,
                               confidence, evidence_text, time_horizon
                        FROM event_fact
                        WHERE commodity_id = (SELECT id FROM commodity_master WHERE commodity_code = %s)
                          AND (announcement_date >= %s OR effective_date >= %s)
                        ORDER BY COALESCE(effective_date, announcement_date) DESC
                        LIMIT 50
                        """,
                        (commodity_code, cutoff, cutoff),
                    )
                rows = cur.fetchall()
                cols = [
                    "company", "event_type", "supply_impact", "demand_impact",
                    "capacity_mtpa", "effective_date", "announcement_date",
                    "confidence", "evidence_text", "time_horizon",
                ]
                events = [dict(zip(cols, r)) for r in rows]
                print(f"\n=== 步骤3：读取事件 ===")
                print(f"  ✅ 从DB读取 {len(events)} 条 event_facts")
                return events

    except Exception as e:
        print(f"\n=== 步骤3：读取事件 ===")
        print(f"  ⚠️  DB查询失败（DB可能离线）: {e}")
        print(f"  ℹ️  使用模拟事件演示")
        return _mock_events()


def _mock_events() -> list[dict]:
    """演示用模拟事件"""
    return [
        {
            "event_type": "投产", "company": "东方盛虹",
            "supply_impact": "增加", "demand_impact": "不变",
            "capacity_mtpa": 250, "confidence": "高",
            "announcement_date": "2026-04-01", "effective_date": "2026-04-01",
            "time_horizon": "短期",
            "evidence_text": "年产250万吨PTA项目正式投产",
        },
        {
            "event_type": "检修", "company": "恒逸石化",
            "supply_impact": "减少", "demand_impact": "不变",
            "capacity_mtpa": 100, "confidence": "高",
            "announcement_date": "2026-03-28", "effective_date": "2026-03-28",
            "time_horizon": "短期",
            "evidence_text": "PTA装置检修15天",
        },
        {
            "event_type": "扩产公告", "company": "荣盛石化",
            "supply_impact": "增加（预期）", "demand_impact": "不变",
            "confidence": "中",
            "announcement_date": "2026-03-20", "effective_date": None,
            "time_horizon": "中期",
            "evidence_text": "浙石化二期PX扩张计划推进中",
        },
    ]


def run_analysis(
    commodity_code: str = "PTA",
    numeric_data: dict | None = None,
    recent_events: list[dict] | None = None,
    trade_date: date | None = None,
) -> dict:
    """步骤4-6：Agent A → C → D → 综合状态卡"""
    td = trade_date or date.today()
    print(f"\n=== 步骤4-6：运行 Agent A/C/D 分析 ===")

    llm = LLMClient()

    # Agent A
    if numeric_data:
        input_data = build_numeric_input(
            spot_price=numeric_data.get("spot_price"),
            fut_close=numeric_data.get("fut_close"),
            basis_main=numeric_data.get("basis_main"),
            basis_5d_change=numeric_data.get("basis_5d_change"),
            basis_20d_change=numeric_data.get("basis_20d_change"),
            basis_percentile_60=numeric_data.get("basis_percentile_60"),
            warehouse_receipt=numeric_data.get("warehouse_receipt"),
            wr_change=numeric_data.get("wr_change"),
            wr_5d_change=numeric_data.get("wr_5d_change"),
            wr_percentile_60=numeric_data.get("wr_percentile_60"),
            time_spread=numeric_data.get("time_spread"),
            back_or_contango=numeric_data.get("back_or_contango"),
            trade_date=td,
        )
        agent_a = sync_analyze_numeric_state(input_data, llm)
        print(f"  ✅ Agent A done: {agent_a.get('direction')} ({agent_a.get('confidence')})")
    else:
        agent_a = {"note": "无数值因子数据"}

    # Agent C
    if recent_events:
        agent_c = sync_analyze_event_trend(
            events=recent_events,
            commodity_name=commodity_code,
            window_days=30,
            numeric_direction=agent_a.get("direction") if isinstance(agent_a, dict) else None,
            llm_client=llm,
        )
        print(f"  ✅ Agent C done: supply={agent_c.get('supply_trend')}, demand={agent_c.get('demand_trend')}")
    else:
        agent_c = {"note": "无事件数据"}

    # Agent D
    if isinstance(agent_a, dict) and agent_a.get("direction") and isinstance(agent_c, dict) and agent_c.get("supply_trend"):
        agent_d = sync_translate_to_trade(
            agent_a_result=agent_a,
            agent_c_result=agent_c,
            commodity_name=commodity_code,
            llm_client=llm,
        )
        print(f"  ✅ Agent D done: fut={agent_d.get('fut_interpretation',{}).get('direction')}, stock={agent_d.get('stock_interpretation',{}).get('direction')}")
    else:
        agent_d = {"note": "Agent A/C 数据不足"}

    # 综合状态卡
    report = generate_daily_state(
        commodity_code=commodity_code,
        trade_date=td,
        numeric_data=numeric_data,
        recent_events=recent_events,
        llm_client=llm,
    )

    return report


def save_and_push(report: dict, commodity_code: str = "PTA"):
    """步骤7：保存 + 推送 Feishu"""
    print("\n=== 步骤7：保存状态卡 ===")

    # 保存到本地文件
    output_dir = Path.home() / ".hermes" / "future_trade" / "reports"
    output_dir.mkdir(parents=True, exist_ok=True)
    td = report.get("report_date", date.today())
    out_file = output_dir / f"{commodity_code.lower()}_{td}.json"
    import json
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"  ✅ 保存到 {out_file}")

    # 推送 Feishu（如果有配置）
    try:
        from gateway.platforms.feishu import send_text_message
        # 读取 config 中的 feishu 配置
        # 简化：直接打印推送内容
        formatted = format_daily_report(report)
        print("\n" + formatted)
        print("\n  ℹ️  Feishu 推送需要 gateway 在线，略过")
    except Exception as e:
        print(f"  ℹ️  Feishu 推送不可用: {e}")


def main():
    parser = argparse.ArgumentParser(description="PTA 日频状态分析全量 Pipeline")
    parser.add_argument("--date", type=str, default=None, help="交易日期 YYYY-MM-DD")
    parser.add_argument("--stock", type=str, default=None, help="只分析特定股票代码")
    parser.add_argument("--commodity", type=str, default=DEFAULT_COMMODITY, help="品种代码")
    parser.add_argument("--skip-sync", action="store_true", help="跳过公告同步")
    parser.add_argument("--days", type=int, default=DEFAULT_WINDOW_DAYS, help="事件窗口天数")
    args = parser.parse_args()

    trade_date = None
    if args.date:
        trade_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    commodity = args.commodity.upper()
    print(f"\n{'='*60}")
    print(f"📊 {commodity} 日频状态分析 Pipeline")
    print(f"   日期：{trade_date or date.today()}")
    print(f"   股票：{args.stock or '全品种'}")
    print(f"{'='*60}")

    # Step 1: Sync + Extract (optional)
    analyzed_results = []
    if not args.skip_sync:
        analyzed_results = run_sync_and_extract(stock_code=args.stock, days=args.days)

    # Step 2: Numeric data
    numeric_data = fetch_numeric_data(commodity_code=commodity, trade_date=trade_date)

    # Step 3: Recent events
    recent_events = fetch_recent_events(commodity_code=commodity, days=args.days, stock_code=args.stock)

    # Step 4-6: Run agents + generate report
    report = run_analysis(
        commodity_code=commodity,
        numeric_data=numeric_data,
        recent_events=recent_events,
        trade_date=trade_date,
    )

    # Step 7: Save + Push
    save_and_push(report, commodity_code=commodity)

    print(f"\n{'='*60}")
    print(f"✅ Pipeline 完成！")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
PTA 日频状态卡演示脚本

不依赖数据库，用模拟数据演示完整的 Agent A+B+C+D 流程。
数据库上线后，用 run_full_pipeline.py 运行真实分析。

Usage:
    python scripts/demo_report.py
"""
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from future_trade.services.daily_report import generate_daily_state, format_daily_report


def main():
    print("=" * 60)
    print("📊 PTA 日频状态卡演示")
    print(f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # === 模拟数值因子（来自 Tushare fut_daily + fut_wsr）===
    numeric_data = {
        "spot_price": 5800,          # 现货价格（元/吨）
        "fut_close": 5750,           # 期货收盘价
        "basis_main": 50,            # 主力基差 = 现货 - 期货
        "basis_5d_change": 10,       # 基差5日变化
        "basis_20d_change": -5,      # 基差20日变化
        "basis_percentile_60": 0.65, # 基差60日分位
        "warehouse_receipt": 48000,  # 仓单量（张）
        "wr_change": 3000,           # 仓单日变化
        "wr_5d_change": 8000,        # 仓单5日变化
        "wr_percentile_60": 0.72,    # 仓单60日分位
        "time_spread": 20,           # 主力-近月月差
        "back_or_contango": "back",  # 现货强=back，期货强=contango
    }

    # === 模拟事件列表（来自公告抽取结果）===
    events = [
        {
            "event_type": "投产",
            "company": "东方盛虹",
            "supply_impact": "增加",
            "demand_impact": "不变",
            "capacity_mtpa": 250,
            "confidence": "高",
            "announcement_date": "2026-04-01",
            "effective_date": "2026-04-01",
            "time_horizon": "短期",
            "evidence_text": "年产250万吨PTA项目正式投产，投产后公司PTA总产能达400万吨/年",
        },
        {
            "event_type": "检修",
            "company": "恒逸石化",
            "supply_impact": "减少",
            "demand_impact": "不变",
            "capacity_mtpa": 100,
            "confidence": "高",
            "announcement_date": "2026-03-28",
            "effective_date": "2026-03-28",
            "time_horizon": "短期",
            "evidence_text": "PTA装置检修15天，预计影响产量约4万吨",
        },
        {
            "event_type": "扩产公告",
            "company": "荣盛石化",
            "supply_impact": "增加（预期）",
            "demand_impact": "不变",
            "confidence": "中",
            "announcement_date": "2026-03-20",
            "effective_date": None,
            "time_horizon": "中期",
            "evidence_text": "浙石化二期PX扩张计划推进中，环评已获受理",
        },
        {
            "event_type": "业绩预告",
            "company": "桐昆股份",
            "supply_impact": "不变",
            "demand_impact": "不变",
            "confidence": "高",
            "announcement_date": "2026-03-15",
            "effective_date": None,
            "time_horizon": "已发生",
            "evidence_text": "预计2025年净利润同比增长15-25%，受益于聚酯行业景气度回升",
        },
    ]

    print("\n📡 数据来源：")
    print(f"  数值因子：模拟数据（真实场景来自 Tushare fut_daily + fut_wsr）")
    print(f"  事件列表：{len(events)} 条（真实场景来自公告抽取 + event_fact 表）")
    print()

    # === 运行完整分析流程 ===
    report = generate_daily_state(
        commodity_code="PTA",
        trade_date=date.today(),
        numeric_data=numeric_data,
        recent_events=events,
    )

    # === 打印结果 ===
    print(format_daily_report(report))

    print("\n" + "=" * 60)
    print("💡 演示结束。数据库上线后，运行：")
    print("   python scripts/sync_notices.py --analyze --timeseries")
    print("=" * 60)

    return report


if __name__ == "__main__":
    main()

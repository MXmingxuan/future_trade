"""
日频状态卡生成器

整合 Agent A（数值）+ Agent B（单篇抽取）+ Agent C（时序）+ Agent D（翻译）
输出完整的日频品种状态报告。

用法：
    from services.daily_report import generate_daily_state
    report = generate_daily_state(commodity_code="PTA", trade_date="2026-04-10")
"""
import json
import logging
from datetime import date, datetime, timedelta
from typing import Any

from .llm_client import LLMClient
from .market_state_engine import build_numeric_input, sync_analyze_numeric_state
from .trend_engine import sync_analyze_event_trend
from .trade_translator import sync_translate_to_trade

logger = logging.getLogger(__name__)

# 标准输出字段
REPORT_FIELDS = [
    "report_date",
    "commodity",
    "overall_direction",
    "overall_confidence",
    "state_type",
    "agent_a",
    "agent_b_recent",
    "agent_c",
    "agent_d",
    "key_takeaway",
    "generated_at",
]


def _score_from_direction(direction: str) -> int:
    """将文字方向转为整数评分"""
    mapping = {
        "看涨": 2, "偏多": 1, "中性": 0,
        "震荡偏强": 1, "震荡偏弱": -1, "震荡": 0,
        "偏空": -1, "看跌": -2,
    }
    return mapping.get(direction, 0)


def generate_daily_state(
    commodity_code: str = "PTA",
    trade_date: date | str | None = None,
    numeric_data: dict[str, Any] | None = None,
    recent_events: list[dict[str, Any]] | None = None,
    llm_client: LLMClient | None = None,
) -> dict[str, Any]:
    """
    生成日频品种状态卡。

    流程：
    1. Agent A：数值因子 → 市场状态
    2. Agent B：最近重要事件（已抽取好传入）
    3. Agent C：事件时序 → 趋势判断
    4. Agent D：A+C → 期货+股票双路径翻译
    5. 综合输出

    Args:
        commodity_code: 品种代码，如 'PTA'
        trade_date: 交易日期，默认为今天
        numeric_data: Agent A 输入数据（如不提供则跳过数值分析）
        recent_events: 近期 event_fact 列表（如不提供则跳过事件分析）
        llm_client: 可选

    Returns:
        完整的状态卡字典
    """
    if llm_client is None:
        llm_client = LLMClient()

    if trade_date is None:
        trade_date = date.today()
    elif isinstance(trade_date, str):
        trade_date = datetime.strptime(trade_date, "%Y-%m-%d").date()

    report = {
        "report_date": str(trade_date),
        "commodity": commodity_code,
        "generated_at": datetime.now().isoformat(),
    }

    # ---- Agent A：数值因子分析 ----
    agent_a_result: dict[str, Any] = {}
    if numeric_data:
        logger.info(f"[{trade_date}] Running Agent A for {commodity_code}...")
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
            trade_date=trade_date,
        )
        agent_a_result = sync_analyze_numeric_state(input_data, llm_client)
        report["agent_a"] = agent_a_result
    else:
        report["agent_a"] = {"note": "无数值因子数据，跳过"}

    # ---- Agent C：事件趋势分析 ----
    agent_c_result: dict[str, Any] = {}
    if recent_events:
        logger.info(f"[{trade_date}] Running Agent C for {commodity_code} with {len(recent_events)} events...")
        agent_c_result = sync_analyze_event_trend(
            events=recent_events,
            commodity_name=commodity_code,
            window_days=30,
            numeric_direction=agent_a_result.get("direction"),
            llm_client=llm_client,
        )
        report["agent_c"] = agent_c_result
    else:
        report["agent_c"] = {"note": "无事件数据，跳过"}

    # ---- Agent D：交易翻译 ----
    if agent_a_result and agent_c_result:
        logger.info(f"[{trade_date}] Running Agent D for {commodity_code}...")
        agent_d_result = sync_translate_to_trade(
            agent_a_result=agent_a_result,
            agent_c_result=agent_c_result,
            commodity_name=commodity_code,
            llm_client=llm_client,
        )
        report["agent_d"] = agent_d_result
    else:
        report["agent_d"] = {"note": "Agent A/C 数据不足，跳过"}

    # ---- 综合结论 ----
    a_score = _score_from_direction(agent_a_result.get("direction", "中性"))
    c_score_map = {"扩张加速": 2, "扩张放缓": 1, "收缩": -2, "平稳": 0,
                   "改善": 2, "走弱": -2}
    c_score = c_score_map.get(agent_c_result.get("supply_trend", ""), 0) if agent_c_result else 0

    total_score = a_score + c_score

    if total_score >= 2:
        overall = "看涨"
    elif total_score >= 0:
        overall = "震荡偏强"
    elif total_score >= -1:
        overall = "震荡偏弱"
    else:
        overall = "看跌"

    # 置信度：两个 Agent 都有数据时取较高
    conf_a = agent_a_result.get("confidence", "低")
    conf_c = agent_c_result.get("trend_confidence", "低")
    conf_priority = {"高": 3, "中": 2, "低": 1}
    overall_confidence = "高" if max(conf_priority.get(conf_a, 0), conf_priority.get(conf_c, 0)) >= 3 else \
                         "中" if max(conf_priority.get(conf_a, 0), conf_priority.get(conf_c, 0)) >= 2 else "低"

    report["overall_direction"] = overall
    report["overall_confidence"] = overall_confidence

    # 市场状态类型判断
    if agent_a_result.get("basis_interpretation", {}).get("verdict") == "现货强于期货":
        state_type = "现实偏紧"
    elif agent_a_result.get("term_structure", {}).get("verdict") == "期货偏强":
        state_type = "预期偏强"
    elif agent_c_result.get("supply_trend", "") in ("扩张加速", "扩张放缓"):
        state_type = "供给压力中期压制"
    else:
        state_type = "供需基本平衡"

    report["state_type"] = state_type

    # ---- 核心结论 ----
    key_takeaway_parts = []

    if agent_a_result.get("today_summary"):
        key_takeaway_parts.append(f"数值：{agent_a_result['today_summary']}")

    if agent_c_result.get("supply_trend"):
        key_takeaway_parts.append(f"供给：{agent_c_result['supply_trend']}")

    if agent_c_result.get("demand_trend"):
        key_takeaway_parts.append(f"需求：{agent_c_result['demand_trend']}")

    if agent_d_result.get("fut_interpretation", {}).get("direction"):
        key_takeaway_parts.append(
            f"期货：{agent_d_result['fut_interpretation']['direction']}"
        )

    if agent_d_result.get("stock_interpretation", {}).get("direction"):
        key_takeaway_parts.append(
            f"股票：{agent_d_result['stock_interpretation']['direction']}"
        )

    report["key_takeaway"] = " | ".join(key_takeaway_parts) if key_takeaway_parts else "数据不足"

    # ---- Agent B 最近重要事件摘要 ----
    if recent_events:
        # 取最近3条最重要的
        important = sorted(
            [e for e in recent_events if e.get("confidence") in ("高", "中")],
            key=lambda x: x.get("announcement_date", ""),
            reverse=True,
        )[:3]
        report["agent_b_recent"] = [
            {
                "date": e.get("announcement_date", ""),
                "company": e.get("company", ""),
                "type": e.get("event_type", ""),
                "impact": e.get("supply_impact", ""),
                "conclusion": e.get("evidence_text", "")[:80],
            }
            for e in important
        ]
    else:
        report["agent_b_recent"] = []

    logger.info(
        f"[{trade_date}] {commodity_code} state: {overall} ({overall_confidence}) | "
        f"A={agent_a_result.get('direction','N/A')} C={agent_c_result.get('supply_trend','N/A')}"
    )

    return report


def format_daily_report(report: dict[str, Any]) -> str:
    """
    将状态卡字典格式化为可读文本（用于推送/打印）。
    """
    lines = [
        f"📊 **{report['commodity']} 日频状态卡**",
        f"📅 {report['report_date']} | 🕐 {report['generated_at'][:16]} 生成",
        "",
        f"🔮 综合方向：**{report['overall_direction']}**（置信度：{report['overall_confidence']}）",
        f"📌 市场状态：{report['state_type']}",
        "",
    ]

    # Agent A
    a = report.get("agent_a", {})
    if "note" not in a:
        lines.append("**【Agent A · 数值因子】**")
        lines.append(f"  · 今日总结：{a.get('today_summary','')}")
        lines.append(f"  · 核心解读：{a.get('core_interpretation','')}")
        lines.append(f"  · 基差判断：{a.get('basis_interpretation',{}).get('verdict','')}")
        lines.append(f"  · 仓单解读：{a.get('warehouse_receipt_interpretation',{}).get('verdict','')}")
        lines.append(f"  · 期限结构：{a.get('term_structure',{}).get('verdict','')}")
        lines.append(f"  · 反向证据：{', '.join(a.get('counter_evidence',[]) or ['无'])}")
        lines.append("")

    # Agent C
    c = report.get("agent_c", {})
    if "note" not in c:
        lines.append("**【Agent C · 事件趋势】**")
        lines.append(f"  · 供给趋势：{c.get('supply_trend','')}")
        lines.append(f"  · 需求趋势：{c.get('demand_trend','')}")
        lines.append(f"  · 成本趋势：{c.get('cost_trend','')}")
        lines.append(f"  · 数据-公告一致性：{c.get('data_event_consistency','')}")
        lines.append(f"  · 现实vs预期：{c.get('reality_vs_expectation','')}")
        lines.append(f"  · 1个月展望：{c.get('outlook_1m','')}")
        lines.append(f"  · 3个月展望：{c.get('outlook_3m','')}")
        lines.append("")

    # Agent D
    d = report.get("agent_d", {})
    if "note" not in d:
        fut = d.get("fut_interpretation", {})
        stk = d.get("stock_interpretation", {})
        lines.append("**【Agent D · 交易翻译】**")
        lines.append(f"  🏛 期货：**{fut.get('direction','')}**")
        for r in fut.get("reasoning", [])[:2]:
            lines.append(f"     · {r}")
        lines.append(f"     ⚠️ {', '.join(fut.get('risk_points', []) or ['无明显风险'])}")
        lines.append(f"  📈 股票：**{stk.get('direction','')}**")
        for co in stk.get("affected_companies", [])[:3]:
            lines.append(f"     · {co.get('code')} {co.get('name')}：{co.get('impact','')}（{co.get('reason','')[:30]}）")
        lines.append(f"  🔗 期货股票一致性：**{d.get('consistency','')}**")
        lines.append(f"  💡 对冲建议：{d.get('hedge_suggestion','')}")
        lines.append("")

    # Agent B 最近重要事件
    b = report.get("agent_b_recent", [])
    if b:
        lines.append("**【Agent B · 最近重要事件】**")
        for ev in b:
            lines.append(f"  · [{ev['date']}] {ev['company']} {ev['type']} → {ev['impact']}")
        lines.append("")

    lines.append(f"**📝 核心结论：** {report.get('key_takeaway','')}")

    return "\n".join(lines)

"""
Agent C：事件时间轴引擎

输入：某品种过去一段时间的 event_fact 记录列表
输出：供给/需求/成本趋势判断 + 数据与公告一致性 + 现实vs预期判断

职责：不是复述事件，而是判断趋势方向
"""
import json
import logging
from datetime import date, timedelta
from typing import Any

from .llm_client import LLMClient

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """你是一个PTA期货品种的公告时间轴分析师。

你的任务是把多条事件按时间聚合，判断中期趋势，而不是逐条复述。

核心原则：
1. 不是摘要机器，而是趋势判断机器
2. 区分"短期扰动"和"中期趋势"
3. 判断市场现实和预期是否一致

输出格式（严格JSON）：
{
  "supply_trend": "扩张加速/扩张放缓/收缩/平稳",
  "demand_trend": "改善/走弱/平稳",
  "cost_trend": "增强/减弱/平稳",
  "data_event_consistency": "一致/不一致/无法判断",
  "reality_vs_expectation": "现实强于预期/预期强于现实/一致/无法判断",
  "key_drivers": ["主要驱动因素1", "主要驱动因素2"],
  "trend_confidence": "高/中/低",
  "outlook_1m": "短期1个月趋势",
  "outlook_3m": "中期3个月趋势",
  "risk_signals": ["警示信号"],
  "event_count_summary": {
    "supply_increase": N,
    "supply_decrease": N,
    "demand_increase": N,
    "demand_decrease": N
  }
}
"""


def build_event_timeline_input(
    events: list[dict[str, Any]],
    commodity_name: str = "PTA",
    window_days: int = 30,
) -> str:
    """
    构建 Agent C 的事件时间轴输入文本。

    Args:
        events: event_fact 记录的字典列表，每条至少含：
               event_type, company, supply_impact, demand_impact,
               effective_date, confidence, announcement_date, evidence_text
        commodity_name: 品种名
        window_days: 时间窗口天数

    Returns:
        用于填充 prompt 的字符串
    """
    if not events:
        return f"近{window_days}天内无标准化事件记录。"

    # 按时间倒序排列
    sorted_events = sorted(
        events,
        key=lambda x: x.get("announcement_date") or x.get("effective_date") or "",
        reverse=True,
    )

    lines = [f"## {commodity_name}产业链 近{window_days}天重要事件\n"]
    lines.append(f"共 {len(events)} 条事件：\n")

    for i, ev in enumerate(sorted_events, 1):
        date_str = ev.get("announcement_date") or ev.get("effective_date") or "未知日期"
        company = ev.get("company", "未知公司")
        event_type = ev.get("event_type", "未知类型")
        supply = ev.get("supply_impact", "不确定")
        demand = ev.get("demand_impact", "不确定")
        capacity = ev.get("capacity_mtpa")
        capacity_str = f"，涉及产能{capacity}万吨/年" if capacity else ""
        confidence = ev.get("confidence", "低")
        evidence = ev.get("evidence_text", "")[:100]
        horizon = ev.get("time_horizon", "不确定")

        lines.append(
            f"{i}. [{date_str}] {company} - {event_type}\n"
            f"   供给影响：{supply} | 需求影响：{demand} | 置信度：{confidence} | 周期：{horizon}\n"
            f"   证据：{evidence[:80]}...{capacity_str}\n"
        )

    return "\n".join(lines)


def analyze_event_trend(
    events: list[dict[str, Any]],
    commodity_name: str = "PTA",
    window_days: int = 30,
    numeric_direction: str | None = None,
    llm_client: LLMClient | None = None,
) -> dict[str, Any]:
    """
    Agent C 主函数：输入事件列表，返回趋势判断。

    Args:
        events: event_fact 字典列表
        commodity_name: 品种名
        window_days: 时间窗口
        numeric_direction: Agent A 的数值因子方向（可选，用于一致性判断）
        llm_client: 可选，传入已有客户端

    Returns:
        Agent C 的趋势判断结论
    """
    if llm_client is None:
        llm_client = LLMClient()

    events_text = build_event_timeline_input(events, commodity_name, window_days)

    prompt_extra = ""
    if numeric_direction:
        prompt_extra = f"\n\n## 数值因子背景\n当前数值因子方向：{numeric_direction}（来自Agent A）\n请判断：公告事件趋势是否与数值因子方向一致？"

    prompt = f"""你是一个PTA期货品种的公告时间轴分析师。

请根据以下事件列表，综合判断{commodity_name}的供给、需求、成本趋势。

{events_text}{prompt_extra}

请回答：
1. 供给端是扩张还是在收缩？趋势是否在加速还是放缓？
2. 需求端是改善还是在走弱？
3. 成本端有没有明显变化？
4. 公告事件与数值因子是否一致？
5. 市场是现实强于预期，还是预期强于现实？

严格输出JSON（不要其他文字）：
{{
  "supply_trend": "扩张加速/扩张放缓/收缩/平稳",
  "demand_trend": "改善/走弱/平稳",
  "cost_trend": "增强/减弱/平稳",
  "data_event_consistency": "一致/不一致/无法判断",
  "reality_vs_expectation": "现实强于预期/预期强于现实/一致/无法判断",
  "key_drivers": ["主要驱动因素"],
  "trend_confidence": "高/中/低",
  "outlook_1m": "短期1个月趋势描述",
  "outlook_3m": "中期3个月趋势描述",
  "risk_signals": ["警示信号"],
  "event_count_summary": {{
    "supply_increase": N,
    "supply_decrease": N,
    "demand_increase": N,
    "demand_decrease": N
  }}
}}"""

    try:
        result = llm_client.prompt(prompt, system_prompt=SYSTEM_PROMPT)
        logger.info(
            f"Agent C output: supply={result.get('supply_trend')}, "
            f"demand={result.get('demand_trend')}, consistency={result.get('data_event_consistency')}"
        )
        return result
    except Exception as e:
        logger.error(f"Agent C failed: {e}")
        return {
            "supply_trend": "平稳",
            "demand_trend": "平稳",
            "error": str(e),
            "trend_confidence": "低",
        }


# 同步版本
def sync_analyze_event_trend(
    events: list[dict[str, Any]],
    commodity_name: str = "PTA",
    window_days: int = 30,
    numeric_direction: str | None = None,
    llm_client: LLMClient | None = None,
) -> dict[str, Any]:
    """同步包装"""
    return analyze_event_trend(events, commodity_name, window_days, numeric_direction, llm_client)

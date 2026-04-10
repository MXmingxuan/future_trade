"""
Agent D：交易翻译引擎

输入：Agent A（数值因子）+ Agent C（事件趋势）的结论
输出：期货方向 + 产业链股票方向（两条路径分开）

核心原则：同一产业事件，对期货和股票不一定同方向
"""
import json
import logging
from typing import Any

from .llm_client import LLMClient

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """你是一个PTA期货和产业链股票的分析师。

你的任务是将产业研究结论翻译成两种不同的交易视角：
1. PTA 期货：价格方向、理由、风险点
2. 产业链股票：不同公司的差异化影响

核心原则：同一产业事件，对期货和股票不一定同方向。
例如：PTA扩产→对期货偏空，但对有规模效应的一体化龙头可能偏多。

输出格式（严格JSON）：
{
  "fut_interpretation": {
    "direction": "看涨/看跌/震荡偏强/震荡偏弱/震荡",
    "reasoning": ["理由1", "理由2"],
    "risk_points": ["风险点"],
    "entry_considerations": ["进场考虑因素"]
  },
  "stock_interpretation": {
    "direction": "看多/看空/中性",
    "upstream_view": "上游（PX/PTA）视角",
    "downstream_view": "下游（聚酯）视角",
    "一体化_view": "一体化龙头视角",
    "affected_companies": [
      {"code": "000301", "name": "东方盛虹", "impact": "偏多/偏空/中性", "reason": "原因"}
    ]
  },
  "consistency": "一致/不一致",
  "reality_weight": {
    "fut_more_real": true/false,
    "stock_more_expectation": true/false,
    "explanation": "解释"
  },
  "hedge_suggestion": "是否有对冲机会建议",
  "confidence": "高/中/低"
}
"""


def build_translation_input(
    agent_a_result: dict[str, Any],
    agent_c_result: dict[str, Any],
    commodity_name: str = "PTA",
) -> str:
    """构建 Agent D 的输入文本"""

    a_dir = agent_a_result.get("direction", "未知")
    a_conf = agent_a_result.get("confidence", "未知")
    a_summary = agent_a_result.get("today_summary", "")
    a_interp = agent_a_result.get("core_interpretation", "")

    c_supply = agent_c_result.get("supply_trend", "平稳")
    c_demand = agent_c_result.get("demand_trend", "平稳")
    c_cost = agent_c_result.get("cost_trend", "平稳")
    c_consistency = agent_c_result.get("data_event_consistency", "无法判断")
    c_reality = agent_c_result.get("reality_vs_expectation", "无法判断")
    c_conf = agent_c_result.get("trend_confidence", "未知")
    c_outlook_1m = agent_c_result.get("outlook_1m", "")
    c_outlook_3m = agent_c_result.get("outlook_3m", "")
    c_risks = agent_c_result.get("risk_signals", [])

    return f"""## {commodity_name} 综合分析输入

### Agent A：数值因子结论
- 方向：{a_dir}（置信度：{a_conf}）
- 今日总结：{a_summary}
- 核心解读：{a_interp}

### Agent C：事件趋势结论
- 供给趋势：{c_supply}
- 需求趋势：{c_demand}
- 成本趋势：{c_cost}
- 数据-公告一致性：{c_consistency}
- 现实vs预期：{c_reality}（置信度：{c_conf}）
- 1个月展望：{c_outlook_1m}
- 3个月展望：{c_outlook_3m}
- 风险信号：{', '.join(c_risks) if c_risks else '无明显风险信号'}
"""


def translate_to_trade(
    agent_a_result: dict[str, Any],
    agent_c_result: dict[str, Any],
    commodity_name: str = "PTA",
    llm_client: LLMClient | None = None,
) -> dict[str, Any]:
    """
    Agent D 主函数：综合 A+C 结论，翻译成交易视角。

    Args:
        agent_a_result: Agent A 的数值因子结论
        agent_c_result: Agent C 的事件趋势结论
        commodity_name: 品种名

    Returns:
        Agent D 的交易翻译结论（含期货+股票双路径）
    """
    if llm_client is None:
        llm_client = LLMClient()

    input_text = build_translation_input(agent_a_result, agent_c_result, commodity_name)

    prompt = f"""{input_text}

请根据以上研究结论，回答：
1. 对{commodity_name}期货来说，这意味着什么方向？有哪些交易考虑？
2. 对产业链相关股票来说，不同环节（上游/下游/一体化）分别受到什么影响？
3. 期货和股票的方向是否一致？哪个更偏现实，哪个更偏预期？
4. 是否有对冲或跨品种的机会？

已知PTA产业链主要公司：
- 000301 东方盛虹（炼化+聚酯，上游）
- 000703 恒逸石化（PTA+锦纶，上游）
- 002493 荣盛石化（PX+PTA，上游）
- 600346 恒力石化（炼化+PTA+聚酯，上游一体化）
- 601233 桐昆股份（聚酯长丝，下游）
- 603225 新凤鸣（聚酯长丝，下游）
- 301216 万凯新材（瓶片，下游）
- 600370 三房巷（瓶片+PTA贸易，下游）

严格输出JSON（不要其他文字）：
{{
  "fut_interpretation": {{
    "direction": "看涨/看跌/震荡偏强/震荡偏弱/震荡",
    "reasoning": ["理由"],
    "risk_points": ["风险"],
    "entry_considerations": ["进场考虑"]
  }},
  "stock_interpretation": {{
    "direction": "看多/看空/中性",
    "upstream_view": "上游视角",
    "downstream_view": "下游视角",
    "一体化_view": "一体化龙头视角",
    "affected_companies": [
      {{"code":"","name":"","impact":"","reason":""}}
    ]
  }},
  "consistency": "一致/不一致",
  "reality_weight": {{
    "fut_more_real": true/false,
    "stock_more_expectation": true/false,
    "explanation": ""
  }},
  "hedge_suggestion": "对冲建议或无",
  "confidence": "高/中/低"
}}"""

    try:
        result = llm_client.prompt(prompt, system_prompt=SYSTEM_PROMPT)
        logger.info(f"Agent D output: fut={result.get('fut_interpretation',{}).get('direction')}, stock={result.get('stock_interpretation',{}).get('direction')}, consistency={result.get('consistency')}")
        return result
    except Exception as e:
        logger.error(f"Agent D failed: {e}")
        return {
            "fut_interpretation": {"direction": "震荡", "reasoning": [f"Agent D 调用失败：{e}"]},
            "stock_interpretation": {"direction": "中性"},
            "consistency": "无法判断",
            "confidence": "低",
            "error": str(e),
        }


# 同步版本
def sync_translate_to_trade(
    agent_a_result: dict[str, Any],
    agent_c_result: dict[str, Any],
    commodity_name: str = "PTA",
    llm_client: LLMClient | None = None,
) -> dict[str, Any]:
    """同步包装"""
    return translate_to_trade(agent_a_result, agent_c_result, commodity_name, llm_client)

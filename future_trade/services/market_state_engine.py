"""
Agent A：数值因子解释引擎

输入：price_factor_daily 的最新一条数据（JSON格式）
输出：当前市场状态的结构化结论（偏多/偏空/中性 + 置信度 + 核心判断）

职责：不是预测价格，而是解释当前状态
"""
import json
import logging
from datetime import date
from typing import Any

from .llm_client import LLMClient

logger = logging.getLogger(__name__)

# Agent A 的系统提示词
SYSTEM_PROMPT = """你是一个PTA期货品种的数值因子分析师。

你的任务是根据最新的市场数据，解释当前市场状态，输出结构化结论。

核心原则：
1. 不是预测价格，而是解释"现在发生了什么"
2. 事实和推断强制分离
3. 用数据说话，不要猜测

输出格式（严格JSON）：
{
  "today_summary": "一句话描述今天的市场状态",
  "core_interpretation": "核心判断（如：现货强于期货，仓单增加压制盘面）",
  "direction": "偏多/偏空/中性",
  "confidence": "高/中/低",
  "basis_interpretation": {
    "current": "基差当前值",
    "trend": "5日变化方向",
    "verdict": "现货强于期货/期货强于现货/双方均衡"
  },
  "warehouse_receipt_interpretation": {
    "current": "仓单当前水平",
    "trend": "变化方向",
    "verdict": "产业资金在期货盘面做空（仓单增=偏空）/产业资金在囤货（仓单增=偏多）"
  },
  "term_structure": {
    "current": "back还是contango",
    "verdict": "现货偏紧/期货偏强/供需均衡"
  },
  "counter_evidence": ["风险点1", "风险点2"],
  "data_sources": ["数据项1", "数据项2"]
}
"""


def build_numeric_input(
    spot_price: float | None,
    fut_close: float | None,
    basis_main: float | None,
    basis_5d_change: float | None,
    basis_20d_change: float | None,
    basis_percentile_60: float | None,
    warehouse_receipt: int | None,
    wr_change: int | None,
    wr_5d_change: int | None,
    wr_percentile_60: float | None,
    time_spread: float | None,
    back_or_contango: str | None,
    trade_date: date | None = None,
) -> dict[str, Any]:
    """
    构建 Agent A 的输入 JSON。

    所有参数可为 None（表示数据缺失）。
    """
    return {
        "trade_date": str(trade_date) if trade_date else "未知",
        "spot_price": spot_price if spot_price is not None else "未知",
        "fut_close": fut_close if fut_close is not None else "未知",
        "basis_main": f"{basis_main:.0f}元/吨" if basis_main is not None else "未知",
        "basis_5d_change": f"{basis_5d_change:+.0f}元/吨" if basis_5d_change is not None else "未知",
        "basis_20d_change": f"{basis_20d_change:+.0f}元/吨" if basis_20d_change is not None else "未知",
        "basis_percentile_60": f"{basis_percentile_60:.0%}" if basis_percentile_60 is not None else "未知",
        "warehouse_receipt": f"{warehouse_receipt}张" if warehouse_receipt is not None else "未知",
        "wr_change": f"{wr_change:+d}张" if wr_change is not None else "未知",
        "wr_5d_change": f"{wr_5d_change:+d}张" if wr_5d_change is not None else "未知",
        "wr_percentile_60": f"{wr_percentile_60:.0%}" if wr_percentile_60 is not None else "未知",
        "time_spread": f"{time_spread:.0f}元/吨" if time_spread is not None else "未知",
        "back_or_contango": back_or_contango or "未知",
    }


def analyze_numeric_state(
    numeric_data: dict[str, Any],
    llm_client: LLMClient | None = None,
) -> dict[str, Any]:
    """
    Agent A 主函数：输入数值因子数据，返回状态解释。

    Args:
        numeric_data: build_numeric_input() 构建的字典
        llm_client: 可选，传入已有客户端

    Returns:
        Agent A 的结构化结论
    """
    if llm_client is None:
        llm_client = LLMClient()

    prompt = f"""你是一个PTA期货品种的数值因子分析师。根据以下最新数据，解释当前市场状态：

## 最新市场数据

- 交易日期：{numeric_data.get('trade_date')}
- 现货价格：{numeric_data.get('spot_price')}
- 期货收盘价：{numeric_data.get('fut_close')}
- 主力基差：{numeric_data.get('basis_main')}
- 基差5日变化：{numeric_data.get('basis_5d_change')}
- 基差20日变化：{numeric_data.get('basis_20d_change')}
- 基差60日分位：{numeric_data.get('basis_percentile_60')}
- 仓单量：{numeric_data.get('warehouse_receipt')}
- 仓单日变化：{numeric_data.get('wr_change')}
- 仓单5日变化：{numeric_data.get('wr_5d_change')}
- 仓单60日分位：{numeric_data.get('wr_percentile_60')}
- 月差（主力-近月）：{numeric_data.get('time_spread')}
- 期限结构：{numeric_data.get('back_or_contango')}

请分析：
1. 基差走向代表现货强还是期货强？
2. 仓单变化代表产业资金在做什么（套保出货还是囤货）？
3. 期限结构（back/contango）反映什么预期？
4. 综合判断当前市场是"现实偏紧"还是"预期偏强"还是"供需平衡"？

严格输出JSON（不要其他文字）：
{{
  "today_summary": "一句话",
  "core_interpretation": "核心判断",
  "direction": "偏多/偏空/中性",
  "confidence": "高/中/低",
  "basis_interpretation": {{
    "current": "当前基差值",
    "trend": "5日变化",
    "verdict": "现货强于期货/期货强于现货/双方均衡"
  }},
  "warehouse_receipt_interpretation": {{
    "current": "仓单水平",
    "trend": "变化方向",
    "verdict": "偏空解读/偏多解读/中性"
  }},
  "term_structure": {{
    "current": "back/contango",
    "verdict": "现货偏紧/期货偏强/供需均衡"
  }},
  "counter_evidence": ["风险点"],
  "data_sources": ["数据来源"]
}}"""

    try:
        result = llm_client.prompt(prompt, system_prompt=SYSTEM_PROMPT)
        logger.info(f"Agent A output: direction={result.get('direction')}, confidence={result.get('confidence')}")
        return result
    except Exception as e:
        logger.error(f"Agent A failed: {e}")
        return {
            "today_summary": f"Agent A 调用失败：{e}",
            "direction": "中性",
            "confidence": "低",
            "error": str(e),
        }


# 同步版本（供 sync 脚本调用）
def sync_analyze_numeric_state(
    numeric_data: dict[str, Any],
    llm_client: LLMClient | None = None,
) -> dict[str, Any]:
    """同步包装，兼容现有脚本"""
    return analyze_numeric_state(numeric_data, llm_client)

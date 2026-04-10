"""
公告分析服务

提供单篇公告分析和时序综合分析功能。
"""
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from db.connection import get_connection

from ..models.analysis_models import AnalysisResult, NoticeAnalysisCache
from .llm_client import get_llm_client

logger = logging.getLogger(__name__)

# prompts 目录路径
PROMPTS_DIR = Path(__file__).parent.parent.parent / "prompts"


def _load_prompt_template(filename: str) -> str:
    """加载提示词模板文件"""
    template_path = PROMPTS_DIR / filename
    if not template_path.exists():
        raise FileNotFoundError(f"Prompt template not found: {template_path}")
    return template_path.read_text(encoding="utf-8")


def _fetch_notice_by_art_code(art_code: str) -> dict | None:
    """从数据库获取公告详情"""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT art_code, stock_code, stock_name, title, notice_date,
                       f_node_name, s_node_name, full_text
                FROM announcements
                WHERE art_code = %s
                """,
                (art_code,),
            )
            row = cur.fetchone()
            if not row:
                return None
            cols = [
                "art_code", "stock_code", "stock_name", "title", "notice_date",
                "f_node_name", "s_node_name", "full_text",
            ]
            return dict(zip(cols, row))


def _get_recent_notices(
    days: int = 30,
    stock_code: str | None = None,
    limit: int = 100,
) -> list[dict]:
    """获取最近 N 天的公告列表"""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    with get_connection() as conn:
        with conn.cursor() as cur:
            if stock_code:
                cur.execute(
                    """
                    SELECT art_code, stock_code, stock_name, title, notice_date,
                           f_node_name, s_node_name
                    FROM announcements
                    WHERE notice_date >= %s AND stock_code = %s
                    ORDER BY notice_date DESC
                    LIMIT %s
                    """,
                    (cutoff, stock_code, limit),
                )
            else:
                cur.execute(
                    """
                    SELECT art_code, stock_code, stock_name, title, notice_date,
                           f_node_name, s_node_name
                    FROM announcements
                    WHERE notice_date >= %s
                    ORDER BY notice_date DESC
                    LIMIT %s
                    """,
                    (cutoff, limit),
                )
            rows = cur.fetchall()
            cols = [
                "art_code", "stock_code", "stock_name", "title", "notice_date",
                "f_node_name", "s_node_name",
            ]
            return [dict(zip(cols, row)) for row in rows]


def _get_cached_result(art_code: str, version: str = "v1") -> dict | None:
    """获取缓存的分析结果"""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT result FROM notice_analysis_cache
                WHERE art_code = %s AND prompt_version = %s
                """,
                (art_code, version),
            )
            row = cur.fetchone()
            return row[0] if row else None


def _save_single_result(
    art_code: str,
    result: dict,
    version: str = "v1",
) -> None:
    """保存单篇分析结果到缓存"""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO notice_analysis_cache (art_code, result, prompt_version)
                VALUES (%s, %s, %s)
                ON CONFLICT (art_code) DO UPDATE SET
                    result = EXCLUDED.result,
                    prompt_version = EXCLUDED.prompt_version,
                    analyzed_at = NOW()
                """,
                (art_code, result, version),
            )


def _save_analysis_result(
    analysis_type: str,
    llm_output: dict,
    input_data: dict,
    target_date: date | None = None,
    stock_code: str | None = None,
    art_code: str | None = None,
    version: str = "v1",
) -> None:
    """保存分析结果到 analysis_results 表"""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO analysis_results
                (analysis_type, target_date, stock_code, art_code,
                 input_data, llm_output, prompt_version)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    analysis_type,
                    target_date,
                    stock_code,
                    art_code,
                    input_data,
                    llm_output,
                    version,
                ),
            )


# ============ 公开接口 ============


def analyze_single_notice(art_code: str, use_cache: bool = True) -> dict[str, Any]:
    """
    分析单篇公告。

    Args:
        art_code: 公告 art_code
        use_cache: 是否使用缓存（默认 True）

    Returns:
        dict: 分析结果 JSON
    """
    # 检查缓存
    if use_cache:
        cached = _get_cached_result(art_code)
        if cached:
            logger.info(f"Using cached result for {art_code}")
            return cached

    # 获取公告内容
    notice = _fetch_notice_by_art_code(art_code)
    if not notice:
        raise ValueError(f"Notice not found: {art_code}")

    full_text = notice.get("full_text") or "（正文为空）"
    # 截取前 8000 字符避免超出 token 限制
    full_text = full_text[:8000]

    # 加载提示词
    template = _load_prompt_template("single_notice_v1.md")
    prompt = template.format(
        stock_name=notice["stock_name"],
        stock_code=notice["stock_code"],
        notice_date=notice["notice_date"],
        f_node_name=notice["f_node_name"] or "未分类",
        s_node_name=notice["s_node_name"] or "未分类",
        title=notice["title"],
        full_text=full_text,
    )

    # 调用 LLM
    client = get_llm_client()
    result = client.analyze_notice(prompt)

    # 保存缓存
    _save_single_result(art_code, result)

    # 保存分析记录
    _save_analysis_result(
        analysis_type="notice_single",
        llm_output=result,
        input_data={
            "art_code": art_code,
            "stock_code": notice["stock_code"],
            "notice_date": notice["notice_date"],
            "title": notice["title"],
        },
        target_date=datetime.strptime(notice["notice_date"], "%Y-%m-%d").date()
        if notice["notice_date"]
        else None,
        stock_code=notice["stock_code"],
        art_code=art_code,
    )

    return result


def analyze_recent_notices(
    days: int = 30,
    stock_code: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """
    批量分析最近 N 天的公告（单篇分析）。

    Args:
        days: 分析最近多少天内的公告
        stock_code: 可选，限定特定股票
        limit: 最多分析多少条

    Returns:
        list[dict]: 每条公告的分析结果
    """
    notices = _get_recent_notices(days=days, stock_code=stock_code, limit=limit)
    results = []
    for notice in notices:
        try:
            result = analyze_single_notice(notice["art_code"], use_cache=True)
            results.append(
                {
                    "notice": notice,
                    "analysis": result,
                }
            )
        except Exception as e:
            logger.error(f"Failed to analyze {notice['art_code']}: {e}")
            results.append(
                {
                    "notice": notice,
                    "analysis": None,
                    "error": str(e),
                }
            )
    return results


def analyze_time_series(
    days: int = 30,
    stock_code: str | None = None,
) -> dict[str, Any]:
    """
    时序综合分析：汇总最近 N 天所有公告的单篇分析结果，
    调用时序分析 Agent 得到趋势判断。

    Args:
        days: 分析最近多少天内的公告
        stock_code: 可选，限定特定股票

    Returns:
        dict: 时序分析结果 JSON
    """
    # 1. 先确保所有相关公告都有单篇分析结果
    notices = _get_recent_notices(days=days, stock_code=stock_code, limit=200)

    # 2. 收集已有分析结果 + 补充分析缺失的
    analysis_list = []
    for notice in notices:
        art_code = notice["art_code"]
        cached = _get_cached_result(art_code)
        if cached:
            analysis_list.append(
                {
                    "notice_date": notice["notice_date"],
                    "stock_name": notice["stock_name"],
                    "stock_code": notice["stock_code"],
                    "title": notice["title"],
                    "analysis": cached,
                }
            )
        else:
            # 实时分析（使用缓存）
            try:
                result = analyze_single_notice(art_code, use_cache=True)
                analysis_list.append(
                    {
                        "notice_date": notice["notice_date"],
                        "stock_name": notice["stock_name"],
                        "stock_code": notice["stock_code"],
                        "title": notice["title"],
                        "analysis": result,
                    }
                )
            except Exception as e:
                logger.error(f"Failed to analyze {art_code}: {e}")

    # 3. 构建时序分析输入
    import json

    analysis_text = json.dumps(analysis_list, ensure_ascii=False, indent=2)

    # 4. 加载时序提示词
    template = _load_prompt_template("time_series_v1.md")
    prompt = template.format(analysis_results=analysis_text)

    # 5. 调用 LLM
    client = get_llm_client()
    result = client.prompt(prompt)

    # 6. 保存时序分析结果
    _save_analysis_result(
        analysis_type="notice_timeseries",
        llm_output=result,
        input_data={
            "days": days,
            "stock_code": stock_code,
            "notices_count": len(analysis_list),
        },
        target_date=date.today(),
        stock_code=stock_code,
    )

    return result

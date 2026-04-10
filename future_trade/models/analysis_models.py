"""
AnalysisResult SQLAlchemy 模型

用于存储 LLM 公告分析结果和数据分析结果。
"""
from datetime import date, datetime
from typing import Optional

from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    Date,
    DateTime,
    Text,
    Index,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class AnalysisResult(Base):
    """
    分析结果存储表

    存储所有 LLM 分析结果（单篇公告、时序综合、数据分析等）
    """
    __tablename__ = "analysis_results"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # 分析类型: 'notice_single' / 'notice_timeseries' / 'data_daily'
    analysis_type = Column(String(50), nullable=False, index=True)

    # 关联日期（公告日期或数据日期）
    target_date = Column(Date, nullable=True, index=True)

    # 股票代码（个股分析时填写，全市场分析时为 null）
    stock_code = Column(String(10), nullable=True, index=True)

    # 关联的公告 art_code（单篇分析时填写）
    art_code = Column(String(100), nullable=True, index=True)

    # 输入数据快照（JSONB）
    input_data = Column(JSONB, nullable=True)

    # LLM 输出结果（JSONB）
    llm_output = Column(JSONB, nullable=False)

    # 提示词版本
    prompt_version = Column(String(20), nullable=False, default="v1")

    # 创建时间
    created_at = Column(DateTime, default=func.now(), nullable=False)

    __table_args__ = (
        Index("idx_analysis_type_date", "analysis_type", "target_date"),
        Index("idx_analysis_stock_date", "stock_code", "target_date"),
        Index("idx_analysis_art_code", "art_code"),
    )

    def __repr__(self) -> str:
        return (
            f"<AnalysisResult(id={self.id}, type={self.analysis_type}, "
            f"date={self.target_date}, stock={self.stock_code})>"
        )


class NoticeAnalysisCache(Base):
    """
    公告分析缓存表

    存储已完成的单篇公告分析结果，避免重复分析。
    与 announcements 表通过 art_code 关联。
    """
    __tablename__ = "notice_analysis_cache"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # 关联公告 art_code
    art_code = Column(String(100), nullable=False, unique=True, index=True)

    # 分析结果（JSONB）
    result = Column(JSONB, nullable=False)

    # 提示词版本
    prompt_version = Column(String(20), nullable=False, default="v1")

    # 分析时间
    analyzed_at = Column(DateTime, default=func.now(), nullable=False)

    def __repr__(self) -> str:
        return f"<NoticeAnalysisCache(art_code={self.art_code})>"

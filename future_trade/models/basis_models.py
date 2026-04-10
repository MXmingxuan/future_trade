"""基差因子模型

存储每日计算的基差数据：
- 现货价格 (来自 100ppi)
- 期货价格 (来自 Tushare fut_daily)
- 主力/近月合约映射 (来自 Tushare fut_mapping)
- 基差计算值
- 分位数统计
"""
from datetime import date
from typing import Optional
from sqlalchemy import Column, Integer, String, Float, Date, DateTime, UniqueConstraint, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import func

Base = declarative_base()


class BasisFactorDaily(Base):
    """基差因子日线数据表"""
    
    __tablename__ = "basis_factor_daily"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False, index=True)          # 品种代码，如 CU.SHFE
    trade_date = Column(Date, nullable=False, index=True)            # 交易日期
    
    # 现货价格
    spot_price = Column(Float, nullable=True)                        # 现货价格
    spot_source = Column(String(20), nullable=True, default='100ppi')  # 现货来源
    
    # 合约代码
    main_contract = Column(String(10), nullable=True)               # 当日主力合约代码
    near_contract = Column(String(10), nullable=True)               # 当日近月合约代码
    
    # 期货价格
    main_settle = Column(Float, nullable=True)                      # 主力结算价
    main_close = Column(Float, nullable=True)                       # 主力收盘价
    near_settle = Column(Float, nullable=True)                     # 近月结算价
    near_close = Column(Float, nullable=True)                       # 近月收盘价
    
    # 基差计算
    basis_main = Column(Float, nullable=True)                       # 主力基差 = spot - main_settle
    basis_near = Column(Float, nullable=True)                       # 近月基差 = spot - near_settle
    
    # 基差率 (%)
    basis_rate_main = Column(Float, nullable=True)                  # 主力基差率 = basis_main / spot × 100
    basis_rate_near = Column(Float, nullable=True)                  # 近月基差率 = basis_near / spot × 100
    
    # 分位数 (基于近月基差)
    basis_near_pct_250 = Column(Float, nullable=True)               # 近月基差250日分位数(%)
    basis_near_pct_500 = Column(Float, nullable=True)               # 近月基差500日分位数(%)
    basis_near_pct_750 = Column(Float, nullable=True)               # 近月基差750日分位数(%)
    
    # 审计字段
    calc_version = Column(String(10), nullable=True, default='v1')   # 计算版本
    created_at = Column(DateTime, default=func.now())               # 创建时间
    
    __table_args__ = (
        UniqueConstraint('symbol', 'trade_date', name='uix_basis_symbol_date'),
        Index('idx_basis_date', 'trade_date'),
        Index('idx_basis_symbol', 'symbol'),
    )


class BasisFactorLatest(Base):
    """基差因子最新快照（实体表，用于看板）
    
    注意：建议优先使用视图 basis_factor_latest_view
    此表仅当日更时手动维护
    """
    __tablename__ = "basis_factor_latest"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False, unique=True)         # 品种代码，唯一
    trade_date = Column(Date, nullable=False)                        # 最新交易日期
    
    # 现货价格
    spot_price = Column(Float, nullable=True)
    spot_source = Column(String(20), nullable=True)
    
    # 合约代码
    main_contract = Column(String(10), nullable=True)
    near_contract = Column(String(10), nullable=True)
    
    # 期货价格
    main_settle = Column(Float, nullable=True)
    main_close = Column(Float, nullable=True)
    near_settle = Column(Float, nullable=True)
    near_close = Column(Float, nullable=True)
    
    # 基差
    basis_main = Column(Float, nullable=True)
    basis_near = Column(Float, nullable=True)
    basis_rate_main = Column(Float, nullable=True)
    basis_rate_near = Column(Float, nullable=True)
    
    # 分位数
    basis_near_pct_250 = Column(Float, nullable=True)
    basis_near_pct_500 = Column(Float, nullable=True)
    basis_near_pct_750 = Column(Float, nullable=True)
    
    # 审计字段
    calc_version = Column(String(10), nullable=True, default='v1')
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

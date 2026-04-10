"""Tushare 数据模型 (修正版) - 对齐 Tushare API 实际返回字段"""
from datetime import date
from typing import Optional, List
from sqlalchemy import Column, Integer, String, Float, Date, DateTime, UniqueConstraint, Text, Index, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import func

Base = declarative_base()


class FutDaily(Base):
    """期货日线行情
    
    Tushare API: pro.fut_daily(ts_code, start_date, end_date)
    """
    __tablename__ = "tushare_fut_daily"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    
    pre_close = Column(Float, nullable=True)      # 昨收价
    pre_settle = Column(Float, nullable=True)    # 昨结算
    open = Column(Float, nullable=True)           # 开盘价
    high = Column(Float, nullable=True)           # 最高价
    low = Column(Float, nullable=True)             # 最低价
    close = Column(Float, nullable=True)           # 收盘价
    settle = Column(Float, nullable=True)          # 结算价
    change1 = Column(Float, nullable=True)         # 涨跌1
    change2 = Column(Float, nullable=True)         # 涨跌2
    vol = Column(Float, nullable=True)             # 成交量
    amount = Column(Float, nullable=True)          # 成交金额
    oi = Column(Float, nullable=True)              # 持仓量
    oi_chg = Column(Float, nullable=True)          # 持仓变化
    
    created_at = Column(DateTime, default=func.now())
    
    __table_args__ = (
        UniqueConstraint('ts_code', 'trade_date', name='uix_fut_daily_code_date'),
        Index('idx_fut_daily_date', 'trade_date'),
    )


class FutMapping(Base):
    """主力/连续合约映射
    
    Tushare API: pro.fut_mapping(trade_date)
    """
    __tablename__ = "tushare_fut_mapping"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    mapping_ts_code = Column(String(20), nullable=True)    # 映射到的合约代码
    
    created_at = Column(DateTime, default=func.now())
    
    __table_args__ = (
        UniqueConstraint('ts_code', 'trade_date', name='uix_fut_mapping_code_date'),
        Index('idx_fut_mapping_date', 'trade_date'),
    )


class FutHolding(Base):
    """持仓排名
    
    Tushare API: pro.fut_holding(symbol, trade_date)
    """
    __tablename__ = "tushare_fut_holding"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(Date, nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    broker = Column(String(100), nullable=True)          # 期货公司/交易商
    vol = Column(BigInteger, nullable=True)                # 成交量
    vol_chg = Column(BigInteger, nullable=True)           # 成交量变化
    long_hld = Column(BigInteger, nullable=True)           # 多头持仓量
    long_chg = Column(BigInteger, nullable=True)           # 多头持仓变化
    short_hld = Column(BigInteger, nullable=True)          # 空头持仓量
    short_chg = Column(BigInteger, nullable=True)          # 空头持仓变化
    
    created_at = Column(DateTime, default=func.now())
    
    __table_args__ = (
        Index('idx_fut_holding_date_symbol', 'trade_date', 'symbol'),
    )


class FutWsr(Base):
    """仓单日报
    
    Tushare API: pro.fut_wsr(symbol, start_date, end_date)
    """
    __tablename__ = "tushare_fut_wsr"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(Date, nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    fut_name = Column(String(50), nullable=True)          # 品种名称
    warehouse = Column(String(100), nullable=True)         # 仓库
    pre_vol = Column(BigInteger, nullable=True)            # 昨日仓单
    vol = Column(BigInteger, nullable=True)                # 今日仓单
    vol_chg = Column(BigInteger, nullable=True)            # 仓单变化
    unit = Column(String(20), nullable=True)               # 计量单位
    
    created_at = Column(DateTime, default=func.now())
    
    __table_args__ = (
        Index('idx_fut_wsr_date_symbol', 'trade_date', 'symbol'),
    )


class FutSettle(Base):
    """结算参数
    
    Tushare API: pro.fut_settle(ts_code, trade_date)
    """
    __tablename__ = "tushare_fut_settle"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    
    settle = Column(Float, nullable=True)                 # 结算价
    trading_fee_rate = Column(Float, nullable=True)        # 交易费率
    trading_fee = Column(Float, nullable=True)             # 交易费
    delivery_fee = Column(Float, nullable=True)            # 交割费
    b_hedging_margin_rate = Column(Float, nullable=True)   # 买套保保证金率
    s_hedging_margin_rate = Column(Float, nullable=True)  # 卖套保保证金率
    long_margin_rate = Column(Float, nullable=True)        # 多头保证金率
    short_margin_rate = Column(Float, nullable=True)       # 空头保证金率
    
    created_at = Column(DateTime, default=func.now())
    
    __table_args__ = (
        UniqueConstraint('ts_code', 'trade_date', name='uix_fut_settle_code_date'),
        Index('idx_fut_settle_date', 'trade_date'),
    )

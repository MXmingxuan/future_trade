"""数据模型"""
from datetime import date
from typing import Optional, List
from pydantic import BaseModel, Field
from sqlalchemy import Column, Integer, String, Float, Date, DateTime, UniqueConstraint, Text, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from tqdm import tqdm

Base = declarative_base()


class Commodity(Base):
    """商品目录表"""
    __tablename__ = "commodities"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), nullable=False)          # 商品名称
    code = Column(String(20), nullable=True)           # 交易所代码
    exchange = Column(String(20), nullable=False)      # 交易所: SHFE/ZCE/DCE/GFE
    unit = Column(String(20), default='元/吨')        # 计量单位
    notes = Column(Text, nullable=True)                # 特殊说明
    detail_url = Column(String(200), nullable=True)   # 详情页URL

    __table_args__ = (
        UniqueConstraint('name', 'exchange', name='uix_name_exchange'),
    )


class PriceData(Base):
    """价格数据表"""
    __tablename__ = "price_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    commodity_id = Column(Integer, nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)

    # 现货
    spot_price = Column(Float, nullable=True)

    # 最近合约
    near_contract = Column(String(10), nullable=True)
    near_price = Column(Float, nullable=True)
    near_diff = Column(Float, nullable=True)
    near_diff_pct = Column(Float, nullable=True)

    # 主力合约
    main_contract = Column(String(10), nullable=True)
    main_price = Column(Float, nullable=True)
    main_diff = Column(Float, nullable=True)
    main_diff_pct = Column(Float, nullable=True)

    created_at = Column(DateTime, default=func.now())

    __table_args__ = (
        UniqueConstraint('commodity_id', 'trade_date', name='uix_commodity_date'),
    )


class CrawlLog(Base):
    """抓取日志表"""
    __tablename__ = "crawl_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    crawl_date = Column(Date, nullable=False, unique=True)
    status = Column(String(20), nullable=False)        # success/failed
    records_count = Column(Integer, default=0)
    error_msg = Column(Text, nullable=True)
    crawled_at = Column(DateTime, default=func.now())


# ============ Pydantic 模型 (用于数据校验) ============

class CommodityBase(BaseModel):
    name: str
    code: Optional[str] = None
    exchange: str
    unit: str = '元/吨'
    notes: Optional[str] = None
    detail_url: Optional[str] = None


class CommodityCreate(CommodityBase):
    pass


class CommodityResponse(CommodityBase):
    id: int

    class Config:
        from_attributes = True


class PriceDataBase(BaseModel):
    commodity_id: int
    trade_date: date
    spot_price: Optional[float] = None
    near_contract: Optional[str] = None
    near_price: Optional[float] = None
    near_diff: Optional[float] = None
    near_diff_pct: Optional[float] = None
    main_contract: Optional[str] = None
    main_price: Optional[float] = None
    main_diff: Optional[float] = None
    main_diff_pct: Optional[float] = None


class PriceDataCreate(PriceDataBase):
    pass


class PriceDataResponse(PriceDataBase):
    id: int

    class Config:
        from_attributes = True


# ============ 数据库操作辅助函数 ============

def get_commodity_id(db: Session, name: str, exchange: str) -> Optional[int]:
    """根据名称和交易所获取商品ID"""
    commodity = db.query(Commodity).filter(
        Commodity.name == name,
        Commodity.exchange == exchange
    ).first()
    return commodity.id if commodity else None


def get_or_create_commodity(db: Session, commodity_data: CommodityCreate) -> int:
    """获取或创建商品，返回ID"""
    commodity = db.query(Commodity).filter(
        Commodity.name == commodity_data.name,
        Commodity.exchange == commodity_data.exchange
    ).first()
    
    if commodity:
        return commodity.id
    
    commodity = Commodity(**commodity_data.model_dump())
    db.add(commodity)
    db.commit()
    db.refresh(commodity)
    return commodity.id


def bulk_upsert_price_data(db: Session, price_records: List[dict], 
                           commodity_map: dict, trade_date: date,
                           show_progress: bool = True,
                           detail_url_map: dict = None) -> int:
    """
    批量插入或更新价格数据
    
    Args:
        db: 数据库会话
        price_records: 价格记录列表
        commodity_map: {商品名称: commodity_id} 映射
        trade_date: 交易日期
        show_progress: 是否显示进度条
        detail_url_map: {detail_url: commodity_id} 映射 (可选)
    
    Returns:
        插入/更新的记录数
    """
    count = 0
    records_iter = tqdm(price_records, desc=f"插入 {trade_date} 数据") if show_progress else price_records
    
    for record in records_iter:
        commodity_name = record.get('name')
        commodity_id = commodity_map.get(commodity_name)
        
        # 如果名称匹配失败，尝试用 detail_url 匹配
        if not commodity_id and detail_url_map:
            detail_url = record.get('detail_url')
            if detail_url:
                commodity_id = detail_url_map.get(detail_url)
        
        if not commodity_id:
            continue
        
        # 检查是否已存在
        existing = db.query(PriceData).filter(
            PriceData.commodity_id == commodity_id,
            PriceData.trade_date == trade_date
        ).first()
        
        if existing:
            # 更新
            for key, value in record.items():
                if key != 'name' and hasattr(existing, key):
                    setattr(existing, key, value)
        else:
            # 插入
            new_record = PriceData(
                commodity_id=commodity_id,
                trade_date=trade_date,
                spot_price=record.get('spot_price'),
                near_contract=record.get('near_contract'),
                near_price=record.get('near_price'),
                near_diff=record.get('near_diff'),
                near_diff_pct=record.get('near_diff_pct'),
                main_contract=record.get('main_contract'),
                main_price=record.get('main_price'),
                main_diff=record.get('main_diff'),
                main_diff_pct=record.get('main_diff_pct'),
            )
            db.add(new_record)
        
        count += 1
    
    db.commit()
    return count

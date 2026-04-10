"""按品种导出CSV脚本

导出每个品种的价格数据到单独的CSV文件
CSV格式: 日期, 现货价格, 近月合约, 近月价格, 近月差%, 主力合约, 主力价格, 主力现期差%
"""
import csv
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.database import DATABASE_PATH
from models.models import Commodity, PriceData


def export_by_commodity():
    """导出每个品种到单独的CSV"""
    
    # 连接数据库
    engine = create_engine(f"sqlite:///{DATABASE_PATH}")
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # 创建导出目录
    export_dir = DATABASE_PATH.parent / "exports_by_commodity"
    export_dir.mkdir(parents=True, exist_ok=True)
    
    # 获取所有商品
    commodities = session.query(Commodity).order_by(Commodity.exchange, Commodity.name).all()
    
    print(f"找到 {len(commodities)} 个商品，开始导出...")
    
    # 按品种导出
    for commodity in commodities:
        # 获取该品种的所有价格数据，按日期升序
        price_records = session.query(PriceData).filter(
            PriceData.commodity_id == commodity.id
        ).order_by(PriceData.trade_date).all()
        
        if not price_records:
            print(f"  [跳过] {commodity.name} ({commodity.exchange}) - 无数据")
            continue
        
        # 生成文件名: 品种名_交易所.csv
        filename = f"{commodity.name}_{commodity.exchange}.csv"
        filepath = export_dir / filename
        
        # 写入CSV
        with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            
            # 写入表头
            writer.writerow([
                '日期',
                '现货价格',
                '近月合约',
                '近月价格',
                '近月差%',
                '主力合约',
                '主力价格',
                '主力现期差%'
            ])
            
            # 写入数据行
            for record in price_records:
                writer.writerow([
                    record.trade_date.strftime('%Y-%m-%d'),
                    record.spot_price if record.spot_price is not None else '',
                    record.near_contract or '',
                    record.near_price if record.near_price is not None else '',
                    f"{record.near_diff_pct:.2f}" if record.near_diff_pct is not None else '',
                    record.main_contract or '',
                    record.main_price if record.main_price is not None else '',
                    f"{record.main_diff_pct:.2f}" if record.main_diff_pct is not None else '',
                ])
        
        print(f"  [导出] {commodity.name} ({commodity.exchange}) - {len(price_records)} 条记录 -> {filename}")
    
    session.close()
    
    print(f"\n完成！导出目录: {export_dir}")
    print(f"共导出 {len(commodities)} 个品种的CSV文件")


if __name__ == "__main__":
    export_by_commodity()
"""初始化数据库表结构和基础数据"""
import sys
from pathlib import Path

# 添加项目根目录到 path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.models import Base, Commodity, CrawlLog
from config.database import DATABASE_PATH


def init_database():
    """初始化数据库"""
    # 创建数据库引擎
    engine = create_engine(f'sqlite:///{DATABASE_PATH}')
    
    # 创建所有表
    Base.metadata.create_all(engine)
    print(f"[OK] 数据库表创建成功: {DATABASE_PATH}")
    
    # 创建会话
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # 检查是否已有商品数据
    existing_count = session.query(Commodity).count()
    if existing_count > 0:
        print(f"[OK] 商品目录已存在 ({existing_count} 条)")
        session.close()
        return engine
    
    # 插入商品目录数据
    commodities = [
        # 上海期货交易所 (SHFE)
        {"name": "铜", "code": "CU", "exchange": "SHFE", "unit": "元/吨", "detail_url": "/sf/792.html"},
        {"name": "螺纹钢", "code": "RB", "exchange": "SHFE", "unit": "元/吨", "detail_url": "/sf/927.html"},
        {"name": "锌", "code": "ZN", "exchange": "SHFE", "unit": "元/吨", "detail_url": "/sf/826.html"},
        {"name": "铝", "code": "AL", "exchange": "SHFE", "unit": "元/吨", "detail_url": "/sf/827.html"},
        {"name": "黄金", "code": "AU", "exchange": "SHFE", "unit": "元/克", "detail_url": "/sf/551.html"},
        {"name": "线材", "code": "WR", "exchange": "SHFE", "unit": "元/吨", "detail_url": "/sf/740.html"},
        {"name": "燃料油", "code": "FU", "exchange": "SHFE", "unit": "元/吨", "detail_url": "/sf/387.html"},
        {"name": "天然橡胶", "code": "RU", "exchange": "SHFE", "unit": "元/吨", "detail_url": "/sf/586.html"},
        {"name": "铅", "code": "PB", "exchange": "SHFE", "unit": "元/吨", "detail_url": "/sf/825.html"},
        {"name": "白银", "code": "AG", "exchange": "SHFE", "unit": "元/千克", "detail_url": "/sf/544.html"},
        {"name": "石油沥青", "code": "BU", "exchange": "SHFE", "unit": "元/吨", "detail_url": "/sf/1022.html"},
        {"name": "热轧卷板", "code": "HC", "exchange": "SHFE", "unit": "元/吨", "detail_url": "/sf/195.html"},
        {"name": "镍", "code": "NI", "exchange": "SHFE", "unit": "元/吨", "detail_url": "/sf/1182.html"},
        {"name": "锡", "code": "SN", "exchange": "SHFE", "unit": "元/吨", "detail_url": "/sf/1181.html"},
        {"name": "纸浆", "code": "SP", "exchange": "SHFE", "unit": "元/吨", "detail_url": "/sf/1053.html"},
        {"name": "不锈钢", "code": "SS", "exchange": "SHFE", "unit": "元/吨", "detail_url": "/sf/1300.html"},
        {"name": "丁二烯橡胶", "code": "BR", "exchange": "SHFE", "unit": "元/吨", "detail_url": "/sf/358.html"},
        
        # 郑州商品交易所 (ZCE)
        {"name": "PTA", "code": "TA", "exchange": "ZCE", "unit": "元/吨", "detail_url": "/sf/356.html"},
        {"name": "白糖", "code": "SR", "exchange": "ZCE", "unit": "元/吨", "detail_url": "/sf/564.html"},
        {"name": "棉花", "code": "CF", "exchange": "ZCE", "unit": "元/吨", "detail_url": "/sf/344.html"},
        {"name": "菜籽油OI", "code": "OI", "exchange": "ZCE", "unit": "元/吨", "detail_url": "/sf/810.html"},
        {"name": "玻璃", "code": "FG", "exchange": "ZCE", "unit": "元/平方米", "detail_url": "/sf/959.html"},
        {"name": "菜籽粕", "code": "RM", "exchange": "ZCE", "unit": "元/吨", "detail_url": "/sf/1014.html"},
        {"name": "硅铁", "code": "SF", "exchange": "ZCE", "unit": "元/吨", "detail_url": "/sf/1154.html"},
        {"name": "锰硅", "code": "SM", "exchange": "ZCE", "unit": "元/吨", "detail_url": "/sf/1155.html"},
        {"name": "甲醇MA", "code": "MA", "exchange": "ZCE", "unit": "元/吨", "detail_url": "/sf/817.html"},
        {"name": "棉纱", "code": "CY", "exchange": "ZCE", "unit": "元/吨", "detail_url": "/sf/1258.html"},
        {"name": "尿素", "code": "UR", "exchange": "ZCE", "unit": "元/吨", "detail_url": "/sf/89.html"},
        {"name": "纯碱", "code": "SA", "exchange": "ZCE", "unit": "元/吨", "detail_url": "/sf/737.html"},
        {"name": "涤纶短纤", "code": "PF", "exchange": "ZCE", "unit": "元/吨", "detail_url": "/sf/976.html"},
        {"name": "PX", "code": "PX", "exchange": "ZCE", "unit": "元/吨", "detail_url": "/sf/968.html"},
        {"name": "烧碱", "code": "SH", "exchange": "ZCE", "unit": "元/吨", "detail_url": "/sf/368.html", 
         "notes": "32%液碱"},
        {"name": "瓶片", "code": "FF", "exchange": "ZCE", "unit": "元/吨", "detail_url": "/sf/173.html"},
        {"name": "丙烯", "code": "PP", "exchange": "ZCE", "unit": "元/吨", "detail_url": "/sf/505.html"},
        
        # 大连商品交易所 (DCE)
        {"name": "棕榈油", "code": "P", "exchange": "DCE", "unit": "元/吨", "detail_url": "/sf/1084.html"},
        {"name": "聚氯乙烯", "code": "V", "exchange": "DCE", "unit": "元/吨", "detail_url": "/sf/107.html"},
        {"name": "聚乙烯", "code": "L", "exchange": "DCE", "unit": "元/吨", "detail_url": "/sf/435.html"},
        {"name": "豆一", "code": "A", "exchange": "DCE", "unit": "元/吨", "detail_url": "/sf/1080.html"},
        {"name": "豆粕", "code": "M", "exchange": "DCE", "unit": "元/吨", "detail_url": "/sf/312.html"},
        {"name": "豆油", "code": "Y", "exchange": "DCE", "unit": "元/吨", "detail_url": "/sf/403.html"},
        {"name": "玉米", "code": "C", "exchange": "DCE", "unit": "元/吨", "detail_url": "/sf/274.html"},
        {"name": "焦炭", "code": "J", "exchange": "DCE", "unit": "元/吨", "detail_url": "/sf/346.html",
         "notes": "一级冶金焦"},
        {"name": "焦煤", "code": "JM", "exchange": "DCE", "unit": "元/吨", "detail_url": "/sf/1121.html"},
        {"name": "铁矿石", "code": "I", "exchange": "DCE", "unit": "元/吨", "detail_url": "/sf/961.html",
         "notes": "现货湿吨，期货干吨"},
        {"name": "鸡蛋", "code": "JD", "exchange": "DCE", "unit": "元/公斤", "detail_url": "/sf/1049.html"},
        {"name": "聚丙烯", "code": "PP", "exchange": "DCE", "unit": "元/吨", "detail_url": "/sf/718.html"},
        {"name": "乙二醇", "code": "EG", "exchange": "DCE", "unit": "元/吨", "detail_url": "/sf/222.html"},
        {"name": "苯乙烯", "code": "EB", "exchange": "DCE", "unit": "元/吨", "detail_url": "/sf/168.html"},
        {"name": "液化石油气", "code": "PG", "exchange": "DCE", "unit": "元/吨", "detail_url": "/sf/158.html"},
        {"name": "生猪", "code": "LH", "exchange": "DCE", "unit": "元/吨", "detail_url": "/sf/936.html"},
        {"name": "纯苯", "code": "EB", "exchange": "DCE", "unit": "元/吨", "detail_url": "/sf/120.html"},
        
        # 广州期货交易所 (GFE)
        {"name": "工业硅", "code": "SI", "exchange": "GFE", "unit": "元/吨", "detail_url": "/sf/238.html"},
        {"name": "碳酸锂", "code": "LC", "exchange": "GFE", "unit": "元/吨", "detail_url": "/sf/1162.html"},
        {"name": "多晶硅", "code": "PS", "exchange": "GFE", "unit": "元/吨", "detail_url": "/sf/463.html"},
    ]
    
    for comm in commodities:
        session.add(Commodity(**comm))
    
    session.commit()
    print(f"[OK] 商品目录初始化完成 ({len(commodities)} 条)")
    
    session.close()
    return engine


if __name__ == "__main__":
    init_database()

"""每日增量爬取脚本 - 抓取当天最新数据"""
import sys
from pathlib import Path
from datetime import date

# 添加项目根目录到 path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config.database import DATABASE_PATH
from services.crawler import Crawler


def crawl_daily():
    """爬取当天最新数据"""
    today = date.today()
    
    print(f"=" * 50)
    print(f"开始爬取当日数据: {today}")
    print(f"=" * 50)
    print()
    
    # 创建数据库连接
    engine = create_engine(f'sqlite:///{DATABASE_PATH}')
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # 创建爬虫实例
    crawler = Crawler()
    
    # 爬取当天数据
    success = crawler.crawl_date(session, today)
    
    print()
    print(f"=" * 50)
    if success:
        print(f"✓ 当日数据爬取成功!")
    else:
        print(f"✗ 当日数据爬取失败 (可能数据尚未发布)")
    print(f"=" * 50)
    
    session.close()


if __name__ == "__main__":
    crawl_daily()

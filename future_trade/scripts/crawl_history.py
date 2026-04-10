"""历史数据爬取脚本 - 抓取过去30天的数据"""
import sys
from pathlib import Path
from datetime import date, timedelta

# 添加项目根目录到 path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config.database import DATABASE_PATH
from services.crawler import Crawler


def crawl_history(days: int = 30):
    """
    爬取历史数据
    
    Args:
        days: 往前抓取的天数
    """
    print(f"=" * 50)
    print(f"开始爬取过去 {days} 天的历史数据")
    print(f"=" * 50)
    
    # 创建数据库连接
    engine = create_engine(f'sqlite:///{DATABASE_PATH}')
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # 创建爬虫实例
    crawler = Crawler()
    
    # 计算日期范围 (不包含今天)
    end_date = date.today() - timedelta(days=1)  # 昨天
    start_date = end_date - timedelta(days=days - 1)  # 30天前
    
    print(f"日期范围: {start_date} ~ {end_date}")
    print()
    
    # 开始爬取
    results = crawler.crawl_date_range(session, start_date, end_date)
    
    print()
    print(f"=" * 50)
    print(f"爬取完成!")
    print(f"成功: {results['success']} 天")
    print(f"失败: {results['failed']} 天")
    print(f"=" * 50)
    
    session.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='爬取历史现期表数据')
    parser.add_argument('--days', type=int, default=30, help='往前抓取的天数 (默认30天)')
    args = parser.parse_args()
    
    crawl_history(args.days)

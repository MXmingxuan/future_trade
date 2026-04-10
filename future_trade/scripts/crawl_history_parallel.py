"""多进程并行历史数据爬虫 - 支持断点续爬"""
import sys
import os
import json
import time
from datetime import date, timedelta
from multiprocessing import Pool, Manager, cpu_count
from pathlib import Path

# 添加项目根目录到 path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config.database import DATABASE_PATH


# 全局配置
NUM_WORKERS = 10  # 并行进程数
CHECKPOINT_FILE = Path(__file__).parent.parent / "data" / "crawl_checkpoints.json"
PROGRESS_INTERVAL = 10  # 每爬取多少天报告一次进度


def load_checkpoint() -> set:
    """加载断点集合"""
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return set(data.get('completed_dates', []))
    return set()


def save_checkpoint(completed_dates: set):
    """保存断点"""
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump({'completed_dates': list(completed_dates)}, f)


def crawl_single_date(args):
    """
    爬取单个日期的数据 (在子进程中执行)
    
    Args:
        args: (trade_date_str, db_path, progress_counter, lock)
    
    Returns:
        (date_str, success, records_count, error_msg)
    """
    trade_date_str, db_path = args
    
    try:
        trade_date = date.fromisoformat(trade_date_str)
        
        # 每个子进程创建自己的浏览器实例
        from playwright.sync_api import sync_playwright
        from services.parser import parse_sf_table
        from models.models import PriceData, CrawlLog, Commodity, bulk_upsert_price_data
        
        # 创建数据库连接
        engine = create_engine(f'sqlite:///{db_path}')
        Session = sessionmaker(bind=engine)
        session = Session()
        
        try:
            # 检查是否已爬取
            existing = session.query(CrawlLog).filter(
                CrawlLog.crawl_date == trade_date,
                CrawlLog.status == 'success'
            ).first()
            
            if existing:
                session.close()
                return (trade_date_str, True, 0, 'already_crawled')
            
            # 使用 Playwright 获取页面
            url = f'https://www.100ppi.com/sf/day-{trade_date.strftime("%Y-%m-%d")}.html'
            
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                )
                page = context.new_page()
                
                try:
                    response = page.goto(url, timeout=60000, wait_until='networkidle')
                    if response is None or response.status >= 400:
                        raise Exception(f'HTTP {response.status if response else "No response"}')
                    
                    time.sleep(0.5)  # 等待页面稳定
                    html = page.content()
                finally:
                    browser.close()
            
            # 解析数据
            records = parse_sf_table(html, trade_date)
            
            if not records:
                # 记录失败
                log = CrawlLog(
                    crawl_date=trade_date,
                    status='failed',
                    records_count=0,
                    error_msg='No data parsed'
                )
                session.add(log)
                session.commit()
                session.close()
                return (trade_date_str, False, 0, 'No data')
            
            # 保存数据
            commodity_map = {c.name: c.id for c in session.query(Commodity).all()}
            count = bulk_upsert_price_data(
                session, records, commodity_map, trade_date, 
                show_progress=False
            )
            
            # 记录成功
            log = CrawlLog(
                crawl_date=trade_date,
                status='success',
                records_count=count,
                error_msg=None
            )
            session.add(log)
            session.commit()
            session.close()
            
            return (trade_date_str, True, count, None)
            
        except Exception as e:
            session.rollback()
            session.close()
            return (trade_date_str, False, 0, str(e))
            
    except Exception as e:
        return (trade_date_str, False, 0, f'Parse error: {e}')


def generate_date_range(start_date: date, end_date: date) -> list:
    """生成日期范围内的所有工作日"""
    dates = []
    current = end_date
    while current >= start_date:
        # 跳过周末 (5=Saturday, 6=Sunday)
        if current.weekday() < 5:
            dates.append(current)
        current -= timedelta(days=1)
    return dates


def crawl_history_parallel(start_date: date, end_date: date, num_workers: int = NUM_WORKERS):
    """
    并行爬取历史数据
    
    Args:
        start_date: 开始日期
        end_date: 结束日期
        num_workers: 并行进程数
    """
    print("=" * 60)
    print(f"并行爬取历史数据")
    print(f"日期范围: {start_date} ~ {end_date}")
    print(f"并行进程数: {num_workers}")
    print("=" * 60)
    
    # 加载断点
    completed = load_checkpoint()
    print(f"已完成的日期数: {len(completed)}")
    
    # 生成需要爬取的日期
    all_dates = generate_date_range(start_date, end_date)
    print(f"总共需要爬取: {len(all_dates)} 个工作日")
    
    # 过滤掉已完成的
    dates_to_crawl = [d for d in all_dates if d.isoformat() not in completed]
    print(f"还需爬取: {len(dates_to_crawl)} 个工作日")
    
    if not dates_to_crawl:
        print("所有日期已爬取完成！")
        return
    
    # 准备参数
    args_list = [
        (d.isoformat(), str(DATABASE_PATH)) 
        for d in dates_to_crawl
    ]
    
    # 统计
    success_count = 0
    fail_count = 0
    total_records = 0
    fail_dates = []
    
    print(f"\n开始爬取...")
    print("-" * 60)
    
    # 使用进程池并行爬取
    with Pool(processes=num_workers) as pool:
        # imap_unordered 返回迭代器，可以边爬取边处理结果
        for i, (date_str, success, count, error) in enumerate(
            pool.imap_unordered(crawl_single_date, args_list, chunksize=5)
        ):
            if success:
                success_count += 1
                total_records += count
                completed.add(date_str)
                
                # 每隔10个打印进度
                if (success_count + fail_count) % 10 == 0:
                    progress = (success_count + fail_count) / len(dates_to_crawl) * 100
                    print(f"进度: {success_count + fail_count}/{len(dates_to_crawl)} ({progress:.1f}%) | 成功: {success_count} | 失败: {fail_count}")
            else:
                fail_count += 1
                fail_dates.append((date_str, error))
                print(f"[失败] {date_str}: {error[:50] if error else 'Unknown'}")
            
            # 每完成50个保存一次断点
            if (success_count + fail_count) % 50 == 0:
                save_checkpoint(completed)
    
    # 最终保存断点
    save_checkpoint(completed)
    
    print()
    print("=" * 60)
    print(f"爬取完成!")
    print(f"成功: {success_count} 天, {total_records} 条记录")
    print(f"失败: {fail_count} 天")
    
    if fail_dates:
        print(f"\n失败的日期 ({len(fail_dates)}):")
        for date_str, error in fail_dates[:20]:
            print(f"  {date_str}: {error[:60] if error else 'Unknown'}")
        if len(fail_dates) > 20:
            print(f"  ... 还有 {len(fail_dates) - 20} 个")
    
    print("=" * 60)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='并行爬取历史现期表数据')
    parser.add_argument('--start', type=str, default='2020-01-01', 
                        help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, default=None, 
                        help='结束日期 (YYYY-MM-DD), 默认昨天')
    parser.add_argument('--workers', type=int, default=NUM_WORKERS, 
                        help=f'并行进程数 (默认{NUM_WORKERS})')
    args = parser.parse_args()
    
    start_date = date.fromisoformat(args.start)
    end_date = date.fromisoformat(args.end) if args.end else date.today() - timedelta(days=1)
    
    crawl_history_parallel(start_date, end_date, args.workers)

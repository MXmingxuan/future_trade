"""Tushare 数据爬虫 (修正版)

爬取国内五大期货交易所的期货数据:
- CFFEX (中金所): 股指期货
- DCE (大商所): 豆粕、玉米、铁矿石等
- CZCE (郑商所): PTA、白糖、棉花等
- SHFE (上期所): 铜、铝、黄金、螺纹钢等
- GFEX (广期所): 工业硅、碳酸锂等

数据范围: 2020-01-01 至今
"""
import sys
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import List, Optional
import time

import tushare as ts
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.tushare_config import TUSHARE_TOKEN
from config.database import DATABASE_PATH
from models.tushare_models import (
    Base, FutDaily, FutMapping, FutWsr
)


# 五大交易所 (注意: 中金所用 CFFEX，广期所用 GFEX)
EXCHANGES = ['CFFEX', 'DCE', 'CZCE', 'SHFE', 'GFEX']


class TushareCrawler:
    def __init__(self, token: str = TUSHARE_TOKEN):
        self.token = token
        self.pro = ts.pro_api(token)
        self.engine = create_engine(f'sqlite:///{DATABASE_PATH}')
        self.Session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)
    
    def get_trade_dates(self, start_date: str, end_date: str) -> List[str]:
        """获取交易日历"""
        try:
            df = self.pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date)
            if df is None or df.empty:
                return []
            return sorted(df[df['is_open'] == 1]['cal_date'].tolist())
        except Exception as e:
            print(f"获取交易日历失败: {e}")
            return []
    
    def get_fut_codes(self, exchange: str = None) -> List[str]:
        """获取期货合约列表"""
        try:
            df = self.pro.fut_basic(exchange=exchange, fut_type='1')
            if df is None or df.empty:
                return []
            return df['ts_code'].tolist()
        except Exception as e:
            print(f"获取合约列表失败 ({exchange}): {e}")
            return []
    
    def get_fut_symbols(self, exchange: str = None) -> List[str]:
        """获取期货品种列表（用于持仓、仓单等）"""
        try:
            df = self.pro.fut_basic(exchange=exchange, fut_type='1')
            if df is None or df.empty:
                return []
            # 提取品种代码（去掉月份）
            symbols = set()
            for ts_code in df['ts_code']:
                # 例如 RB2501.SHF -> RB
                symbol = ts_code.split('.')[0]
                if len(symbol) <= 4:  # 品种代码通常2-4位
                    symbols.add(symbol)
            return sorted(list(symbols))
        except Exception as e:
            print(f"获取品种列表失败 ({exchange}): {e}")
            return []
    
    # ============ 数据获取方法 ============
    
    def fetch_fut_daily(self, ts_code: str, start_date: str, end_date: str) -> List[dict]:
        """获取期货日线行情"""
        try:
            df = self.pro.fut_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if df is None or df.empty:
                return []
            records = []
            for _, row in df.iterrows():
                trade_date = datetime.strptime(str(row['trade_date']), '%Y%m%d').date()
                records.append({
                    'ts_code': row['ts_code'],
                    'trade_date': trade_date,
                    'pre_close': row.get('pre_close'),
                    'pre_settle': row.get('pre_settle'),
                    'open': row.get('open'),
                    'high': row.get('high'),
                    'low': row.get('low'),
                    'close': row.get('close'),
                    'settle': row.get('settle'),
                    'change1': row.get('change1'),
                    'change2': row.get('change2'),
                    'vol': row.get('vol'),
                    'amount': row.get('amount'),
                    'oi': row.get('oi'),
                    'oi_chg': row.get('oi_chg'),
                })
            return records
        except Exception as e:
            return []
    
    def fetch_fut_mapping(self, trade_date: str) -> List[dict]:
        """获取主力/连续合约映射"""
        try:
            df = self.pro.fut_mapping(trade_date=trade_date)
            if df is None or df.empty:
                return []
            records = []
            for _, row in df.iterrows():
                td = datetime.strptime(str(row['trade_date']), '%Y%m%d').date()
                records.append({
                    'ts_code': row['ts_code'],
                    'trade_date': td,
                    'mapping_ts_code': row.get('mapping_ts_code'),
                })
            return records
        except Exception as e:
            return []
    
    def fetch_fut_wsr(self, symbol: str, start_date: str, end_date: str) -> List[dict]:
        """获取仓单日报"""
        try:
            df = self.pro.fut_wsr(symbol=symbol, start_date=start_date, end_date=end_date)
            if df is None or df.empty:
                return []
            records = []
            for _, row in df.iterrows():
                td = datetime.strptime(str(row['trade_date']), '%Y%m%d').date()
                records.append({
                    'trade_date': td,
                    'symbol': row.get('symbol'),
                    'fut_name': row.get('fut_name'),
                    'warehouse': row.get('warehouse'),
                    'pre_vol': row.get('pre_vol'),
                    'vol': row.get('vol'),
                    'vol_chg': row.get('vol_chg'),
                    'unit': row.get('unit'),
                })
            return records
        except Exception as e:
            return []
    
    
    
    # ============ 数据保存方法 ============
    
    def save_records(self, model_class, records: List[dict], session=None) -> int:
        """通用保存方法"""
        if not records:
            return 0
        
        should_close = session is None
        if session is None:
            session = self.Session()
        
        count = 0
        try:
            for record in records:
                # 构造查询条件
                filter_args = {}
                for key in record:
                    if key in ['ts_code', 'trade_date', 'symbol']:
                        filter_args[key] = record[key]
                
                existing = session.query(model_class).filter_by(**filter_args).first()
                
                if existing:
                    for key, value in record.items():
                        if hasattr(existing, key):
                            setattr(existing, key, value)
                else:
                    session.add(model_class(**record))
                count += 1
            
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"保存失败: {e}")
            count = 0
        finally:
            if should_close:
                session.close()
        
        return count
    
    def save_fut_daily(self, records: List[dict]) -> int:
        return self.save_records(FutDaily, records)
    
    def save_fut_mapping(self, records: List[dict]) -> int:
        return self.save_records(FutMapping, records)
    
    def save_fut_wsr(self, records: List[dict]) -> int:
        return self.save_records(FutWsr, records)
    
    
    # ============ 爬取主流程 ============
    
    def crawl_fut_daily(self, start_date: str, end_date: str) -> int:
        """爬取期货日线行情 - 支持中断续跑，批量提交"""
        print(f"\n{'='*50}")
        print("爬取 fut_daily (期货日线行情)")
        print(f"{'='*50}")
        
        # 获取所有合约
        print("获取合约列表...")
        all_codes = []
        for exchange in tqdm(EXCHANGES, desc="获取交易所合约"):
            codes = self.get_fut_codes(exchange=exchange)
            all_codes.extend(codes)
            print(f"  {exchange}: {len(codes)} 个合约")
        
        # 获取已保存的合约，避免重复爬取
        session = self.Session()
        saved_codes = set()
        for r in session.query(FutDaily.ts_code).distinct().all():
            saved_codes.add(r[0])
        session.close()
        
        codes_to_fetch = [c for c in all_codes if c not in saved_codes]
        print(f"共 {len(all_codes)} 个合约，已爬取 {len(saved_codes)}，需爬取 {len(codes_to_fetch)}")
        
        if not codes_to_fetch:
            print("全部合约已爬取完成")
            return 0
        
        # 使用单一 session，批量提交，每 100 个合约打印进度
        session = self.Session()
        total_count = 0
        batch_count = 0
        
        for code in tqdm(codes_to_fetch, desc="fut_daily"):
            try:
                records = self.fetch_fut_daily(code, start_date, end_date)
                if records:
                    for record in records:
                        filter_args = {'ts_code': record['ts_code'], 'trade_date': record['trade_date']}
                        existing = session.query(FutDaily).filter_by(**filter_args).first()
                        if existing:
                            for key, value in record.items():
                                if hasattr(existing, key):
                                    setattr(existing, key, value)
                        else:
                            session.add(FutDaily(**record))
                        total_count += 1
                    batch_count += 1
                    
                    # 每 100 个合约提交一次，避免中断后大量丢失
                    if batch_count >= 100:
                        session.commit()
                        batch_count = 0
                        print(f"  [进度] 已处理 {codes_to_fetch.index(code)+1}/{len(codes_to_fetch)} 合约，累计 {total_count} 条记录")
                
                time.sleep(0.1)
            except Exception as e:
                print(f"  合约 {code} 出错: {e}")
                continue
        
        # 最终提交
        session.commit()
        session.close()
        
        print(f"fut_daily 完成: 共 {total_count} 条记录")
        return total_count
    
    def crawl_fut_mapping(self, start_date: str, end_date: str) -> int:
        """爬取主力合约映射"""
        print(f"\n{'='*50}")
        print("爬取 fut_mapping (主力合约映射)")
        print(f"{'='*50}")
        
        trade_dates = self.get_trade_dates(start_date, end_date)
        print(f"共 {len(trade_dates)} 个交易日")
        
        total_count = 0
        for td in tqdm(trade_dates, desc="fut_mapping"):
            records = self.fetch_fut_mapping(td)
            if records:
                count = self.save_fut_mapping(records)
                total_count += count
            time.sleep(0.1)
        
        print(f"fut_mapping 完成: 共 {total_count} 条记录")
        return total_count
    
    
    def crawl_fut_wsr(self, start_date: str, end_date: str) -> int:
        """爬取仓单日报"""
        print(f"\n{'='*50}")
        print("爬取 fut_wsr (仓单日报)")
        print(f"{'='*50}")
        
        # 获取所有品种
        all_symbols = []
        for exchange in tqdm(EXCHANGES, desc="获取品种"):
            symbols = self.get_fut_symbols(exchange=exchange)
            all_symbols.extend(symbols)
        
        all_symbols = list(set(all_symbols))
        print(f"共 {len(all_symbols)} 个品种")
        
        total_count = 0
        for symbol in tqdm(all_symbols, desc="fut_wsr"):
            records = self.fetch_fut_wsr(symbol, start_date, end_date)
            if records:
                count = self.save_fut_wsr(records)
                total_count += count
            time.sleep(0.1)
        
        print(f"fut_wsr 完成: 共 {total_count} 条记录")
        return total_count
    
    
    
    def crawl_all(self, start_date: str = '20200101', end_date: str = None):
        """爬取所有数据"""
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        print(f"\n{'#'*60}")
        print(f"# Tushare 期货数据爬取")
        print(f"# 时间范围: {start_date} ~ {end_date}")
        print(f"# 交易所: {', '.join(EXCHANGES)}")
        print(f"{'#'*60}")
        
        results = {}
        
        # 1. fut_daily - 日线行情
        results['fut_daily'] = self.crawl_fut_daily(start_date, end_date)
        
        # 2. fut_mapping - 主力映射
        results['fut_mapping'] = self.crawl_fut_mapping(start_date, end_date)
        
        # 3. fut_wsr - 仓单
        results['fut_wsr'] = self.crawl_fut_wsr(start_date, end_date)
        
        # fut_holding - 持仓排名 (用户不需要，跳过)
        # fut_settle - 结算参数 (数据量大，跳过)
        
        print(f"\n{'='*60}")
        print("全部爬取完成!")
        print(f"{'='*60}")
        for name, count in results.items():
            print(f"  {name}: {count} 条")
        
        return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Tushare 期货数据爬虫')
    parser.add_argument('--start', default='20200101', help='开始日期 YYYYMMDD')
    parser.add_argument('--end', default=None, help='结束日期 YYYYMMDD')
    parser.add_argument('--table', default='all', choices=['all', 'fut_daily', 'fut_mapping', 'fut_wsr'], 
                        help='爬取指定表')
    
    args = parser.parse_args()
    
    crawler = TushareCrawler()
    
    if args.table == 'all':
        crawler.crawl_all(args.start, args.end)
    else:
        func = getattr(crawler, f'crawl_{args.table}')
        func(args.start, args.end)

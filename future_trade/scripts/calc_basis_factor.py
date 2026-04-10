"""基差因子计算脚本

使用 100ppi 自身的现货和期货价格数据计算基差因子：
- 现货价格: price_data.spot_price
- 期货价格: price_data.near_price (近月) / price_data.main_price (主力)
- 合约代码: price_data.near_contract / price_data.main_contract

步骤：
1. 从 price_data 获取所有有效记录
2. 计算基差、基差率
3. 计算滚动分位数 (250/500/750日)
4. 存入 basis_factor_daily 表
"""
import sys
from pathlib import Path
from datetime import date, datetime
from typing import List, Dict, Tuple, Optional
import sqlite3

import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.database import DATABASE_PATH
from models.basis_models import BasisFactorDaily


def get_db_connection():
    """获取数据库连接"""
    return sqlite3.connect(DATABASE_PATH)


def load_price_data(conn: sqlite3.Connection) -> List[Dict]:
    """
    从 price_data 加载所有有效记录
    返回: [{symbol, trade_date, spot_price, near_contract, near_price, main_contract, main_price}]
    """
    from datetime import datetime
    
    cursor = conn.execute('''
        SELECT 
            c.exchange || '.' || c.name AS symbol,
            c.code AS commodity_code,
            pd.trade_date,
            pd.spot_price,
            pd.near_contract,
            pd.near_price,
            pd.main_contract,
            pd.main_price,
            pd.near_diff_pct,
            pd.main_diff_pct
        FROM price_data pd
        JOIN commodities c ON pd.commodity_id = c.id
        WHERE pd.spot_price IS NOT NULL
          AND pd.spot_price > 0
          AND (pd.near_price IS NOT NULL OR pd.main_price IS NOT NULL)
        ORDER BY c.exchange, c.name, pd.trade_date
    ''')
    
    records = []
    from datetime import date as date_type
    for row in cursor.fetchall():
        symbol, commodity_code, trade_date, spot_price, near_contract, near_price, main_contract, main_price, near_diff_pct, main_diff_pct = row
        # Convert string date to Python date object
        if isinstance(trade_date, str):
            trade_date = datetime.strptime(trade_date, '%Y-%m-%d').date()
        elif isinstance(trade_date, datetime):
            trade_date = trade_date.date()
        
        records.append({
            'symbol': symbol,
            'commodity_code': commodity_code,
            'trade_date': trade_date,
            'spot_price': spot_price,
            'near_contract': near_contract,
            'near_price': near_price,
            'main_contract': main_contract,
            'main_price': main_price,
            'near_diff_pct': near_diff_pct,  # 来自100ppi
            'main_diff_pct': main_diff_pct,  # 来自100ppi
        })
    
    print(f"加载 price_data: {len(records)} 条")
    return records


def calculate_basis(records: List[Dict]) -> List[Dict]:
    """计算基差和基差率"""
    
    result = []
    for r in tqdm(records, desc="计算基差"):
        spot_price = r['spot_price']
        
        # 近月基差
        near_price = r['near_price']
        near_contract = r['near_contract']
        basis_near = None
        basis_rate_near = None
        
        if near_price is not None and near_price > 0 and spot_price > 0:
            basis_near = spot_price - near_price
            basis_rate_near = (basis_near / spot_price) * 100
        
        # 主力基差
        main_price = r['main_price']
        main_contract = r['main_contract']
        basis_main = None
        basis_rate_main = None
        
        if main_price is not None and main_price > 0 and spot_price > 0:
            basis_main = spot_price - main_price
            basis_rate_main = (basis_main / spot_price) * 100
        
        result.append({
            'symbol': r['symbol'],
            'trade_date': r['trade_date'],
            'spot_price': spot_price,
            'spot_source': '100ppi',
            'near_contract': near_contract,
            'near_settle': near_price,    # price_data 的 near_price 作为近月结算价
            'main_contract': main_contract,
            'main_settle': main_price,   # price_data 的 main_price 作为主力结算价
            'basis_near': basis_near,
            'basis_main': basis_main,
            'basis_rate_near': basis_rate_near,
            'basis_rate_main': basis_rate_main,
            'basis_near_pct_250': None,  # 待计算
            'basis_near_pct_500': None,
            'basis_near_pct_750': None,
            'calc_version': 'v1',
        })
    
    return result


def calculate_percentile(value: float, series: np.ndarray, min_samples: int) -> Optional[float]:
    """计算分位数
    
    Args:
        value: 当前值
        series: 历史序列
        min_samples: 最小样本要求
    
    Returns:
        百分位数 (0-100) 或 None
    """
    valid = series[~np.isnan(series)]
    if len(valid) < min_samples:
        return None
    
    # 百分位排名
    pct = (valid < value).sum() / len(valid) * 100
    return round(pct, 2)


def calculate_percentiles(records: List[Dict]) -> List[Dict]:
    """计算滚动分位数 (基于近月基差)"""
    
    from collections import defaultdict
    
    # 按 symbol 分组
    by_symbol = defaultdict(list)
    for r in records:
        by_symbol[r['symbol']].append(r)
    
    result = []
    
    for symbol, symbol_records in tqdm(by_symbol.items(), desc="计算分位数"):
        # 按日期排序
        symbol_records.sort(key=lambda x: x['trade_date'])
        
        # 提取近月基差序列
        basis_near_series = np.array([
            r['basis_near'] if r['basis_near'] is not None else np.nan 
            for r in symbol_records
        ])
        
        for i, record in enumerate(symbol_records):
            basis_near = record['basis_near']
            
            if basis_near is None or np.isnan(basis_near):
                result.append(record)
                continue
            
            # 获取历史序列（不包含当前）
            hist = basis_near_series[:i]
            if len(hist) == 0:
                result.append(record)
                continue
            
            # 计算分位数
            record['basis_near_pct_250'] = calculate_percentile(basis_near, hist, 200)
            record['basis_near_pct_500'] = calculate_percentile(basis_near, hist, 400)
            record['basis_near_pct_750'] = calculate_percentile(basis_near, hist, 600)
            
            result.append(record)
    
    return result


def save_to_database(records: List[Dict]):
    """保存到数据库"""
    
    engine = create_engine(f'sqlite:///{DATABASE_PATH}')
    Session = sessionmaker(bind=engine)
    session = Session()
    
    count = 0
    for record in tqdm(records, desc="保存数据"):
        # 检查是否已存在
        existing = session.query(BasisFactorDaily).filter(
            BasisFactorDaily.symbol == record['symbol'],
            BasisFactorDaily.trade_date == record['trade_date']
        ).first()
        
        # 移除不在模型中的字段
        record_to_save = {k: v for k, v in record.items() if k not in ['commodity_code']}
        
        if existing:
            # 更新
            for key, value in record_to_save.items():
                if key not in ['symbol', 'trade_date']:
                    setattr(existing, key, value)
        else:
            session.add(BasisFactorDaily(**record_to_save))
        
        count += 1
        
        # 批量提交
        if count % 5000 == 0:
            session.commit()
    
    session.commit()
    session.close()
    
    print(f"保存完成: {count} 条记录")
    return count


def main():
    print("=" * 60)
    print("基差因子计算")
    print("=" * 60)
    
    # 1. 加载数据
    print("\n[1/4] 加载 price_data...")
    conn = get_db_connection()
    records = load_price_data(conn)
    conn.close()
    
    if not records:
        print("没有数据，退出")
        return
    
    # 2. 计算基差
    print("\n[2/4] 计算基差和基差率...")
    records = calculate_basis(records)
    
    # 3. 计算分位数
    print("\n[3/4] 计算滚动分位数...")
    records = calculate_percentiles(records)
    
    # 4. 保存到数据库
    print("\n[4/4] 保存到数据库...")
    save_to_database(records)
    
    print("\n" + "=" * 60)
    print("完成!")
    print("=" * 60)


if __name__ == "__main__":
    main()

"""页面解析模块"""
import re
from datetime import date
from typing import List, Tuple, Optional
from bs4 import BeautifulSoup, Tag


def parse_sf_table(html: str, trade_date: date) -> List[dict]:
    """解析现期表 HTML"""
    soup = BeautifulSoup(html, 'html.parser')
    main_table = find_main_table(soup)
    if not main_table:
        return []
    return parse_table_with_exchanges(main_table, trade_date)


def find_main_table(soup: BeautifulSoup) -> Optional[Tag]:
    """找到主数据表格"""
    tables = soup.find_all('table')
    for table in tables:
        rows = table.find_all('tr')
        if len(rows) > 50:
            return table
    return None if not tables else max(tables, key=lambda t: len(t.find_all('tr')))


def parse_table_with_exchanges(table: Tag, trade_date: date) -> List[dict]:
    """解析包含交易所分组的表格"""
    records = []
    current_exchange = 'SHFE'
    
    exchange_keywords = {
        '上海期货交易所': 'SHFE',
        '郑州商品交易所': 'ZCE',
        '大连商品交易所': 'DCE',
        '广州期货交易所': 'GFE',
    }
    
    rows = table.find_all('tr')
    
    for row in rows:
        cells = row.find_all(['td', 'th'])
        
        # 只处理12单元格的数据行（主数据行）
        # 跳过2单元格的补充行和表头行
        if len(cells) != 12:
            continue
        
        first_text = cells[0].get_text(strip=True).replace('\xa0', '')
        
        # 检查是否是交易所分隔行
        if first_text in exchange_keywords:
            current_exchange = exchange_keywords[first_text]
            continue
        
        # 跳过空行和表头行
        if not first_text or first_text in ['商品', '现货', '价格', '合约']:
            continue
        
        try:
            record = parse_data_row(cells, current_exchange, trade_date)
            if record:
                records.append(record)
        except Exception:
            continue
    
    return records


def parse_data_row(cells: List[Tag], exchange: str, trade_date: date) -> Optional[dict]:
    """
    解析数据行 (12单元格格式)
    
    Cell 0:  商品名称 (可能包含 <a> 链接)
    Cell 1:  现货价格
    Cell 2:  近月合约代码
    Cell 3:  近月合约价格
    Cell 4:  近月现期差 (复合格式，如 '-355-0.36%')
    Cell 5:  近月现期差值 (如 '-355')
    Cell 6:  近月现期差百分比 (如 '-0.36%')
    Cell 7:  远月合约代码
    Cell 8:  远月合约价格
    Cell 9:  远月现期差 (复合格式)
    Cell 10: 远月现期差值
    Cell 11: 远月现期差百分比
    """
    try:
        # 商品名称 - 优先从链接提取
        first_cell = cells[0]
        link = first_cell.find('a')
        
        if link:
            name = link.get_text(strip=True).replace('\xa0', '')
            href = link.get('href', '')
            # 提取 URL 路径部分，如 https://www.100ppi.com/sf/792.html -> /sf/792.html
            if href:
                if '/sf/' in href:
                    detail_url = '/sf/' + href.split('/sf/')[-1]
                elif href.startswith('/'):
                    detail_url = href
                else:
                    detail_url = '/' + href.split('/')[-1]
            else:
                detail_url = None
        else:
            name = first_cell.get_text(strip=True).replace('\xa0', '')
            detail_url = None
        
        if not name:
            return None
        
        # 现货价格
        spot_price = parse_number(cells[1].get_text(strip=True).replace('\xa0', ''))
        
        # 近月合约
        near_contract = cells[2].get_text(strip=True).replace('\xa0', '')
        near_price = parse_number(cells[3].get_text(strip=True).replace('\xa0', ''))
        
        # 近月现期差 - 使用单独的单元格
        near_diff = parse_number(cells[5].get_text(strip=True).replace('\xa0', ''))
        near_diff_pct = parse_percentage(cells[6].get_text(strip=True).replace('\xa0', ''))
        
        # 主力/远月合约
        main_contract = cells[7].get_text(strip=True).replace('\xa0', '')
        main_price = parse_number(cells[8].get_text(strip=True).replace('\xa0', ''))
        
        # 远月现期差 - 使用单独的单元格
        main_diff = parse_number(cells[10].get_text(strip=True).replace('\xa0', ''))
        main_diff_pct = parse_percentage(cells[11].get_text(strip=True).replace('\xa0', ''))
        
        if spot_price is None or near_price is None:
            return None
        
        return {
            'name': name,
            'exchange': exchange,
            'detail_url': detail_url,
            'spot_price': spot_price,
            'near_contract': near_contract,
            'near_price': near_price,
            'near_diff': near_diff,
            'near_diff_pct': near_diff_pct,
            'main_contract': main_contract,
            'main_price': main_price,
            'main_diff': main_diff,
            'main_diff_pct': main_diff_pct,
        }
    except (IndexError, ValueError):
        return None


def parse_number(text: str) -> Optional[float]:
    """解析数字字符串"""
    if not text or text == '-' or text == '\xa0':
        return None
    
    text = text.replace(',', '').replace(' ', '').replace('\xa0', '')
    
    if text.startswith('(') and text.endswith(')'):
        text = '-' + text[1:-1]
    
    try:
        return float(text)
    except ValueError:
        return None


def parse_percentage(text: str) -> Optional[float]:
    """解析百分比字符串"""
    if not text:
        return None
    text = text.replace('%', '')
    return parse_number(text)


def get_table_date(html: str) -> Optional[date]:
    """从页面提取日期"""
    soup = BeautifulSoup(html, 'html.parser')
    patterns = [r'(\d{4})年(\d{2})月(\d{2})日', r'(\d{4})-(\d{2})-(\d{2})']
    
    text = soup.get_text()
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            year, month, day = match.groups()
            return date(int(year), int(month), int(day))
    return None

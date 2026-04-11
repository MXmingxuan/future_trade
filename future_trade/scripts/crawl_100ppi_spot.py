"""
爬取 https://www.100ppi.com/sf2/ 现货+基差数据（使用 Playwright）

数据来源：生意社 (100ppi.com)
页面：https://www.100ppi.com/sf2/ — 商品主力基差表

输出：写入 PostgreSQL price_factor_daily 表的 spot_price / basis_main / basis_rate 字段

使用说明：
    # 安装 playwright（用 hermes venv）：
    /root/.hermes/hermes-agent/venv/bin/pip3 install playwright
    /root/.hermes/hermes-agent/venv/bin/python3 -m playwright install chromium

    # 爬取当天数据
    /root/.hermes/hermes-agent/venv/bin/python3 -m future_trade.scripts.crawl_100ppi_spot

    # 爬取指定日期
    /root/.hermes/hermes-agent/venv/bin/python3 -m future_trade.scripts.crawl_100ppi_spot --date 20260410

    # 爬取日期范围
    /root/.hermes/hermes-agent/venv/bin/python3 -m future_trade.scripts.crawl_100ppi_spot --start 20260401 --end 20260410
"""
import sys
from pathlib import Path
from datetime import date, datetime, timedelta
import logging
import argparse
import json

sys.path.insert(0, str(Path(__file__).parent.parent))

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from tqdm import tqdm

# ============ 配置 ============
import psycopg2
from config.postgres_config import get_settings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/root/future_trade/logs/crawl_100ppi.log', encoding='utf-8'),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

# 品种名 → (commodity_id, 品种代码) 映射
# commodity_master 表初始化数据（来自 schema.sql）：
# PTA=1, PX=2, MEG=3, MA=4, PP=5, PF=6
COMMODITY_MAP = {
    'PTA':    {'id': 1, 'code': 'PTA'},
    'PX':     {'id': 2, 'code': 'PX'},
    'MEG':    {'id': 3, 'code': 'MEG'},
    '乙二醇': {'id': 3, 'code': 'MEG'},
    'MA':     {'id': 4, 'code': 'MA'},
    '甲醇':   {'id': 4, 'code': 'MA'},
    'PP':     {'id': 5, 'code': 'PP'},
    '短纤':   {'id': 6, 'code': 'PF'},
    'PF':     {'id': 6, 'code': 'PF'},
    '白糖':   {'id': None, 'code': 'SR'},
    '棉花':   {'id': None, 'code': 'CF'},
}


def fetch_page(url: str, timeout: int = 30000) -> str | None:
    """使用 Playwright 获取页面 HTML（绕过 anti-bot）"""
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=['--disable-blink-features=AutomationControlled']
        )
        ctx = browser.new_context(
            user_agent=(
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/120.0.0.0 Safari/537.36'
            ),
            locale='zh-CN'
        )
        page = ctx.new_page()

        try:
            # 先访问主页建立 cookie 链
            page.goto('https://www.100ppi.com/', timeout=timeout, wait_until='networkidle')
            page.wait_for_timeout(2000)

            # 再访问目标页
            page.goto(url, timeout=timeout, wait_until='networkidle')
            page.wait_for_timeout(3000)

            html = page.content()
            browser.close()
            return html
        except Exception as e:
            logger.error(f'Playwright 获取页面失败: {e}')
            browser.close()
            return None


def parse_num(t: str):
    """解析数字字符串"""
    if not t or t.strip() in ('-', '\xa0', '', 'N/A'):
        return None
    t = t.replace(',', '').replace(' ', '').replace('\xa0', '')
    if t.startswith('(') and t.endswith(')'):
        t = '-' + t[1:-1]
    try:
        return float(t)
    except ValueError:
        return None


def parse_pct(t: str):
    """解析百分比（返回小数）"""
    if not t:
        return None
    t = t.replace('%', '').replace(' ', '').replace('\xa0', '')
    return parse_num(t)


def parse_sf2_page(html: str) -> list[dict]:
    """解析 https://www.100ppi.com/sf2/ 页面的现期表"""
    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.find_all('table')
    if not tables:
        return []

    main_table = max(tables, key=lambda t: len(t.find_all('tr')))
    rows = main_table.find_all('tr')

    records = []
    current_exchange = None

    exchange_map = {
        '上海期货交易所': 'SHFE',
        '郑州商品交易所': 'CZCE',
        '大连商品交易所': 'DCE',
        '广州期货交易所': 'GFEX',
    }

    for row in rows:
        cells = row.find_all(['td', 'th'])

        # 1 cell = 交易所分隔行
        if len(cells) == 1:
            txt = cells[0].get_text(strip=True).replace('\xa0', '')
            if txt in exchange_map:
                current_exchange = exchange_map[txt]
            continue

        # 忽略表头行和补充分组行
        if len(cells) in (4, 7, 2):
            continue

        # 数据行必须是 10 cells
        if len(cells) != 10:
            continue

        name = cells[0].get_text(strip=True).replace('\xa0', '')
        if not name or name in ('商品', '现货', '主力合约', '180日内主力基差'):
            continue

        spot_price = parse_num(cells[1].get_text(strip=True))
        main_contract = cells[2].get_text(strip=True).replace('\xa0', '')
        main_price = parse_num(cells[3].get_text(strip=True))
        basis = parse_num(cells[5].get_text(strip=True))
        basis_pct = parse_pct(cells[6].get_text(strip=True))  # 已经是小数形式
        basis_180d_max = parse_num(cells[7].get_text(strip=True))
        basis_180d_min = parse_num(cells[8].get_text(strip=True))
        basis_180d_avg = parse_num(cells[9].get_text(strip=True))

        # 映射到 commodity_id
        commodity_info = COMMODITY_MAP.get(name, {'id': None, 'code': name})

        record = {
            'name': name,
            'commodity_id': commodity_info['id'],
            'exchange': current_exchange or 'UNKNOWN',
            'spot_price': spot_price,
            'main_contract': main_contract,
            'main_price': main_price,
            'basis': basis,                          # 现货 - 期货
            'basis_pct': basis_pct,                   # 已经是小数 (0.05 = 5%)
            'basis_180d_max': basis_180d_max,
            'basis_180d_min': basis_180d_min,
            'basis_180d_avg': basis_180d_avg,
        }
        records.append(record)

    return records


def crawl_for_date(trade_date: date) -> int:
    """爬取指定日期的现期表数据并写入 PostgreSQL"""
    url = 'https://www.100ppi.com/sf2/'
    logger.info(f'爬取 {trade_date} 现期表: {url}')

    html = fetch_page(url)
    if not html:
        logger.error(f'获取页面失败: {trade_date}')
        return 0

    records = parse_sf2_page(html)
    if not records:
        logger.warning(f'解析数据为空: {trade_date}')
        return 0

    logger.info(f'解析到 {len(records)} 条记录')

    # 过滤有 commodity_id 的记录
    valid_records = [r for r in records if r['commodity_id'] is not None]
    logger.info(f'有效记录（含commodity_id）: {len(valid_records)} 条')

    # 连接 PostgreSQL
    try:
        settings = get_settings()
        conn = psycopg2.connect(
            host=settings.pg_host,
            port=settings.pg_port,
            database=settings.pg_database,
            user=settings.pg_user,
            password=settings.pg_password,
        )
        conn.autocommit = False
        cur = conn.cursor()
    except Exception as e:
        logger.error(f'数据库连接失败: {e}（数据库离线？）')
        return 0

    saved_count = 0
    try:
        for record in valid_records:
            # basis_rate = basis / main_price (期货价格为分母)
            basis_rate = None
            if record['basis'] is not None and record['main_price'] and record['main_price'] != 0:
                basis_rate = record['basis'] / record['main_price']

            cur.execute("""
                INSERT INTO price_factor_daily (
                    commodity_id, trade_date,
                    spot_price, main_contract,
                    basis_main, basis_rate,
                    basis_5d_change, basis_20d_change, basis_percentile_60,
                    time_spread, back_or_contango
                ) VALUES (
                    %(commodity_id)s, %(trade_date)s,
                    %(spot_price)s, %(main_contract)s,
                    %(basis_main)s, %(basis_rate)s,
                    %(basis_5d_change)s, %(basis_20d_change)s, %(basis_percentile_60)s,
                    %(time_spread)s, %(back_or_contango)s
                )
                ON CONFLICT (commodity_id, trade_date) DO UPDATE SET
                    spot_price = EXCLUDED.spot_price,
                    main_contract = EXCLUDED.main_contract,
                    basis_main = EXCLUDED.basis_main,
                    basis_rate = EXCLUDED.basis_rate;
            """, {
                'commodity_id': record['commodity_id'],
                'trade_date': trade_date,
                'spot_price': record['spot_price'],
                'main_contract': record['main_contract'],
                'basis_main': record['basis'],
                'basis_rate': basis_rate,
                'basis_5d_change': None,
                'basis_20d_change': None,
                'basis_percentile_60': None,
                'time_spread': None,
                'back_or_contango': None,
            })
            saved_count += 1

        conn.commit()
        logger.info(f'写入 {saved_count} 条记录到 price_factor_daily')
    except Exception as e:
        logger.error(f'数据库写入失败: {e}')
        conn.rollback()
        saved_count = 0
    finally:
        cur.close()
        conn.close()

    return saved_count


def crawl_date_range(start_date: date, end_date: date) -> dict:
    """爬取日期范围的数据（跳过周末）"""
    results = {'success': 0, 'failed': 0}
    current = end_date

    while current >= start_date:
        if current.weekday() < 5:  # 跳过周末
            count = crawl_for_date(current)
            if count > 0:
                results['success'] += 1
            else:
                results['failed'] += 1
        current -= timedelta(days=1)

    logger.info(f'完成: 成功 {results["success"]} 天, 失败 {results["failed"]} 天')
    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='爬取 100ppi 现货基差数据（Playwright版）')
    parser.add_argument('--date', default=None, help='指定日期 YYYYMMDD，默认今天')
    parser.add_argument('--start', default=None, help='开始日期 YYYYMMDD')
    parser.add_argument('--end', default=None, help='结束日期 YYYYMMDD')
    args = parser.parse_args()

    if args.date:
        td = datetime.strptime(args.date, '%Y%m%d').date()
        crawl_for_date(td)
    elif args.start and args.end:
        start = datetime.strptime(args.start, '%Y%m%d').date()
        end = datetime.strptime(args.end, '%Y%m%d').date()
        crawl_date_range(start, end)
    else:
        crawl_for_date(date.today())

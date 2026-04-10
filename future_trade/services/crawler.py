"""爬虫核心模块 - 使用 Playwright 处理反爬机制"""
import os
import time
import logging
from typing import Optional, List
from datetime import date, datetime, timedelta
from pathlib import Path

from playwright.sync_api import sync_playwright
from sqlalchemy.orm import Session

from config.database import CRAWL_CONFIG, DATABASE_PATH, LOG_CONFIG
from models.models import PriceData, CrawlLog


class Crawler:
    """爬虫核心类 (Playwright 版本)"""
    
    def __init__(self):
        self.base_url = CRAWL_CONFIG["base_url"]
        self.timeout = CRAWL_CONFIG["timeout"] * 1000  # Playwright uses milliseconds
        self.retry_times = CRAWL_CONFIG["retry_times"]
        self.retry_delay = CRAWL_CONFIG["retry_delay"]
        
        # 设置日志
        logging.basicConfig(
            level=getattr(logging, LOG_CONFIG["level"]),
            format=LOG_CONFIG["format"],
            handlers=[
                logging.FileHandler(LOG_CONFIG["file"], encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        self.playwright = None
        self.browser = None
        self.context = None
    
    def _ensure_browser(self):
        """确保浏览器已启动"""
        if self.browser is None:
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(
                headless=True,
                args=['--disable-blink-features=AutomationControlled']
            )
            self.context = self.browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                locale='zh-CN'
            )
    
    def _close_browser(self):
        """关闭浏览器"""
        if self.browser:
            self.browser.close()
            self.browser = None
        if self.playwright:
            self.playwright.stop()
            self.playwright = None
    
    def fetch_page(self, url: str) -> Optional[str]:
        """
        获取页面内容 (使用 Playwright)
        
        Args:
            url: 页面URL
        
        Returns:
            页面 HTML 内容，失败返回 None
        """
        self._ensure_browser()
        
        for attempt in range(self.retry_times):
            try:
                self.logger.debug(f"正在请求: {url} (尝试 {attempt + 1}/{self.retry_times})")
                
                page = self.context.new_page()
                
                # 设置请求拦截，忽略一些资源
                page.route("**/*.{png,jpg,jpeg,gif,svg,css,woff,woff2}", 
                          lambda route: route.abort())
                
                response = page.goto(url, timeout=self.timeout, wait_until="networkidle")
                
                if response is None or response.status >= 400:
                    self.logger.warning(f"HTTP错误: {response.status if response else 'No response'}")
                    page.close()
                    if attempt < self.retry_times - 1:
                        time.sleep(self.retry_delay)
                        continue
                    return None
                
                # 等待页面稳定
                time.sleep(1)
                
                # 获取完整 HTML
                html = page.content()
                page.close()
                
                self.logger.debug(f"成功获取页面: {url}")
                return html
                
            except Exception as e:
                self.logger.warning(f"请求错误 (尝试 {attempt + 1}/{self.retry_times}): {e}")
                if page and not page.is_closed():
                    page.close()
                if attempt < self.retry_times - 1:
                    time.sleep(self.retry_delay)
                    
        self.logger.error(f"获取页面失败: {url}")
        return None
    
    def parse_table(self, html: str, trade_date: date) -> List[dict]:
        """
        解析现期表 HTML
        
        Args:
            html: 页面 HTML
            trade_date: 交易日期
        
        Returns:
            解析后的数据列表
        """
        from services.parser import parse_sf_table
        return parse_sf_table(html, trade_date)
    
    def save_data(self, db: Session, records: List[dict], trade_date: date) -> int:
        """
        保存数据到数据库
        
        Args:
            db: 数据库会话
            records: 解析后的数据
            trade_date: 交易日期
        
        Returns:
            保存的记录数
        """
        from models.models import bulk_upsert_price_data, Commodity
        
        # 构建商品名称到ID的映射
        commodity_map = {
            c.name: c.id for c in db.query(Commodity).all()
        }
        
        # 构建 detail_url 到 ID 的映射
        detail_url_map = {
            c.detail_url: c.id for c in db.query(Commodity).all() if c.detail_url
        }
        
        count = bulk_upsert_price_data(db, records, commodity_map, trade_date, 
                                       detail_url_map=detail_url_map)
        self.logger.info(f"保存 {count} 条记录到数据库")
        return count
    
    def crawl_date(self, db: Session, trade_date: date) -> bool:
        """
        爬取指定日期的数据
        
        Args:
            db: 数据库会话
            trade_date: 交易日期
        
        Returns:
            是否成功
        """
        # 检查是否已爬取
        existing = db.query(CrawlLog).filter(CrawlLog.crawl_date == trade_date).first()
        if existing and existing.status == 'success':
            self.logger.info(f"日期 {trade_date} 已爬取，跳过")
            return True
        
        # 构建 URL
        url = CRAWL_CONFIG["history_url_pattern"].format(date=trade_date.strftime("%Y-%m-%d"))
        self.logger.info(f"开始爬取: {url}")
        
        # 获取页面
        html = self.fetch_page(url)
        if not html:
            self._log_crawl(db, trade_date, 'failed', 0, "获取页面失败")
            return False
        
        # 解析数据
        try:
            records = self.parse_table(html, trade_date)
        except Exception as e:
            self.logger.error(f"解析页面失败: {e}")
            self._log_crawl(db, trade_date, 'failed', 0, f"解析失败: {e}")
            return False
        
        if not records:
            self.logger.warning(f"未解析到数据: {trade_date}")
            self._log_crawl(db, trade_date, 'failed', 0, "未解析到数据")
            return False
        
        # 保存数据
        try:
            count = self.save_data(db, records, trade_date)
            self._log_crawl(db, trade_date, 'success', count)
            self.logger.info(f"[OK] {trade_date} 爬取成功 ({count} 条记录)")
            return True
        except Exception as e:
            self.logger.error(f"保存数据失败: {e}")
            self._log_crawl(db, trade_date, 'failed', 0, f"保存失败: {e}")
            return False
    
    def _log_crawl(self, db: Session, crawl_date: date, status: str, 
                   records_count: int = 0, error_msg: str = None):
        """记录爬取日志"""
        from datetime import datetime as dt
        log = db.query(CrawlLog).filter(CrawlLog.crawl_date == crawl_date).first()
        
        if log:
            log.status = status
            log.records_count = records_count
            log.error_msg = error_msg
            log.crawled_at = dt.now()
        else:
            log = CrawlLog(
                crawl_date=crawl_date,
                status=status,
                records_count=records_count,
                error_msg=error_msg
            )
            db.add(log)
        
        db.commit()
    
    def crawl_date_range(self, db: Session, start_date: date, end_date: date) -> dict:
        """
        爬取日期范围的数据
        
        Args:
            db: 数据库会话
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            统计结果 {'success': int, 'failed': int}
        """
        current = end_date
        results = {'success': 0, 'failed': 0}
        
        try:
            while current >= start_date:
                if self.crawl_date(db, current):
                    results['success'] += 1
                else:
                    results['failed'] += 1
                
                # 往前一天
                current -= timedelta(days=1)
                
                # 每爬取一页休息一下
                time.sleep(2)
        finally:
            # 确保关闭浏览器
            self._close_browser()
        
        return results
    
    def __del__(self):
        """析构时关闭浏览器"""
        self._close_browser()

"""数据库配置"""
import os
from pathlib import Path

# 项目根目录
BASE_DIR = Path(__file__).resolve().parent.parent

# 数据库路径
DATABASE_PATH = BASE_DIR / "data" / "future_trade.db"

# 确保data目录存在
DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)

# 爬虫配置
CRAWL_CONFIG = {
    "base_url": "https://www.100ppi.com/sf/",
    "history_url_pattern": "https://www.100ppi.com/sf/day-{date}.html",
    "timeout": 30,
    "retry_times": 3,
    "retry_delay": 5,  # 秒
}

# 代理配置 (需要从环境变量或配置文件读取)
PROXY_CONFIG = {
    "enabled": False,  # 暂时禁用，先测试直连
    # 代理URL，格式: http://user:pass@host:port
    # 或者从环境变量 PROXY_URL 读取
    "url": os.getenv("PROXY_URL", ""),
}

# 日志配置
LOG_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(levelname)s - %(message)s",
    "file": BASE_DIR / "crawl.log",
}

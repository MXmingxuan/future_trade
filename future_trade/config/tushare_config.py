"""Tushare 配置"""
import os

# Tushare Token
TUSHARE_TOKEN = "e3005d0c8df82146706128c35263800b863396c059201688e1fc1ace"

# Tushare API 基础配置
TUSHARE_CONFIG = {
    "api_base": "http://api.tushare.pro",  # HTTP API
    "trade_date_range": {
        "start": "20200101",  # 从2020年开始
        "end": None,  # 今天
    }
}

# 期货交易所代码映射
EXCHANGE_MAP = {
    "CFX": "CFX",  # 中金所
    "DCE": "DCE",  # 大商所
    "CZCE": "ZCE",  # 郑商所
    "SHFE": "SHFE",  # 上期所
    "GFEX": "GFE",  # 广期所
}

# 交易所名称
EXCHANGE_NAMES = {
    "CFX": "中金所",
    "DCE": "大商所",
    "CZCE": "郑商所",
    "SHFE": "上期所",
    "GFEX": "广期所",
}

# Future Trade - PTA 产业链数据分析系统

## 项目背景

本项目是一个专注于 **PTA（精对苯二甲酸）产业链** 的数据分析平台。

PTA 是石油化工下游的重要化工原料，其产业链从上游原材料到下游应用覆盖多个环节：

```
PTA 产业链结构
─────────────────────────────────────────────────────────────
【上游原材料】
  荣盛石化 (002493) - 炼化一体化，PX/PTA 产能
  恒逸石化 (000703) - 炼化一体化，PX/PTA 产能
  恒力石化 (600346) - 炼化一体化，PX/PTA 产能
  东方盛虹 (000301) - 炼化一体化，炼化+聚酯

【下游聚酯长丝】
  桐昆股份 (601233) - 聚酯长丝龙头
  新凤鸣   (603225) - 聚酯长丝龙头

【瓶片/切片】
  万凯新材 (301216) - 瓶级 PET
  三房巷   (600370) - 瓶片/PTA 贸易
─────────────────────────────────────────────────────────────
```

**核心思路**：通过爬取、存储和分析产业链上各公司的公告文本，让 AI 能够理解每家公司的产能变化、业务进展、经营情况，结合期货价格数据做出更全面的判断。

- 重点关注：投资公告、重大项目、股权激励、业绩预告、产能投放等与**产能价格和公司经营**直接相关的内容
- 少关注但不丢弃：制度类、常规公告（公司章程、高管变动、股东大会决议等与基本面分析关系不大的内容）

## 数据库

**PostgreSQL** 两个数据库：

- `future_trade` - 期货现货数据 + 公告数据
- `fcta` - 股票数据（来自 fcta-equity 项目，与本项目共享）

连接信息：
- Host: `127.0.0.1`
- Port: `5432`
- User: `postgres`
- Password: `161514`

配置文件: `future_trade/config/postgres_config.py`

## 数据表

### 公告数据（来自东方财富网）

**核心数据**。爬取各公司公告全文 HTML，存到 `future_trade` 数据库，用于后续 AI 分析。

| 表名 | 说明 |
|------|------|
| `announcements` | 上市公司公告全文（含标题、分类、正文 HTML） |

**覆盖公司**（PTA 产业链）：

| 股票代码 | 公司名称 | 角色 | 公告数 |
|------|------|------|--------|
| 000301 | 东方盛虹 | 炼化+聚酯 | 217 |
| 000703 | 恒逸石化 | 上游原材料 | 271 |
| 002493 | 荣盛石化 | 上游原材料 | 130 |
| 600346 | 恒力石化 | 上游原材料 | 157 |
| 601233 | 桐昆股份 | 下游聚酯长丝 | 155 |
| 603225 | 新凤鸣 | 下游聚酯长丝 | 239 |
| 301216 | 万凯新材 | 瓶片 | 160 |
| 600370 | 三房巷 | 瓶片/PTA 贸易 | 196 |

**公告分类重点**：
- ⭐⭐⭐ 重大事项（投资公告、重大项目合同）
- ⭐⭐⭐ 业绩预告 / 业绩快报
- ⭐⭐ 融资公告（增发、配股、可转债）
- ⭐⭐ 资产重组
- ⭐ 股权激励
- ⭐ 信息变更 / 高管变动 / 股东大会（少关注）

**announcements 表结构**：

| 字段 | 说明 |
|------|------|
| `art_code` | 东方财富唯一标识（主键） |
| `stock_code` | 股票代码 |
| `stock_name` | 公司简称 |
| `title` | 公告标题 |
| `notice_date` | 公告日期 |
| `f_node_name` | 主分类名称 |
| `s_node_name` | 子分类名称 |
| `full_text` | HTML 全文（用于 AI 分析） |
| `attach_url` | PDF 附件地址 |

### 现货数据（来自 100ppi.com）

| 表名 | 说明 | 记录数 |
|------|------|--------|
| `commodities` | 商品目录（54个品种） | 54 |
| `price_data` | 现货价格日线数据 | 71,844 |
| `crawl_log` | 爬虫运行日志 | 1,636 |
| `basis_factor_daily` | 基差因子日线数据 | 71,844 |

### 期货数据（来自 Tushare）

| 表名 | 说明 | 记录数 |
|------|------|--------|
| `fut_basic` | 合约基本信息 | 10,829 |
| `trade_cal` | 交易日历 | 47,952 |
| `fut_daily` | 期货日线行情 | 2,539,244 |
| `fut_mapping` | 主力/连续合约映射 | 517,211 |
| `fut_holding` | 持仓排名 | 14,510,751 |
| `fut_wsr` | 仓单日报 | 1,713,400 |
| `fut_settle` | 结算参数 | 824,260 |
| `fut_weekly_monthly` | 周/月线数据 | 455,432 |

**fut_daily 覆盖交易所**: DCE(大商所)、SHF(上期所)、ZCE(郑商所)、CFX(中金所)、INE(能源中心)、GFE(广期所)

### 股票数据（来自 Tushare，存储在 fcta 数据库）

| 表名 | 说明 | 记录数 |
|------|------|--------|
| `eq_basic` | 股票列表 | 5,821 |
| `eq_daily` | A股日线行情 | 13,507,575+ |
| `eq_adj_factor` | 复权因子 | 14,426,983+ |
| `eq_daily_features` | 日线指标 | 13,608,645+ |
| `eq_index_members` | 指数成分 | 219,909+ |
| `eq_namechange` | 名称变更 | 17,877 |
| `eq_trade_cal` | 交易日历 | 5,964 |
| `etf_daily` | ETF日线 | 2,749,298 |

## 项目结构

```
future_trade/
├── config/
│   └── postgres_config.py    # PostgreSQL + Tushare 配置
├── db/
│   ├── __init__.py
│   └── connection.py        # PostgreSQL 连接池管理
├── fetchers/                 # FCTA 风格数据抓取器
│   ├── __init__.py
│   ├── base_fetcher.py       # 核心基类（速率限制、retry、upsert）
│   ├── fut_daily_fetcher.py  # 期货日线（按日期抓取）
│   ├── fut_mapping_fetcher.py # 主力合约映射
│   ├── fut_wsr_fetcher.py    # 仓单日报
│   ├── fut_basic_fetcher.py  # 合约基本信息
│   ├── trade_cal_fetcher.py  # 交易日历
│   └── notice_fetcher.py     # 东方财富公告抓取（列表 + 全文）
├── models/
│   ├── models.py             # 商品/价格数据模型
│   ├── tushare_models.py     # Tushare 数据模型
│   └── basis_models.py       # 基差因子模型
├── scripts/
│   ├── sync_tushare.py       # Tushare 期货数据同步脚本
│   ├── sync_equity.py        # Tushare 股票数据同步脚本
│   ├── sync_notices.py       # 东方财富公告同步脚本
│   ├── calc_basis_factor.py  # 基差因子计算
│   ├── crawl_tushare.py      # 旧版爬虫（已废弃）
│   └── export_by_commodity.py # 按品种导出 CSV
└── services/
    ├── crawler.py             # 100ppi 爬虫服务
    └── parser.py             # 解析器
```

## 使用方法

### 公告数据同步（东方财富）

```bash
cd future_trade

# 同步所有 PTA 产业链公司公告（从上次日期继续）
python scripts/sync_notices.py

# 指定单家公司
python scripts/sync_notices.py --stock 000301

# 指定日期范围
python scripts/sync_notices.py --stock 000301 --start 20250101 --end 20260401

# 默认从 2025-01-01 开始爬取，存全文 HTML
```

### 期货数据同步（Tushare）

```bash
# 增量同步（从上次同步日期到今天）
python scripts/sync_tushare.py

# 全量同步（从 2010-01-01 开始）
python scripts/sync_tushare.py --full

# 从指定日期开始全量同步
python scripts/sync_tushare.py --full --start 20250101

# 只同步某个表
python scripts/sync_tushare.py --table fut_daily
```

### 股票数据同步（Tushare）

```bash
# 增量同步（存到 fcta 数据库）
python scripts/sync_equity.py
```

### 数据导出

```bash
# 按品种导出为 CSV
python scripts/export_by_commodity.py
```

### 基差因子计算

```bash
python scripts/calc_basis_factor.py
```

## 数据来源

1. **现货价格**: [100ppi.com](https://www.100ppi.com) - 中国大宗商品现货价格指数
2. **期货数据**: [Tushare Pro](http://tushare.pro) - Tushare 期货 API
3. **股票数据**: [Tushare Pro](http://tushare.pro) - Tushare 股票 API
4. **公告数据**: [东方财富网](https://data.eastmoney.com/notices/) - 上市公司公告（列表 API + 全文 API）

## 技术特点

- **FCTA 风格架构**: 按日期抓取（每天 1 次 API）替代逐合约抓取，快 100 倍
- **公告全文获取**: 东方财富公告列表 API + 全文 API，每条公告存完整 HTML 正文
- **速率限制**: 0.5s/次调用间隔，避免被限流
- **增量同步**: 自动从上次同步日期继续，支持增量更新
- **Upsert 写入**: `ON CONFLICT DO UPDATE` 保证数据一致性
- **PostgreSQL 双数据库**: `future_trade` 存期货现货+公告，`fcta` 存股票数据
- **AI 分析友好**: 公告 `full_text` 字段直接可送给大模型进行产能/经营分析

## 数据库架构说明

```
PostgreSQL
├── future_trade 数据库
│   ├── 现货数据（100ppi.com）
│   │   ├── commodities
│   │   ├── price_data
│   │   ├── crawl_log
│   │   └── basis_factor_daily
│   ├── 期货数据（Tushare）
│   │   ├── fut_basic / fut_daily / fut_mapping
│   │   ├── fut_holding / fut_wsr / fut_settle
│   │   └── trade_cal
│   └── 公告数据（东方财富）
│       └── announcements  ← 新增，PTA 产业链 8 家公司的公告全文
│
└── fcta 数据库
    └── 股票数据（Tushare，来自 fcta-equity 项目）
        ├── eq_daily / eq_adj_factor / eq_daily_features
        ├── eq_basic / eq_namechange / eq_index_members
        └── etf_daily
```

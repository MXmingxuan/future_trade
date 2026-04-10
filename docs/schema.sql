-- ============================================================
-- future_trade 数据库 schema
-- 版本：v1.0
-- 日期：2026-04-11
-- 说明：基于"实体-事件-状态"模型，围绕 PTA 产业链
--       品种研究设计，含 8 张核心表
-- ============================================================

BEGIN;

-- -------------------------------------------------------
-- 1. commodity_master — 品种主表
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS commodity_master (
    id              SERIAL PRIMARY KEY,
    commodity_code  VARCHAR(20)  NOT NULL UNIQUE,  -- 如 'PTA', 'PX', 'MEG'
    commodity_name  VARCHAR(100) NOT NULL,
    exchange        VARCHAR(20),                    -- 如 'CZCE'（郑商所）
    contract_unit   VARCHAR(20),                    -- 合约单位（吨/手）
    tick_size       NUMERIC(10,4),                 -- 最小变动价位
    notes           TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE commodity_master IS '商品期货品种主表';
COMMENT ON COLUMN commodity_master.commodity_code IS '品种代码，如 PTA、PX、MEG、短纤';

-- -------------------------------------------------------
-- 2. company_master — 产业链公司主表
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS company_master (
    id              SERIAL PRIMARY KEY,
    stock_code      VARCHAR(10)  NOT NULL UNIQUE,   -- 如 '000301'
    stock_name      VARCHAR(100) NOT NULL,
    exchange        VARCHAR(10),                    -- SZ / SH
    listed          BOOLEAN DEFAULT TRUE,
    main_business   TEXT,                          -- 主营业务描述
    chain_position  VARCHAR(50),                   -- 上游原材料 / PTA生产 / 下游聚酯 / 贸易
    region          VARCHAR(100),                  -- 主要生产基地
    notes           TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE company_master IS '产业链公司主表（含上市公司基础信息）';

-- -------------------------------------------------------
-- 3. company_chain_exposure — 公司-品种产业链暴露度
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS company_chain_exposure (
    id                  SERIAL PRIMARY KEY,
    company_id          INTEGER NOT NULL REFERENCES company_master(id) ON DELETE CASCADE,
    commodity_id        INTEGER NOT NULL REFERENCES commodity_master(id) ON DELETE CASCADE,
    exposure_type       VARCHAR(20),               -- 产能型 / 贸易型 / 消费型
    exposure_ratio      NUMERIC(6,4),              -- 暴露比例（0-1），如 0.35 表示 35% 营收与该品种相关
    capacity_mtpa       NUMERIC(12,2),             -- 该公司在该品种上的产能（万吨/年）
    integration_level   VARCHAR(20),               -- 一体化程度：完全一体化 / 部分一体化 / 无一体化
    profit_elasticity   VARCHAR(20),               -- 利润弹性：高 / 中 / 低
    notes               TEXT,
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW(),
    UNIQUE (company_id, commodity_id)
);

COMMENT ON TABLE company_chain_exposure IS '公司对特定品种的产业链暴露度';
COMMENT ON COLUMN company_chain_exposure.exposure_ratio IS '营收暴露比例，1.0=完全依赖该品种';

-- -------------------------------------------------------
-- 4. price_factor_daily — 每日数值因子（核心数据表）
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS price_factor_daily (
    id                  SERIAL PRIMARY KEY,
    commodity_id        INTEGER NOT NULL REFERENCES commodity_master(id),
    trade_date          DATE    NOT NULL,
    -- 现货
    spot_price          NUMERIC(12,4),             -- 现货价格（元/吨）
    spot_price_change   NUMERIC(12,4),             -- 现货日涨跌
    -- 期货
    fut_close           NUMERIC(12,4),             -- 期货收盘价
    fut_settle          NUMERIC(12,4),            -- 期货结算价
    main_contract       VARCHAR(20),               -- 主力合约代码
    near_contract       VARCHAR(20),              -- 近月合约代码
    -- 基差
    basis_main          NUMERIC(12,4),            -- 主力基差 = 现货 - 期货
    basis_near          NUMERIC(12,4),            -- 近月基差
    basis_rate          NUMERIC(8,4),             -- 基差率 = 基差/期货价格
    -- 过去变化
    basis_5d_change     NUMERIC(12,4),            -- 基差5日变化
    basis_20d_change    NUMERIC(12,4),            -- 基差20日变化
    basis_percentile_60 NUMERIC(8,4),            -- 基差60日分位（0-1）
    -- 仓单
    warehouse_receipt   INTEGER,                  -- 仓单量（张）
    wr_change           INTEGER,                   -- 仓单日变化
    wr_5d_change        INTEGER,                   -- 仓单5日变化
    wr_percentile_60    NUMERIC(8,4),            -- 仓单60日分位
    -- 期限结构
    time_spread         NUMERIC(12,4),            -- 主力-近月月差
    back_or_contango    VARCHAR(10),              -- back(现货强) / contango(期货强)
    -- 加工差（后续扩展）
    processing_margin   NUMERIC(12,4),
    -- 库存（后续扩展）
    social_inventory    NUMERIC(14,2),
    -- Agent A 输出结论
    agent_a_direction   VARCHAR(20),             -- 偏多/偏空/中性
    agent_a_confidence  VARCHAR(10),              -- 高/中/低
    agent_a_summary     TEXT,                     -- Agent A 文字结论
    -- 元数据
    created_at          TIMESTAMP DEFAULT NOW(),
    UNIQUE (commodity_id, trade_date)
);

COMMENT ON TABLE price_factor_daily IS '每日数值因子，核心分析数据表';
CREATE INDEX IF NOT EXISTS idx_pfd_date ON price_factor_daily(trade_date);
CREATE INDEX IF NOT EXISTS idx_pfd_commodity ON price_factor_daily(commodity_id);

-- -------------------------------------------------------
-- 5. disclosure_doc — 公告/报告元数据
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS disclosure_doc (
    id              SERIAL PRIMARY KEY,
    company_id      INTEGER REFERENCES company_master(id),
    stock_code      VARCHAR(10),
    stock_name      VARCHAR(100),
    ann_date        DATE,
    doc_type        VARCHAR(50),                  -- 临时公告 / 年报 / 半年报 / 调研记录 / 业绩预告
    title           VARCHAR(500),
    art_code        VARCHAR(50),                  -- 东方财富文章代码
    source          VARCHAR(50),                  -- 东方财富 / 巨潮 / 证监会
    url             TEXT,
    pdf_url         TEXT,
    raw_text        TEXT,                         -- 原始正文（可为空，先存元数据）
    source_level    INTEGER DEFAULT 1,           -- 证据等级 1-5
    fetched_at       TIMESTAMP DEFAULT NOW(),
    UNIQUE (source, art_code)
);

COMMENT ON TABLE disclosure_doc IS '公告/报告元数据（正文可后续补充）';
CREATE INDEX IF NOT EXISTS idx_dd_company ON disclosure_doc(company_id);
CREATE INDEX IF NOT EXISTS idx_dd_date ON disclosure_doc(ann_date);
CREATE INDEX IF NOT EXISTS idx_dd_type ON disclosure_doc(doc_type);

-- -------------------------------------------------------
-- 6. event_fact — 事件抽取结果（核心输出表）
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS event_fact (
    id                  SERIAL PRIMARY KEY,
    company_id          INTEGER REFERENCES company_master(id),
    commodity_id        INTEGER REFERENCES commodity_master(id),
    -- 事件标识
    event_type          VARCHAR(50) NOT NULL,    -- 扩产/检修/停车/投产/订单变化/成本变化/业绩变动/项目审批
    chain_position      VARCHAR(50),              -- 上游/中游/下游
    product             VARCHAR(100),             -- 涉及产品
    -- 规模
    capacity_mtpa       NUMERIC(12,2),           -- 产能规模（万吨/年），可为null
    affected_volume     NUMERIC(12,2),           -- 影响量（万吨）
    -- 方向
    supply_impact       VARCHAR(20),             -- 增加/减少/不变/不确定
    demand_impact       VARCHAR(20),             -- 增加/减少/不变/不确定
    -- 时间
    effective_date     DATE,                     -- 生效时间
    time_horizon        VARCHAR(20),             -- 短期(<3月)/中期(3-12月)/长期(>1年)
    announcement_date   DATE,                     -- 公告日期
    -- 证据
    evidence_text      TEXT,                     -- 关键证据片段
    confidence          VARCHAR(10) NOT NULL,    -- 高/中/低（对应证据等级 1-3/4/5）
    -- 来源
    source_doc_id       INTEGER REFERENCES disclosure_doc(id),
    -- LLM 推断（明确标注，非事实）
    llm_inference       TEXT,                     -- LLM 推断内容（如：若无补量，短期供给偏紧）
    -- 元数据
    created_at          TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE event_fact IS '从公告中抽取的标准化事件（核心输出）';
COMMENT ON COLUMN event_fact.confidence IS '高=一级至三级证据；中=四级证据；低=五级证据或LLM推断';
CREATE INDEX IF NOT EXISTS idx_ef_company ON event_fact(company_id);
CREATE INDEX IF NOT EXISTS idx_ef_commodity ON event_fact(commodity_id);
CREATE INDEX IF NOT EXISTS idx_ef_type ON event_fact(event_type);
CREATE INDEX IF NOT EXISTS idx_ef_date ON event_fact(effective_date);
CREATE INDEX IF NOT EXISTS idx_ef_ann_date ON event_fact(announcement_date);

-- -------------------------------------------------------
-- 7. commodity_event_timeline — 品种事件时间轴
-- -------------------------------------------------------
-- 将 event_fact 映射到品种层（某龙头公司扩产→PTA需求影响）
CREATE TABLE IF NOT EXISTS commodity_event_timeline (
    id                  SERIAL PRIMARY KEY,
    commodity_id        INTEGER NOT NULL REFERENCES commodity_master(id),
    event_fact_id       INTEGER NOT NULL REFERENCES event_fact(id),
    -- 映射结论
    mapped_supply_impact VARCHAR(20),
    mapped_demand_impact VARCHAR(20),
    mapped_confidence    VARCHAR(10),
    -- 事件聚合标签
    event_tag           VARCHAR(50),             -- 供给扩张/供给收缩/需求改善/需求走弱/成本推动
    -- 时间窗口
    window_start        DATE,
    window_end          DATE,
    -- 聚合信号（Agent C 输出）
    agent_c_supply_trend  VARCHAR(30),
    agent_c_demand_trend  VARCHAR(30),
    agent_c_cost_trend    VARCHAR(30),
    agent_c_conclusion    TEXT,
    created_at          TIMESTAMP DEFAULT NOW(),
    UNIQUE (commodity_id, event_fact_id)
);

COMMENT ON TABLE commodity_event_timeline IS '事件到品种层的映射时间轴';
CREATE INDEX IF NOT EXISTS idx_cet_commodity ON commodity_event_timeline(commodity_id);
CREATE INDEX IF NOT EXISTS idx_cet_date ON commodity_event_timeline(window_start);

-- -------------------------------------------------------
-- 8. commodity_state_daily — 日频品种状态（最终输出）
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS commodity_state_daily (
    id                  SERIAL PRIMARY KEY,
    commodity_id        INTEGER NOT NULL REFERENCES commodity_master(id),
    trade_date          DATE NOT NULL,
    -- 评分（-2 到 +2，0 为中性）
    supply_score        INTEGER,                  -- 供给边际：-2=显著收缩 ~ +2=显著扩张
    demand_score        INTEGER,                  -- 需求边际：-2=显著走弱 ~ +2=显著改善
    cost_score          INTEGER,                  -- 成本边际：-2=成本大幅上升 ~ +2=成本大幅下降
    inventory_score     INTEGER,                  -- 库存边际：-2=库存积压 ~ +2=库存紧张
    -- Agent A/B/C 综合结论
    numeric_direction   VARCHAR(20),             -- 数值因子方向：偏多/偏空/中性
    event_direction     VARCHAR(20),             -- 事件方向：偏多/偏空/中性
    -- 最终综合结论
    overall_direction   VARCHAR(20) NOT NULL,   -- 综合方向：看涨/看跌/震荡
    overall_confidence  VARCHAR(10),             -- 综合置信度
    state_type          VARCHAR(50),             -- 现实偏紧/预期偏强/供需平衡/库存积压/...
    -- Agent D 翻译
    fut_interpretation  JSONB,                   -- {direction, reasoning[], risk_points[]}
    stock_interpretation JSONB,                  -- {direction, affected_companies[], reasoning[]}
    consistency        VARCHAR(20),             -- 期货股票一致性：一致/不一致
    -- 风险和关注点
    key_risks           TEXT[],                  -- 主要风险点
    watch_direction     VARCHAR(100),            -- 值得观察的方向
    -- 证据链摘要
    evidence_summary    TEXT,                   -- 支撑结论的关键事实（引用）
    -- 元数据
    generated_at        TIMESTAMP DEFAULT NOW(),
    UNIQUE (commodity_id, trade_date)
);

COMMENT ON TABLE commodity_state_daily IS '日频品种状态卡（最终输出，每日一条）';
CREATE INDEX IF NOT EXISTS idx_csd_date ON commodity_state_daily(trade_date);
CREATE INDEX IF NOT EXISTS idx_csd_commodity ON commodity_state_daily(commodity_id);

-- -------------------------------------------------------
-- 初始化品种数据
-- -------------------------------------------------------
INSERT INTO commodity_master (commodity_code, commodity_name, exchange, contract_unit, tick_size)
VALUES
    ('PTA',   '精对苯二甲酸', 'CZCE', '5吨/手',  2),
    ('PX',    '对二甲苯',     'CZCE', '5吨/手',  2),
    ('MEG',   '乙二醇',       'CZCE', '10吨/手', 1),
    ('MA',    '甲醇',         'CZCE', '10吨/手', 1),
    ('PP',    '聚丙烯',       'CZCE', '5吨/手',  1),
    ('PF',    '短纤',         'CZCE', '5吨/手',  2)
ON CONFLICT (commodity_code) DO NOTHING;

-- -------------------------------------------------------
-- 初始化公司数据（PTA产业链8家）
-- -------------------------------------------------------
INSERT INTO company_master (stock_code, stock_name, exchange, main_business, chain_position, region)
VALUES
    ('000301', '东方盛虹', 'SZ', '炼化+聚酯+PTA', '上游原材料+中游', '江苏连云港'),
    ('000703', '恒逸石化', 'SZ', 'PTA+锦纶',      '上游原材料+中游', '浙江+文莱'),
    ('002493', '荣盛石化', 'SZ', 'PX+PTA+聚酯',   '上游原材料+中游', '浙江舟山'),
    ('600346', '恒力石化', 'SH', '炼化+PTA+聚酯', '上游原材料+中游', '江苏南通'),
    ('601233', '桐昆股份', 'SH', '聚酯长丝',      '下游聚酯',         '浙江桐乡'),
    ('603225', '新凤鸣',   'SH', '聚酯长丝',      '下游聚酯',         '浙江桐乡'),
    ('301216', '万凯新材', 'SZ', '瓶片',           '下游聚酯',         '浙江嘉兴'),
    ('600370', '三房巷',   'SH', '瓶片+PTA贸易',  '下游聚酯',         '江苏江阴')
ON CONFLICT (stock_code) DO NOTHING;

-- -------------------------------------------------------
-- 初始化暴露度数据
-- -------------------------------------------------------
INSERT INTO company_chain_exposure (company_id, commodity_id, exposure_type, exposure_ratio, capacity_mtpa, integration_level, profit_elasticity)
SELECT
    cm.id, cm2.id,
    CASE cm.chain_position
        WHEN '上游原材料+中游' THEN '产能型'
        WHEN '下游聚酯' THEN '消费型'
        ELSE '贸易型'
    END,
    CASE cm.chain_position
        WHEN '上游原材料+中游' THEN 0.60
        WHEN '下游聚酯' THEN 0.75
        ELSE 0.30
    END,
    CASE cm.stock_code
        WHEN '000301' THEN 400
        WHEN '000703' THEN 600
        WHEN '002493' THEN 500
        WHEN '600346' THEN 2000
        WHEN '601233' THEN 0
        WHEN '603225' THEN 0
        WHEN '301216' THEN 0
        WHEN '600370' THEN 0
    END,
    CASE cm.chain_position
        WHEN '上游原材料+中游' THEN '完全一体化'
        WHEN '下游聚酯' THEN '无一体化'
        ELSE '部分一体化'
    END,
    '高'
FROM company_master cm
CROSS JOIN commodity_master cm2
WHERE cm2.commodity_code = 'PTA'
ON CONFLICT (company_id, commodity_id) DO NOTHING;

COMMIT;

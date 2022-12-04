CREATE TABLE IF NOT EXISTS `kline`(
   `market_cate1` VARCHAR(10) NOT NULL COMMENT '市场1级类目, 1000:数字货币 1000:股票',
   `market_cate2` VARCHAR(10) NOT NULL COMMENT '市场2级类目, 1001:数字货币-现货 1002:数字货币-U本位合约',
   `interval` VARCHAR(10) NOT NULL COMMENT '时间周期',
   `symbol` VARCHAR(20) NOT NULL COMMENT '代码',
   `open` DOUBLE NOT NULL COMMENT '开盘价',
   `close` DOUBLE NOT NULL COMMENT '收盘价',
   `high` DOUBLE NOT NULL COMMENT '最高价',
   `low` DOUBLE NOT NULL COMMENT '最低价',
   `volume` DOUBLE NOT NULL COMMENT '交易量',
   `taker_amt` DOUBLE NOT NULL COMMENT 'taker_amt',
   `taker_volume` DOUBLE NOT NULL COMMENT 'taker_volume',
   `trd_amt` DOUBLE NOT NULL COMMENT 'trd_amt',
   `trd_cnt` DOUBLE NOT NULL COMMENT 'trd_cnt',
   `open_ts` BIGINT NOT NULL COMMENT '开盘时间戳',
   `close_ts` BIGINT NOT NULL COMMENT '收盘时间戳',
   `open_dt` DATETIME NOT NULL COMMENT '开盘时间戳',
   `close_dt` DATETIME NOT NULL COMMENT '收盘时间戳',
   `is_delete` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '逻辑删除',
   `update_time` TIMESTAMP NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
   `insert_time` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
   INDEX index1(`symbol`, `interval`),
   INDEX index_is_delete(`is_delete`),
   INDEX index_open_dt(`open_dt`),
   PRIMARY KEY(`symbol`, `market_cate2`, `interval`, `open_ts`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='历史k线表'
;

CREATE TABLE IF NOT EXISTS `openInterestHist`(
   `market_cate1` VARCHAR(10) NOT NULL COMMENT '市场1级类目, 1000:数字货币 2000:股票',
   `market_cate2` VARCHAR(10) NOT NULL COMMENT '市场2级类目, 1001:数字货币-现货 1002:数字货币-U本位合约',
   `interval` VARCHAR(10) NOT NULL COMMENT '时间周期',
   `symbol` VARCHAR(20) NOT NULL COMMENT '代码',
   `sumOpenInterest` DOUBLE NOT NULL COMMENT 'sumOpenInterest',
   `sumOpenInterestValue` DOUBLE NOT NULL COMMENT 'sumOpenInterestValue',
   `timestamp` BIGINT NOT NULL COMMENT 'timestamp',
   `datetime` DATETIME NOT NULL COMMENT 'datetime',
   `is_delete` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '逻辑删除',
   `update_time` TIMESTAMP NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
   `insert_time` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
   INDEX index1(`symbol`, `interval`),
   INDEX index_is_delete(`is_delete`),
   INDEX index_open_dt(`datetime`),
   PRIMARY KEY(`symbol`, `market_cate2`, `interval`, `datetime`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='开仓统计'
;


CREATE TABLE IF NOT EXISTS `topLongShortAccountRatio`(
   `market_cate1` VARCHAR(10) NOT NULL COMMENT '市场1级类目, 1000:数字货币 2000:股票',
   `market_cate2` VARCHAR(10) NOT NULL COMMENT '市场2级类目, 1001:数字货币-现货 1002:数字货币-U本位合约',
   `interval` VARCHAR(10) NOT NULL COMMENT '时间周期',
   `symbol` VARCHAR(20) NOT NULL COMMENT '代码',
   `longShortRatio` DOUBLE NOT NULL COMMENT 'longShortRatio',
   `long` DOUBLE NOT NULL COMMENT 'longAccount',
   `short` DOUBLE NOT NULL COMMENT 'shortAccount',
   `timestamp` BIGINT NOT NULL COMMENT 'timestamp',
   `datetime` DATETIME NOT NULL COMMENT 'datetime',
   `is_delete` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '逻辑删除',
   `update_time` TIMESTAMP NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
   `insert_time` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
   INDEX index1(`symbol`, `interval`),
   INDEX index_is_delete(`is_delete`),
   INDEX index_open_dt(`datetime`),
   PRIMARY KEY(`symbol`, `market_cate2`, `interval`, `datetime`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='大户多空账户比'
;

CREATE TABLE IF NOT EXISTS `topLongShortPositionRatio`(
   `market_cate1` VARCHAR(10) NOT NULL COMMENT '市场1级类目, 1000:数字货币 2000:股票',
   `market_cate2` VARCHAR(10) NOT NULL COMMENT '市场2级类目, 1001:数字货币-现货 1002:数字货币-U本位合约',
   `interval` VARCHAR(10) NOT NULL COMMENT '时间周期',
   `symbol` VARCHAR(20) NOT NULL COMMENT '代码',
   `longShortRatio` DOUBLE NOT NULL COMMENT 'longShortRatio',
   `long` DOUBLE NOT NULL COMMENT 'longPosition',
   `short` DOUBLE NOT NULL COMMENT 'shortPosition',
   `timestamp` BIGINT NOT NULL COMMENT 'timestamp',
   `datetime` DATETIME NOT NULL COMMENT 'datetime',
   `is_delete` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '逻辑删除',
   `update_time` TIMESTAMP NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
   `insert_time` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
   INDEX index1(`symbol`, `interval`),
   INDEX index_is_delete(`is_delete`),
   INDEX index_open_dt(`datetime`),
   PRIMARY KEY(`symbol`, `market_cate2`, `interval`, `datetime`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='大户多空仓位比'
;

CREATE TABLE IF NOT EXISTS `globalLongShortAccountRatio`(
   `market_cate1` VARCHAR(10) NOT NULL COMMENT '市场1级类目, 1000:数字货币 2000:股票',
   `market_cate2` VARCHAR(10) NOT NULL COMMENT '市场2级类目, 1001:数字货币-现货 1002:数字货币-U本位合约',
   `interval` VARCHAR(10) NOT NULL COMMENT '时间周期',
   `symbol` VARCHAR(20) NOT NULL COMMENT '代码',
   `longShortRatio` DOUBLE NOT NULL COMMENT 'longShortRatio',
   `long` DOUBLE NOT NULL COMMENT 'longAccount',
   `short` DOUBLE NOT NULL COMMENT 'shortAccount',
   `timestamp` BIGINT NOT NULL COMMENT 'timestamp',
   `datetime` DATETIME NOT NULL COMMENT 'datetime',
   `is_delete` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '逻辑删除',
   `update_time` TIMESTAMP NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
   `insert_time` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
   INDEX index1(`symbol`, `interval`),
   INDEX index_is_delete(`is_delete`),
   INDEX index_open_dt(`datetime`),
   PRIMARY KEY(`symbol`, `market_cate2`, `interval`, `datetime`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='多空人数比'
;

CREATE TABLE IF NOT EXISTS `takerBuySellVolRatio`(
   `market_cate1` VARCHAR(10) NOT NULL COMMENT '市场1级类目, 1000:数字货币 2000:股票',
   `market_cate2` VARCHAR(10) NOT NULL COMMENT '市场2级类目, 1001:数字货币-现货 1002:数字货币-U本位合约',
   `interval` VARCHAR(10) NOT NULL COMMENT '时间周期',
   `symbol` VARCHAR(20) NOT NULL COMMENT '代码',
   `buySellRatio` DOUBLE NOT NULL COMMENT '', 
   `buy` DOUBLE NOT NULL COMMENT '',
   `sell` DOUBLE NOT NULL COMMENT '',
   `buyValue` DOUBLE NOT NULL COMMENT '',
   `sellValue` DOUBLE NOT NULL COMMENT '',
   `timestamp` BIGINT NOT NULL COMMENT 'timestamp',
   `datetime` DATETIME NOT NULL COMMENT 'datetime',
   `is_delete` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '逻辑删除',
   `update_time` TIMESTAMP NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
   `insert_time` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
   INDEX index1(`symbol`, `interval`),
   INDEX index_is_delete(`is_delete`),
   INDEX index_open_dt(`datetime`),
   PRIMARY KEY(`symbol`, `market_cate2`, `interval`, `datetime`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='多空人数比'
;

CREATE TABLE IF NOT EXISTS `jicha`(
   `market_cate1` VARCHAR(10) NOT NULL COMMENT '市场1级类目, 1000:数字货币 2000:股票',
   `market_cate2` VARCHAR(10) NOT NULL COMMENT '市场2级类目, 1001:数字货币-现货 1002:数字货币-U本位合约',
   `interval` VARCHAR(10) NOT NULL COMMENT '时间周期',
   `symbol` VARCHAR(20) NOT NULL COMMENT '代码',
   `indexPrice` DOUBLE NOT NULL COMMENT '', 
   `basisRate` DOUBLE NOT NULL COMMENT '',
   `futuresPrice` DOUBLE NOT NULL COMMENT '',
   `basis` DOUBLE NOT NULL COMMENT '',
   `timestamp` BIGINT NOT NULL COMMENT 'timestamp',
   `datetime` DATETIME NOT NULL COMMENT 'datetime',
   `is_delete` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '逻辑删除',
   `update_time` TIMESTAMP NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
   `insert_time` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
   INDEX index1(`symbol`, `interval`),
   INDEX index_is_delete(`is_delete`),
   INDEX index_open_dt(`datetime`),
   PRIMARY KEY(`symbol`, `market_cate2`, `interval`, `datetime`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='基差'
;

CREATE TABLE IF NOT EXISTS `binance_exchange_info`(
    `market_cate1` VARCHAR(10) NOT NULL COMMENT '市场1级类目, 1000:数字货币 1000:股票',
    `market_cate2` VARCHAR(10) NOT NULL COMMENT '市场2级类目, 1001:数字货币-现货 1002:数字货币-U本位合约',
    `symbol` VARCHAR(20) NOT NULL COMMENT '代码',
    `status` VARCHAR(20) NOT NULL COMMENT '交易状态',
    `baseAsset` VARCHAR(20) NOT NULL COMMENT 'baseAsset',
    `quoteAsset` VARCHAR(20) NOT NULL COMMENT 'quoteAsset',
    `contractType` VARCHAR(20) DEFAULT '' COMMENT 'contractType',
    `onboardDate` BIGINT DEFAULT 0 COMMENT 'onboardDate',
    `marginAsset` VARCHAR(20) DEFAULT '' COMMENT 'marginAsset',
    `underlyingType` VARCHAR(20) DEFAULT '' COMMENT 'underlyingType',
    `liquidationFee` DOUBLE DEFAULT 0 COMMENT 'liquidationFee',
    `is_delete` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '逻辑删除',
    `update_time` TIMESTAMP NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `insert_time` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(`symbol`, `market_cate2`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='交易对信息表'
;


select 
a.open_dt,
a.symbol,
max(a.close) as close,
group_concat(concat(b.symbol,':',b.close)) as other
from (
select 
symbol,open_dt,close
from kline 
where `interval`='15m'
and market_cate2='1001'
and symbol='BTCUSDT'
) a
left join (
select 
    symbol,open_dt, close from kline where `interval`='15m' and market_cate2='1001'
    ) b on (a.open_dt=b.open_dt)
group by a.symbol,a.open_dt
order by a.symbol,a.open_dt desc limit 100
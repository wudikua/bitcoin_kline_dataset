# 项目说明

* 项目下载binance历史行情数据（日线、小时线、分钟线 cross  u本位合约、币本位合约、币行情），并导入到mysql，支持增量全量数据更新
* schema.sql 对应表结构
* data_loader.py 内包含主要实现
* binance_crontab.py 内包含定时任务代码，支持使用proxy挂代理下载。
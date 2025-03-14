-- Hive SQL
-- Function： 产品的domestic_sto生命周期
---行数据表示：一个产品一个批号的生命周期
-- History: 
-- 2021-06-15    Donny   v1.0    init

drop table if exists dws_plc_domestic_sto_daily_trans;
create external table dws_plc_domestic_sto_daily_trans
(
     material                               string comment 'STO' 
    ,batch                                  string comment 'DN'
    ,qr_code                                string comment 'QR code'
    ,domestic_sto                           string
    ,domestic_sto_dn                        string
    ,domestic_sto_create_dt                 string comment '国内转仓STO 创建时间'
    ,domestic_dn_create_dt                  string comment '国内转仓STO 创建时间'
    ,domestic_pgi                           string comment '国内转仓PGI'
    ,domestic_migo                          string comment '国内转仓DN MIGO'
    ,domestic_putaway                       string comment '国内转仓Putaway'
) comment 'Product Life Cycle Domestic Sto Daily Transation'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_plc_domestic_sto_daily_trans/'
tblproperties ("parquet.compression"="lzo");
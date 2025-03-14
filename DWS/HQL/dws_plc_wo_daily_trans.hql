-- Hive SQL
-- Function： 产品的生命周期
---行数据表示：一个产品一个批号的生命周期
-- History: 
-- 2021-06-15    Donny   v1.0    init

drop table if exists dws_plc_wo_daily_trans;
create external table dws_plc_wo_daily_trans
(
    work_order_no                           string 
    ,material                               string comment 'STO' 
    ,batch                                  string comment 'Batch Number'
    ,qr_code                                string comment 'QR code'
    ,wo_created_dt                          string comment '工单创建时间'
    ,wo_completed_dt                        string comment '本地化结束的时间'
    ,wo_release_dt                          string comment 'QA 工单检验完成时间'
    ,wo_internal_putway                     string comment '本仓上架时间Putaway'
) comment 'Product Life WO Cycle Daily Transation'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_plc_wo_daily_trans/'
tblproperties ("parquet.compression"="lzo");
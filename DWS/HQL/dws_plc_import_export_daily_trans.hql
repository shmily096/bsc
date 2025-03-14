-- Hive SQL
-- Function： 产品的生命周期
---行数据表示：一个产品一个批号的生命周期
-- History: 
-- 2021-06-15    Donny   v1.0    init

drop table if exists dws_plc_import_export_daily_trans;
create external table dws_plc_import_export_daily_trans
(
     material                               string comment 'STO' 
    ,batch                                  string comment 'DN'
    ,import_dn                              string
    ,import_dn_created_datetime             string comment 'DN created date' --DN创建时间 
    ,import_pgi                             string comment 'STO DN PGI' --T1 发货过账时间 PGI
    ,import_pick_up_date                    string comment 't1 pick up date' --T1实际提货时间
    ,import_invoice_receiving_date          string comment 'receving mail date' --预报时间
    ,import_actual_arrival_time             string comment 'Actul arrival time' -- 到港时间
    ,import_dock_warrant_date               string comment 'Manifests started time' -- 舱单开始时间
    ,import_declaration_start_date          string comment 'Declaration started date' --报关开始时间
    ,import_declaration_completion_date     string comment 'Declaration completion date' --报关完成日期
    ,import_into_inventory_date             string comment '实际到仓时间'
    ,import_migo                            string comment '进口Migo时间' --DN单实际收货时间
) comment 'Product Life Cycle import export Daily Transation'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_plc_import_export_daily_trans/'
tblproperties ("parquet.compression"="lzo");
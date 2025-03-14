-- Hive SQL
-- Function： 进口leadtime
---行数据表示：一个DN的进口节点
-- History: 
-- 2021-06-15    Donny   v1.0    init

drop table if exists dws_import_export_daily_trans;
create external table dws_import_export_daily_trans
(
     pgi_date                                   string  
    ,import_migo                                string 
    ,import_declaration_completion_date         string 
    ,import_dock_warrant_date                   string 
    ,import_actual_arrival_time                 string 
    ,import_pgi                                 string 
    ,import_dn                                  string
    ,work_order_no                              string 
    ,work_order_num                             string
    ,inbound_leadtime                           float 
    ,inter_trans_leadtime                       float 
    ,migo_leadtime                              float
    ,import_record_leadtime                     float 
) comment 'import_export Daily Transation'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_import_export_daily_trans/'
tblproperties ("parquet.compression"="lzo");
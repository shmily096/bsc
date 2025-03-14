-- hive sql
-- function:qrcode维度看slc货物上架leadtime
-- 按dn_pgi日期进行分区存储,每个分区存储的是当前已putaway的所有本地化,本仓上架
-- 行数据表示qrcode下对应的所有本地化,本仓上架
-- history:
-- 2021-06-27    amanda   v1.0    init

drop table if exists dws_product_putaway_leadtime_yh_daily_trans;
create external table dws_product_putaway_leadtime_yh_daily_trans
(
    pgi_date                              string comment 'pgi时间'
    ,qr_code_num                              string comment 'qr code数量'
	,work_order_num                        string comment '工单数量'
    ,import_dn                             string
    ,work_order_no                        string
	,domestic_putaway                     string
	,domestic_migo                        string
	,domestic_sto_create_dt               string
    ,wo_released_dt                       string comment '工单释放时间'
    ,import_migo                          string
    ,yh_putaway                           float comment 'yh货物上架leadtime'
    ,localization                         float comment 'yh本地化leadtime'
    ,domestic_trans                       float comment 'yh转仓leadtime'
    ,putaway                              float comment '外仓上架leadtime'
)
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_product_putaway_leadtime_yh_daily_trans/'
tblproperties ("parquet.compression"="lzo");

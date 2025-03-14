-- Hive SQL
-- Function： 进出口发货单详情实事表
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-13    Donny   v1.1    update fields

drop table if exists dwd_fact_import_export_dn_detail;
create external table dwd_fact_import_export_dn_detail
(
    sto_no                  string COMMENT '进出口转仓单编码',
    delivery_no             string COMMENT '进出口发货单编码',
    line_number             string COMMENT '发货单行号',
    material_code           string COMMENT '产品代码',
    qty                     bigint COMMENT '发货数量',
    batch_number            string COMMENT '批号',
    ship_from_location      string COMMENT '发货仓位',
    ship_to_location        string COMMENT '收货仓位'
) COMMENT '进出口发货单详情'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_import_export_dn_detail/'
tblproperties ("parquet.compression"="lzo");
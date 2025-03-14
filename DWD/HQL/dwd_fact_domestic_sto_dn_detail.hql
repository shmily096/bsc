-- Hive SQL
-- Function： 国内转仓发货单详情
-- History: 
-- 2021-05-07    Donny   v1.0    draft

drop table if exists dwd_fact_domestic_sto_dn_detail;
create external table dwd_fact_domestic_sto_dn_detail
(
    sto_no            string comment 'T2-T3转仓单编号',
    delivery_no       string comment '国内发货单编号',
    line_number       string comment '发货单行号',
    material          string comment '产品代码',
    qty               bigint comment '数量',
    batch             string comment '批次',
    qr_code           string comment 'QR code'
) comment '国内转仓发货单详情'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_domestic_sto_dn_detail/'
tblproperties ("parquet.compression"="lzo");
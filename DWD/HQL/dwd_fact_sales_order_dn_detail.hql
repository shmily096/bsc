
-- Hive SQL
-- Function： 国内销售发货单详情事实表 
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-18    Donny   v1.1    update field name

drop table if exists dwd_fact_sales_order_dn_detail;
create external table dwd_fact_sales_order_dn_detail
(
   so_no             string comment '销售订单编码',
   delivery_id       string comment '发货单编码',
   line_number       string comment '发货单行号',
   material          string comment '产品代码',
   qty               string comment '发货数量',
   batch             string comment '批次',
   qr_code           string comment '发货QR'
) comment '国内销售发货单详情'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_sales_order_dn_detail/'
tblproperties ("parquet.compression"="lzo");
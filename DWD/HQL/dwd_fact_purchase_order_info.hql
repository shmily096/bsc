
-- Hive SQL
-- Function： 三方成品采购订单事实表 
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-18    Donny   v1.1    update the field information

drop table if exists dwd_fact_purchase_order_info;
create external table dwd_fact_purchase_order_info
(
    purch_id           string comment '采购单编号',
    po_status          string comment '采购单状态',
    created_datetime   string comment '采购单创建日期（时间）',
    updated_datetime   string comment '更新日期',
    created_by         string comment '创建人',
    updated_by         string comment '更新人',
    line_number        string comment '采购单行',
    material           string comment '产品代码',
    qty                bigint comment '采购数量',
    received_qty       bigint comment '收货数量',
    unit               string comment '单位',
    purch_price        decimal(16,2) comment '采购价格',
    currency           string comment '币种',
    migo_date          string comment '收货日期',
    to_plant_id        string comment '收货plant代码',
    to_locatoin_id     string comment '收货仓位代码',
    financial_dimension_id string comment '财务维度'
) comment '三方成品采购订单'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_purchase_order_info/'
tblproperties ("parquet.compression"="lzo");
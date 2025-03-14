-- Hive SQL
-- Function： 国内销售发货单事实表 
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-28    Donny   v1.1    change table name
-- 2021-06-08    Donny   v1.2    add field receiving_confirmation_date

drop table if exists dwd_fact_sales_order_dn_info;
create external table dwd_fact_sales_order_dn_info
(
    so_no               string comment '销售订单编码',
    delivery_id         string comment '发货单编码',
    created_datetime    string comment '发货单创建日期',
    updated_datetime    string comment '发货单更新日期',
    created_by          string comment '发货单创建人',
    updated_by          string comment '发货单更新人',
    ship_to_address     string comment '地址编码',
    real_shipto_address string comment '实际送货地址',
    planned_gi_date     string comment '计划出库时间',
    actual_gi_date      string comment '实际出库时间',
    receiving_confirmation_date string comment '客户确认收货时间',
    delivery_mode       string comment '发货方式',
    carrier_id          string comment '承运人',
    pick_location_id    string comment '发货仓位',
    total_qty           bigint comment '合计数量'
) comment '国内销售发货单'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_sales_order_dn_info/'
tblproperties ("parquet.compression"="lzo");
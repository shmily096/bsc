-- Hive SQL
-- Function： 销售订单事实表
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-07    Donny   v1.1    update the name of fields
-- 2021-05-24    Donny   v1.2    update the name of fields
-- 2021-06-18    Donny   v1.3    add division
--alter table dwd_fact_sales_order_info add columns (sub_divison string, rebate_rate float);
drop table if exists dwd_fact_sales_order_info;
create external table dwd_fact_sales_order_info
(
    so_no               string comment '销售订单编码',
    order_type          string comment '订单类型',
    order_reason        string comment '订单原因',
    reject_reason       string comment '拒绝原因',
    order_remarks       string comment '订单备注',
    created_datetime    string comment '订单创建日期',
    updated_datetime    string comment '订单更新日期',
    created_by          string comment '订单创建人',
    updated_by          string comment '订单更新人',
    order_status        string comment '订单状态',
    reference_po_number string comment '关联PO',
    line_number         string comment '订单行号',
    material            string comment '产品编码',
    batch               string comment '批号', 
    qty                 bigint comment '数量',
    unit                string comment '单位',
    net_value           decimal(16,2)  comment '金额',
    currency            string comment '币种',
    request_delivery_date string comment '客户要求到货时间',
    pick_up_plant       string comment '发出plant',
    customer_code       string comment 'Sold To',
    ship_to_code        string comment '客户地址代码',
    division_id         string comment 'BU维度',
    customer_level3     string comment  'Customer Level3',
    customer_type       string comment 'Customer Type-Customer Level 4 code',
    order_operation_type string comment 'Order Operation Type',
    default_location    string comment 'Default location',
    sub_divison         string comment 'Sub Division',
    rebate_rate         float comment 'Rebate Rate',
    item_type           string comment 'Item type'
) comment 'Sales Order Fact Table'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_sales_order_info/'
tblproperties ("parquet.compression"="lzo");
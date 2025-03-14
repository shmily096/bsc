-- Hive SQL
-- Function： ODS 销售订单
-- History: 
-- 2021-05-08    Donny   v1.0    draft

drop table if exists ods_sales_order;
create external table ods_sales_order
(
    id                bigint,
    update_dt         string,
    active            string,
    so_no             string,
    order_type        string,
    order_reason      string,
    reject_reason     string,
    order_remarks     string,
    so_create_dt      string,
    so_update_dt      string,
    so_create_by      string,
    so_updated_by     string,
    so_status         string,
    po_number         string,
    sales_org         string,
    storage_loc       string,
    soline_no         string,
    material          string,
    batch             string,
    profit_center     string,
    delivery_date     string,
    qty               bigint,
    net_value         decimal(16,2)  comment '金额',
    currency          string,
    delivery_block    string,
    billing_block     string,
    unit              string,
    request_delivery_date string,
    pick_up_plant     string,
    customer_code     string,
    ship_to_code      string
) comment '销售订单'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_sales_order/';
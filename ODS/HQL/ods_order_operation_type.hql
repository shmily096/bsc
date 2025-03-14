-- Hive SQL
-- Function： SO order operation type（ODS 层）
-- History: 
-- 2021-05-25   Donny   v1.0    init

drop table if exists ods_order_operation_type;
create external table ods_order_operation_type
(
    master_id                   string comment 'Customer Type+Order Type + Order Reason', 
    operation_type              string comment 'order operation type chinese description', --订单操作类型
    order_type                  string comment 'SAP Order Type', 
    order_reason                string comment 'SAP Order Reason', 
    order_reason_description    string comment 'SAP Order Reason description', 
    customer_type               string comment 'Customer type', --客户类型
    business_type               string comment 'Business Type', --业务类型
    flow_direction              string comment 'Flow Direction', --流向
    order_type_and_reason       string comment 'SAP Order Type & Order Reason', 
    wms_operation               string comment 'WMS Operation' -- 仓库WMS逻辑及操作
) comment 'Order operation type master data'
row format delimited fields terminated by '\t' 
location '/bsc/opsdw/ods/ods_order_operation_type/';
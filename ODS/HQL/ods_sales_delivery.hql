-- Hive SQL
-- Function： ODS 销售发货单
-- History: 
-- 2021-05-08    Donny   v1.0    draft

drop table if exists ods_sales_delivery;
create external table ods_sales_delivery
(
    id                bigint comment 'ID',
    update_dt         string comment '更新时间',
    active            string comment 'Active',
    so_no              string comment '销售订单编码',
    sap_delivery_no   string comment '发货单编码',
    dn_create_dt      string comment '发货单创建日期',
    dn_update_dt      string comment '发货单更新日期',
    dn_create_by      string comment '发货单创建人',
    dn_updated_by     string comment '发货单更新人',
    ship_to           string comment '地址编码',
    real_ship_to_address string comment '实际送货地址',
    delivery_line     string comment '发货单行号',
    material          string comment '产品代码',
    qty               string comment '发货数量',
    qr_code           string comment '发货QRCode',
    batch             string comment '批次',
    planned_gi_date    string comment '计划出库时间',
    actual_gi_date     string comment '实际出库时间',
    delivery_mode     string comment '发货方式',
    carrier           string comment '承运人',
    pick_location     string comment '发货仓位'
) comment '销售发货单'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_sales_delivery/';
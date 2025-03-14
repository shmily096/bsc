-- Hive SQL
-- Function： ODS 进出口发货单
-- History: 
--  2021-05-07    Donny   v1.0    draft
drop table if exists ods_import_export_delivery;
create external table ods_import_export_delivery
(
    id                bigint,
    update_dt         string,
    active            string,
    sto_no            string comment '进出口转仓单编码',
    sap_delivery_no_inbound string comment '进出口发货单编码',
    sap_delivery_no_outbound string comment '进出口发货单编码',
    dn_create_dt      string comment '发货单创建日期',
    dn_status         string comment '发货单状态',
    dn_update_dt      string comment '发货单更新日期',
    dn_create_by      string comment '发货单创建人',
    dn_updated_by     string comment '发货单更新人',
    receiver_customer_code string comment '收货方客户代码',
    sap_delivery_line_no string comment '发货单行号',
    material          string comment '产品代码',
    qty               string comment '发货数量',
    batch             string comment '批号',
    ship_from_plant   string comment '发货plant',
    ship_from_location string comment '发货仓位',
    ship_to_plant     string comment '收货plant',
    ship_to_location  string comment '收货仓位',
    delivery_mode     string comment '发运方式',
    actual_migo_dt    string comment '实际收货日期',
    pgi_date          string COMMENT '计划发货时间'
) comment '进出口发货单'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_import_export_delivery/'
;
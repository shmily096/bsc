-- Hive SQL
-- Function： ODS 国内转仓发货单
-- History: 
-- 2021-05-08    Donny   v1.0    draft
-- 2021-05-21    Donny   v1.1    add new field:pgi_date

drop table if exists ods_domestic_delivery;
create external table ods_domestic_delivery
(
    id                bigint comment 'ID',
    update_dt         string comment '更新日期',
    active            string comment 'Active',
    sto_no            string comment 'T2-T3转仓单编号',
    sap_delivery_no_inbound string comment '国内发货单编号',
    sap_delivery_no_outbound string comment '国内发货单编号',
    dn_create_dt      string comment '发货单创建日期',
    dn_create_by      string comment '创建人',
    dn_update_dt      string comment '发货单更新日期',
    dn_update_by      string comment '修改人',
    dn_status         string comment '发货单状态',
    sap_delivery_line_no string comment '发货单行号',
    material          string comment '产品代码',
    qty               string comment '数量',
    batch             string comment '批次',
    ship_from_plant   string comment '转仓单发送plant',
    ship_from_location string comment '转仓单发出仓位',
    ship_to_plant     string comment '收货plant',
    ship_to_location  string comment '收货仓位',
    qr_code           string comment 'QR code',
    delivery_mode     string comment '发货模式',
    carrier           string comment '承运人',
    actual_migo_dt    string comment '实际收货日期',
    pgi_date          string COMMENT '计划发货时间'
) comment '国内转仓发货单'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_domestic_delivery/';
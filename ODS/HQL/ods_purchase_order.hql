-- Hive SQL
-- Function： ODS 三方成品采购订单
-- History: 
-- 2021-05-08    Donny   v1.0    draft

drop table if exists ods_purchase_order;
create external table ods_purchase_order
(
    id                bigint comment 'ID',
    update_dt         string comment '更新时间',
    active            string comment 'Active',
    purchase_order_no string comment '采购单编号',
    po_create_dt      string comment '采购单创建日期',
    po_create_by      string comment '创建人',
    po_updated_dt     string comment '更新日期>',
    po_updated_by     string comment '更新人',
    po_status         string comment '采购单状态',
    po_line_no        string comment '采购单行',
    material          string comment '产品代码',
    qty               bigint comment '采购数量',
    unit              string comment '单位',
    purchase_price    decimal(16,2) comment '采购价格',
    currency          string comment '币种',
    migo_date         string comment '收货日期',
    batch             string comment '批次',
    received_qty      bigint comment '收货数量'
) comment '三方成品采购订单'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_purchase_order/';
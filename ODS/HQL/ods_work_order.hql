-- Hive SQL
-- Function： ODS 发货单对应表
-- History: 
--  2021-05-08    Donny   v1.0    draft

drop table if exists ods_work_order;
create external table ods_work_order
(
    update_dt         string comment '更新时间',
    plant             string comment '本地化工单创建仓库',
    commercial_invoice_no string comment '商业发票',
    sap_delivery_no   string comment '发货单编号',
    work_order_no     string comment '工单编号',
    create_dt         string comment '工单创建日期',
    create_by         string comment '工单创建人',
    work_order_status string comment '工单状态',
    start_dt          string comment '工单执行开始日期',
    started_by        string comment '工单执行人',
    complete_dt       string comment '工单完成日期',
    release_dt        string comment '工单质量放行日期',
    release_by        string comment '工单质量放行人',
    line_no           string comment '工单行号',
    material          string comment '产品代码',
    batch             string comment '批次',
    current_qty       string comment '计划本地化数量',
    processed_qty     string comment '执行数量',
    qr_code           string comment 'QR Code',
    release_qty       string comment '放行数量'
) comment '本地化工单'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_work_order/';
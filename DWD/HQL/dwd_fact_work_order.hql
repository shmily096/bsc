-- Hive SQL
-- Function： 本地化工单信息事实表
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-24    Donny   v1.1    update table schema
-- 2021-05-29    Donny   v1.2    fix typo issue

drop table if exists dwd_fact_work_order;
create external table dwd_fact_work_order
(
    plant_id                string comment '本地化工单创建仓库',
    commercial_invoice_no   string comment '商业发票ID',
    delivery_no             string comment '发货单编号',
    work_order_no           string comment '工单编号',
    created_datetime        string comment '工单创建日期（时间）',
    created_by              string comment '工单创建人',
    started_datetime        string comment '工单执行开始日期（时间）',
    started_by              string comment '工单执行人',
    completed_datetime      string comment '工单完成日期（时间）',
    released_datetime       string comment '工单质量放行日期（时间）',
    released_by             string comment '工单质量放行人',
    line_no                 string comment '工单行号',
    material                string comment '产品代码',
    batch                   string comment '批次',
    current_qty             string comment '计划本地化数量',
    processed_qty           string comment '执行数量',
    release_qty             string comment '放行数量',
    qr_code                 string comment 'QR Code',
    work_order_status       string COMMENT '工单状态'
) COMMENT 'Work order loclization fact information'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_work_order/'
tblproperties ("parquet.compression"="lzo");
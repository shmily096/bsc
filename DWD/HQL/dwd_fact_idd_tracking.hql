-- Hive SQL
-- Function： IDD Tracking Information
-- History: 
-- 2021-05-07    Donny   v1.0    draft

drop table if exists dwd_fact_idd_tracking;
create external table dwd_fact_idd_tracking
(
    idd_delivery      string comment 'DN编号',
    material_code     string comment '产品编号',
    batch             string comment '批号',
    Declaration_state string comment '申报状态',
    report_date       string comment 'IDD发现日期',
    idd_type          string comment 'IDD种类',
    qty               string comment 'IDD数量',
    packing_list_datetime string comment 'BSC提供PackingList日期',
    slc_response_datetime string comment 'SLC反馈表格信息时间',
    submit_datetime   string comment '提交IDD时间',
    t1_respone_datetime string comment 'T1反馈时间',
    related_dn        string comment '数据补充Delivery',
    idd_status        string comment '完成状态',
    putaway_datetime  string comment '上架时间',
    remark            string comment '备注',
    receiving_plant   string comment '接收仓库'
) comment 'IDD Tracking Information'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_idd_tracking/'
tblproperties ("parquet.compression"="lzo");
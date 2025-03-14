-- Hive SQL
-- Function： IDD主数据 （ODS 层）
-- History: 
-- 2021-05-08    Donny   v1.0    draft

drop table if exists ods_idd_master;
create external table ods_idd_master
(
    idd_delivery      string comment 'IDD Delivery',
    material          string comment '产品',
    batch             string comment '批号',
    declare_status    string comment '申报状态',
    idd_date          string comment 'IDD发现日期',
    idd_type          string comment 'IDD种类',
    idd_quantity      string comment 'IDD数量',
    packing_list_date string comment 'BSC提供PackingList日期',
    slc_date          string comment 'SLC反馈表格信息时间',
    idd_submit_date   string comment '提交IDD时间',
    t1_date           string comment 'T1反馈时间',
    data_supplement_delivery string comment '数据补充Delivery',
    idd_status        string comment 'IDD完成状态',
    shelf_date        string comment '上架时间',
    remark            string comment '备注',
    receiving_plant   string comment '收货仓'
) comment 'IDD主数据'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_idd_master/';
-- Hive SQL
-- Function： ODS 进出口转仓表
-- History: 
--  2021-05-07    Donny   v1.0    draft
--  2021-05-08    Donny   v1.0    update the field inforamtion

drop table if exists ods_import_export_transaction;
create external table ods_import_export_transaction
(
    id                bigint comment 'ID',
    update_dt         string comment '更新时间',
    active            string comment 'Active',
    sto_no            string comment '进出口转仓单编号',
    sto_create_dt     string comment '创建日期',
    sto_update_dt     string comment '修改日期',
    sto_created_by    string comment '创建人',
    sto_updated_by    string comment '修改人',
    sto_status        string comment '转仓单状态',
    sto_type          string comment '转仓单种类',
    sto_order_reason  string comment '转仓单order reason',
    order_remarks     string comment '转仓单订单备注',
    ship_from_plant   string comment '发货plant',
    ship_to_plant     string comment '目的地plant代码',
    sto_line_no       string comment '转仓点行号',
    material          string comment '产品代码',
    qty               bigint comment '产品数量',
    unit              string comment '单位'
) comment '进出口转仓单'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' -- 指定分割符为\t 
STORED AS -- 指定存储方式，读数据采用 LzoTextInputFormat；输出数据采用 TextOutputFormat
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_import_export_transaction/' -- 指定数据在 hdfs 上的存储位置
;



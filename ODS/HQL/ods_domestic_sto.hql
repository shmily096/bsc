-- Hive SQL
-- Function： ODS 国内转仓订单
-- History: 
--  2021-05-08    Donny   v1.0    draft

drop table if exists ods_domestic_sto;
create external table ods_domestic_sto
(
    id                bigint comment 'ID',
    update_dt         string comment '更新时间',
    active            string comment 'Active',
    sto_no             string comment 'T2-T3转仓单编号',
    sto_ceate_dt      string comment '转仓单创建日期',
    sto_create_by     string comment '创建人',
    sto_update_dt     string comment '转仓单更新日期',
    sto_updated_by    string comment '更新人',
    sto_status        string comment '转仓单状态',
    remarks           string comment '转仓单备注',
    sto_type          string comment '转仓单类型',
    sto_reason        string comment '转仓单类型原因',
    ship_from_plant   string comment '发货仓代码',
    ship_to_plant     string comment '收货仓代码',
    stoline_no        string comment '转仓单行号',
    material          string comment '产品代码',
    qty               string comment '数量',
    unit              string comment '单位'
) comment '国内转仓订单'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_domestic_sto/';
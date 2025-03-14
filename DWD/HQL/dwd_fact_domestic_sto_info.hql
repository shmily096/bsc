-- Hive SQL
-- Function： 国内转仓订单事实表
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-17    Donny   v1.1    update the field name
-- 2021-05-24    Donny   v1.2    update the field
-- 2021-06-18    Donny   v1.3    add field:default location

drop table if exists dwd_fact_domestic_sto_info;
create external table dwd_fact_domestic_sto_info
(
       sto_no            string comment 'T2-T3转仓单编号',
       create_datetime   string comment '转仓单创建日期（时间）',
       created_by        string comment '创建人',
       update_datetime   string comment '转仓单更新日期（时间）',
       updated_by        string comment '更新人',
       ship_from_plant   string comment '发货仓代码',
       ship_to_plant     string comment '收货仓代码',
       order_status      string comment '转仓单状态',
       remarks           string comment '转仓单备注',
       order_type        string comment '转仓单类型',
       order_reason      string comment '转仓单类型原因',
       line_number       string comment '转仓单行号',
       material          string comment '产品代码',
       qty               bigint comment '数量',
       unit              string comment '单位',
       financial_dimension_id string comment 'BU维度',
       net_amount        decimal(16,2)  comment '金额',
       default_location  string comment '默认的Storage Location'
) comment '国内转仓订单'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_domestic_sto_info/'
tblproperties ("parquet.compression"="lzo");
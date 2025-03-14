-- Hive SQL
-- Function： 国内转仓发货单事实表
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-18    Donny   v1.1    update the name and the order
-- 2021-05-18    Donny   v1.2    add pgi date

drop table if exists dwd_fact_domestic_sto_dn_info;
create external table dwd_fact_domestic_sto_dn_info
(
    sto_no            string comment 'T2-T3转仓单编号',
    delivery_no       string comment '国内Outbound 发货单编号',
    reference_dn_number string comment '关联Inbound DN单',
    create_datetime   string comment '发货单创建日期',
    create_by         string comment '创建人',
    update_datetime   string comment '发货单更新日期',
    update_by         string comment '修改人',
    delivery_mode     string comment '发货模式',
    dn_status         string comment '发货单状态',
    ship_from_location string comment '始发Location',
    ship_from_plant   string comment '发货plant代码',
    ship_to_plant     string comment '收货plant代码',
    ship_to_location  string comment '收货仓位',
    carrier           string comment '承运人',
    actual_migo_date  string comment '实际收货日期',
    planned_good_issue_datetime string comment '计划发货时间',
    actua_good_issue_datetime string comment '实际发货时间',
    total_qty         bigint comment '合计数量',
    actual_putaway_datetime string comment '实际上架时间',
    pgi_datetime string comment 'post goods issue'
) comment '国内转仓发货单'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_domestic_sto_dn_info/'
tblproperties ("parquet.compression"="lzo");
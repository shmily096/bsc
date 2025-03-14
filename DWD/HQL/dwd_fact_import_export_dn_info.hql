-- Hive SQL
-- Function： 进出口发货单实事表
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-13    Donny   v1.1    update the name and fields
-- 2021-06-10    Donny   V1.2    add SKU type
    --alter table dwd_fact_import_export_dn_info add columns (item_business_group string);

drop table if exists dwd_fact_import_export_dn_info;
create external table dwd_fact_import_export_dn_info
(
    sto_no                  string COMMENT '进出口转仓单编码',
    delivery_no             string COMMENT 'Outbound发货单编码',
    reference_dn_no         string COMMENT '关联Inbound DN单',
    created_datetime        string COMMENT '发货单创建时间',
    updated_datetime        string COMMENT '发货单更新时间',
    created_by              string COMMENT '发货单创建人',
    updated_by              string COMMENT '发货单更新人',
    receiver_customer_code  string COMMENT '收货方客户代码',
    delivery_mode           string COMMENT '发运方式',
    order_status            string COMMENT '状态',
    ship_from_plant         string COMMENT '发货plant代码',
    ship_to_plant           string COMMENT '收货plant代码',
    total_qty               bigint COMMENT '发货数量',
    planned_good_issue_datetime string COMMENT '计划发货时间',
    actual_good_issue_datetime string COMMENT '实际发货时间',
    actual_migo_datetime    string COMMENT '实际收货时间',
    actual_putaway_datetime string COMMENT '实际上架时间',
    fin_dim_id              string COMMENT 'BU维度',
    item_business_group     string COMMENT '产品业务类别'
) COMMENT '进出口发货单'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_import_export_dn_info/'
tblproperties ("parquet.compression"="lzo");
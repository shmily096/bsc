 -- Hive SQL 
 -- Function： 进出口转仓单  
 -- History: 
 -- 2021-05-10 Rebecca v1.0 draft
 -- 2021-05-12 donny    v1.1    update the table name & type

drop table if exists dwd_fact_import_export_sto;
create external table dwd_fact_import_export_sto
(
    sto_id             string comment '进出口转仓编号',
    created_datetime   string comment '创建时间',
    updated_datetime   string comment '最新更新时间',
    created_by         string comment '创建人',
    updated_by         string comment '更新人',
    order_status       string comment '转仓单状态',
    order_type_id      string comment '转仓单类型',
    order_reason       string comment '转仓单类型原因',
    order_remarks      string comment '转仓单订单备注',
    ship_from_plant_id string comment '发货plant编码',
    ship_to_plant_id   string comment '目的plant编码',
    line_number        string comment '行号',
    material_code      string comment '产品代码',
    qty                string comment '产品数量',
    unit               string comment '单位',
    unit_price         string comment '单价 MVP2扩展字段',
    net_amount         string comment '金额 MVP2扩展字段'
) comment '进出口转仓单'
partitioned by(dt string)
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_import_export_sto/'
tblproperties ("parquet.compression"="lzo");
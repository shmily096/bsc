-- Hive SQL
-- Function： 国内SO单信息
---行数据表示：一个SO单状态
-- History: 
-- 2021-06-23    Donny   v1.0    init

drop table if exists dws_so_leve3_daily_trans;
create external table dws_so_leve3_daily_trans
(
     cust_type                              string comment 'Cust level 3' 
    ,so_no                                  string comment 'SO Number'
    ,dn_no                                  string comment 'DN'
    ,create_dt                              string comment 'SO 创建时间'
    ,dn_create_dt                           string comment 'SO DN 创建时间'
    ,pgi                                    string comment 'SO PGI 时间'
    ,customer_receive_dt                    string comment 'SO 客户签收时间'
    ,order_operation_leadtime               bigint comment 'SO 订单处理leadtime'
    ,dn_operation_leadtime                  bigint comment '发货处理Leadtime' 
    ,dn_ship_leadtime                       bigint comment '货物运输Leadtime'
) comment 'SO cust of type Daily Transation'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_so_leve3_daily_trans/'
tblproperties ("parquet.compression"="lzo");
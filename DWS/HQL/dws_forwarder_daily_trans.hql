-- Hive SQL
-- Function： Forwarder 维度统计已报关的订单的报关交易信息 （DWS Layer）
-- 按创建报关开始日期进行分区存储，每个分区存储的是当前报关的所有订单进报关易信息
-- 行数据表示forwarder对应一个DN对应的报关交易信息
-- History: 
-- 2021-06-09    Donny   v1.0    init
-- 2021-07-08    Amanda   v1.1   update fields

drop table if exists dws_forwarder_daily_trans;
create external table dws_forwarder_daily_trans
(
    pgi_date                        string
    ,forwarder                      string comment 'Forwarder' --货运代理商
    ,sto_no                         string
    ,delivery_no                    string comment 'DN' 
    ,import_into_inventory_date     string comment '实际到仓时间'
    ,import_actual_arrival_time     string comment 'Actul arrival time' -- 到港时间
    ,pick_up_leadtime               float comment '进境备案时间'
) comment 'Forwarder Transation Leadtime'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_forwarder_daily_trans/'
tblproperties ("parquet.compression"="lzo");

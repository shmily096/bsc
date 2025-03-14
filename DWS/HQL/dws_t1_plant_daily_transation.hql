-- Hive SQL
-- Function： T1Plant 维度统计已创建DN的订单的进口交易信息 （DWS Layer）
-- 按创建DN日期进行分区存储，每个分区存储的是当前已PGI的所有订单进口交易信息
-- 行数据表示T1Plant下一个DN对应的进口交易信息
-- History: 
-- 2021-06-09    Donny   v1.0    init
-- 2021-06-23    Donny   v1.1    Update the fields
-- 2021-06-23    Amanda   v1.2    Update the fields

drop table if exists dws_t1_plant_daily_transation;
create external table dws_t1_plant_daily_transation
(
    pgi_date                        string comment 'so_dn_pgi_date'
    ,ship_from_plant                string comment 'plant id' --T1 Plant的编码 
    ,sto_no                         string
    ,import_dn                      string
    ,import_actual_arrival_time     string comment 'Actul arrival time' -- 到港时间
    ,import_pgi                     string comment 'STO DN PGI' --T1 发货过账时间 PGI
    ,inter_trans_leadtime           float comment 'International Trans Leadtime' -- 到港时间 - PGI
) comment 'T1Plant Transation Leadtime'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_t1_plant_daily_transation/'
tblproperties ("parquet.compression"="lzo");
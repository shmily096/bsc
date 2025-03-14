-- hive sql
-- function�� plantά�ȷ�������leadtime
-- ��pgi�������½��з����洢,ÿ�������洢���ǵ�ǰ��pgi�����з�������leadtime
-- �����ݱ�ʾplantά�ȷ�������leadtime
-- history: 
-- 2021-06-27    amanda   v1.0    init

drop table if exists dws_plant_delivery_processing_daily_trans;
create external table dws_plant_delivery_processing_daily_trans
(
    pick_up_plant      string comment 'pick up plant'
    ,pgi_processing    float comment 'pgi process'
    ,actual_gi_date    string comment 'actual gi date'
    ,so_no        string
    ,material   string
    ,batch      string
) comment 'plantά�ȷ�������leadtime'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_plant_delivery_processing_daily_trans/'
tblproperties ("parquet.compression"="lzo");
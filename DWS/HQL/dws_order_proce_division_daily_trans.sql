-- Hive SQL
-- Function�� divisionά�ȿ���������leadtime
-- ��PGI���ڽ��з����洢��ÿ�������洢���ǵ�ǰ��PGI�����ж���������Ϣ
-- �����ݱ�ʾPGI������һ��batch,material��Ӧ�Ķ���������Ϣ
-- History: 
-- 2021-06-17    Donny   v1.0    init

drop table if exists dws_order_proce_division_daily_trans;
create external table dws_order_proce_division_daily_trans
(
    so_no                string comment 'SO Number'
    ,material            string comment 'SKU'
    ,batch               string comment 'Batch'
    ,division_id         string comment 'BU'
    ,so_dn_datetime      string comment 'so dn create' 
    ,actual_gi_date      string comment 'PGI'
    ,so_create_datetime  string comment 'so create'
    ,order_processing    float comment '��������leadtime' 
) comment '����������Ϣ'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_order_proce_division_daily_trans/'
tblproperties ("parquet.compression"="lzo");


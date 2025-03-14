-- hive sql
-- function��type of bussinessά�ȿ���������leadtime
-- ��pgi���ڽ��з����洢��ÿ�������洢���ǵ�ǰ��pgi�����ж���������Ϣ
-- �����ݱ�ʾpgi������һ��batch,material��Ӧ�Ķ���������Ϣ
-- history: 
-- 2021-06-17    donny   v1.0    init

drop table if exists dws_order_proce_tob_daily_trans;
create external table dws_order_proce_tob_daily_trans
(
    so_no                string comment 'SO NO'
    ,material            string comment 'SKU'
    ,batch               string comment 'Batch'
    ,item_type         string comment 'Item Type'
    ,so_dn_datetime      string comment 'so dn create' 
    ,actual_gi_date      string comment 'pgi'
    ,so_create_datetime  string comment 'so create'
    ,order_processing    float comment '��������leadtime' 
) comment '����������Ϣ'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_order_proce_tob_daily_trans/'
tblproperties ("parquet.compression"="lzo");
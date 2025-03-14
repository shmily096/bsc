-- hive sql
-- function�� ��Ʒcategoryά�ȿ���������leadtime
-- ��pgi���ڽ��з����洢,ÿ�������洢���ǵ�ǰ��pgi�����ж���������Ϣ
-- �����ݱ�ʾpgi������һ��batch,material��Ӧ�Ķ���������Ϣ
-- history: 
-- 2021-06-17    donny   v1.0    init

drop table if exists dws_order_proce_custlev3_daily_trans;
create external table dws_order_proce_custlev3_daily_trans
(
    so_no                string comment 'sales order number'
    ,material            string comment'sku'
    ,batch               string comment'batch'
    ,customer_level3     string comment 'customer level 3'
    ,so_dn_datetime      string comment 'so dn create' 
    ,actual_gi_date      string comment 'pgi'
    ,so_create_datetime  string comment 'so create'
    ,order_processing    float comment '��������leadtime' 
) comment '����������Ϣ'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_order_proce_custlev3_daily_trans/'
tblproperties ("parquet.compression"="lzo");
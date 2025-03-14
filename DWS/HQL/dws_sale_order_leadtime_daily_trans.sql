-- hive sql
-- function:batch,materialά�ȿ���������ʱ��leadtime
-- ��pgi���ڽ��з����洢��ÿ�������洢���ǵ�ǰ��pgi�����ж�������������������ʱ����Ϣ
-- �����ݱ�ʾbatch,material�¶�Ӧ�����ж�������������������ʱ����Ϣ
-- history:
-- 2021-06-27    amanda   v1.0    init

drop table if exists dws_sale_order_leadtime_daily_trans;
create external table dws_sale_order_leadtime_daily_trans
(    
    delivery_id                    string comment 'dn id'
    ,material                      string comment 'sku'
    ,batch                         string comment 'batch'
    ,item_type                     string comment 'Item Type'
    ,so_create_datetime            string comment 'so����ʱ��' 
    ,so_dn_datetime                string comment 'so dn������ʱ��' 
    ,actual_gi_date                string comment 'pgiʱ��' 
    ,receiving_confirmation_date   string comment '�ջ�ʱ��'  
    ,order_processing              float comment  '��������leadtime'
    ,pgi_processing                float comment  '��������leadtime'
    ,transport                     float comment  '����ʱ��leadtime'
    ,product_sale                  float comment  '��������leadtime'
) comment '��������ʱ��leadtime'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_sale_order_leadtime_daily_trans/'
tblproperties ("parquet.compression"="lzo");


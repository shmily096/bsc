-- 解决中文乱码问题，设置表的编码格式为GBK
--ALTER TABLE opsdw.ods_order_operation_type SET SERDEPROPERTIES ('serialization.encoding'='GBK'); 
--ALTER TABLE opsdw.dwd_dim_order_operation_type SET SERDEPROPERTIES ('serialization.encoding'='GBK'); 

--load data into ods from local file
load data local inpath '/user/code/bsc/ODS/LocalData/wo_dn_qr_d835.csv' overwrite into table opsdw.ods_order_operation_type;

--load data into dwd from ods 
insert overwrite table opsdw.dwd_dim_order_operation_type partition(dt='2021-06-21')
select * from opsdw.ods_order_operation_type;
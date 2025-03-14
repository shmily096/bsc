-- 解决中文乱码问题，设置表的编码格式为GBK
ALTER TABLE opsdw.ods_cust_business_type SET SERDEPROPERTIES ('serialization.encoding'='GBK'); 
ALTER TABLE opsdw.dwd_dim_cust_business_type SET SERDEPROPERTIES ('serialization.encoding'='GBK'); 

--load data into ods from local file
load data local inpath '/user/code/bsc/ODS/LocalData/cust_business_type.csv' overwrite into table opsdw.ods_cust_business_type;

--load data into dwd from ods 
insert overwrite table opsdw.dwd_dim_cust_business_type partition(dt='2021-06-21')
select * from opsdw.ods_cust_business_type;
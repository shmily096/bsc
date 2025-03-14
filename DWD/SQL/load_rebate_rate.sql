-- 解决中文乱码问题，设置表的编码格式为GBK
--ALTER TABLE opsdw.ods_division_rebate_rate SET SERDEPROPERTIES ('serialization.encoding'='GBK'); 
--ALTER TABLE opsdw.dwd_dim_order_operation_type SET SERDEPROPERTIES ('serialization.encoding'='GBK'); 

--load data into ods from local file
load data local inpath '/user/code/bsc/ODS/LocalData/rebate_rate_v3.txt' overwrite into table opsdw.ods_division_rebate_rate;

--load data into dwd from ods 
insert overwrite table opsdw.dwd_dim_division_rebate_rate partition(dt='2021-06-26')
select 
    id
    ,upper(division)
    ,cust_business_type
    ,sub_divison
    ,rate
    ,default_rate

from opsdw.ods_division_rebate_rate;


SELECT  id
       ,division
       ,cust_business_type
       ,sub_divison
       ,rate
       ,default_rate
       ,dt
FROM opsdw.dwd_dim_division_rebate_rate;
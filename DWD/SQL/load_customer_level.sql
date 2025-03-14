-- 解决中文乱码问题，设置表的编码格式为GBK
ALTER TABLE opsdw.ods_customer_level SET SERDEPROPERTIES ('serialization.encoding'='GBK'); 
ALTER TABLE opsdw.dwd_dim_customer_level SET SERDEPROPERTIES ('serialization.encoding'='GBK'); 


--load data into ods from local file
load data local inpath '/user/code/bsc/ODS/LocalData/customer_level.txt' overwrite into table opsdw.ods_customer_level;

--load data into dwd from ods 
insert overwrite table opsdw.dwd_dim_customer_level
select level1_code
       ,level1_english_name
       ,level1_chinese_name
       ,level2_code
       ,level2_english_name
       ,level2_chinese_name
       ,level3_code
       ,level3_english_name
       ,level3_chinese_name
       ,level4_code
       ,level4_chinese_name
       ,business_category
from opsdw.ods_customer_level;
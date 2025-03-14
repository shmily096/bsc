set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

--7 division
insert overwrite table opsdw.dwd_dim_division partition(dt='2021-06-21')
select  id
       ,division
       ,short_name
       ,cn_name
       ,display_name
       ,case display_name
             when 'PION' then 'product_line4_name'
             when 'IC' then 'product_line3_name'
             when 'URO' then 'product_line2_name'
             when 'PUL' then 'product_line2_name'
       else 'product_line1_name'
       end
from opsdw.ods_division_master
where dt='2021-06-21';
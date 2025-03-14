
-- Hive SQL
-- Function： 
---行数据表示：
-- History: 
-- 2021-06-25    Donny   v1.0    init

drop table if exists dwt_lifecycle_yh_heatmap;
create external table dwt_lifecycle_yh_heatmap
(    pgi_month    string, 
     pgi_year   string,
     product_line1   string, 
     cust_level3    string, 
     leadtime_section  string,
     leadtime   float    
) comment 'lifecycle瀑布图'
partitioned by(dt_year string,dt_month string)
stored as parquet
location '/bsc/opsdw/dwt/dwt_lifecycle_yh_heatmap/'
tblproperties ("parquet.compression"="lzo");
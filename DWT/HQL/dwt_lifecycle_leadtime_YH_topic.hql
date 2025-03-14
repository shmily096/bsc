-- Hive SQL
-- Function：YH汇总年月product line cust level 维度leadtime e2e lifecycle
-- 行数据表示：
-- History:
-- 2021-07-08   Amanda   v1.0   init

drop table if exists dwt_lifecycle_leadtime_YH_topic;
create external table dwt_lifecycle_leadtime_YH_topic
(
    pgi_month               string
    ,pgi_year               string
    ,product_line1          string
    ,product_line2          string
    ,product_line3          string
    ,product_line4          string
    ,product_line5          string
    ,cust_level1            string
    ,cust_level2            string
    ,cust_level3            string
    ,cust_level4            string
    ,inter_trans            float
    ,import_record_leadtime float
    ,T2_migo                float
    ,localization           float
    ,domestic_trans         float
    ,yh_product_putaway     float
    ,in_store_so_create     float
    ,so_order_processing    float
    ,so_pgi_processing      float
    ,so_delivery            float
    ,in_store               float
    ,E2E                    float
) comment 'lifecycle leadtime of yh'
partitioned by(dt_year string,dt_month string)
stored as parquet
location '/bsc/opsdw/dwt/dwt_lifecycle_leadtime_YH_topic/'
tblproperties ("parquet.compression"="lzo");
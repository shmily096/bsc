-- Hive SQL
-- Function：YH product line cust level 维度leadtime e2e lifecycle
-- 行数据表示：
-- History:
-- 2021-07-09    Amanda   v1.0    init

drop table if exists dws_lifecycle_leadtime_YH_daily_trans;
create external table  dws_lifecycle_leadtime_YH_daily_trans
(
    pgi_date                                string
    ,division                               string comment 'Division'
    ,product_line1                          string
    ,product_line2                          string
    ,product_line3                          string
    ,product_line4                          string
    ,product_line5                          string
    ,cust_level1                            string
    ,cust_level2                            string
    ,cust_level3                            string
    ,cust_level4                            string
    ,work_order_no                          string
    ,import_dn                          string
    ,so_customer_receive_dt                 string
    ,so_dn_create_dt                        string
    ,so_create_dt                           string
    ,domestic_putway                        string
    ,domestic_migo                          string
    ,wo_completed_dt                        string
    ,wo_created_dt                          string
    ,import_migo                            string
    ,import_declaration_completion_date     string
    ,import_actual_arrival_time             string
    ,import_pgi                             string
    ,inter_trans                            float
    ,import_record_leadtime                 float
    ,T2_migo                                float
    ,localization                           float
    ,domestic_trans                         float
    ,yh_product_putaway                     float
    ,in_store_so_create                     float
    ,so_order_processing                    float
    ,so_pgi_processing                      float
    ,so_delivery                            float
    ,in_store                               float
    ,E2E                                    float
) comment 'leadtime lifecycle YH'
partitioned by(dt string)
stored as parquet
location '/bsc/opsdw/dws/dws_lifecycle_leadtime_YH_daily_trans/'
tblproperties ("parquet.compression"="lzo");


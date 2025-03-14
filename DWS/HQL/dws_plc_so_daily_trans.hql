-- Hive SQL
-- Function： 产品的销售生命周期
---行数据表示：一个产品一个批号的生命周期
-- History: 
-- 2021-07-06    Donny   v1.0    init

drop table if exists dws_plc_so_daily_trans;
create external table dws_plc_so_daily_trans
(
     material                               string comment 'STO' 
    ,batch                                  string comment 'batch number'
    ,qr_code                                string comment 'QR code'
    ,import_dn                              string
    ,work_order_no                          string
    ,domestic_sto_dn                        string
    ,is_transfer_whs                        string comment 'Default Location:d838'
    ,division                               string comment 'Division'
    ,sub_division                           string comment 'Sub Division'
    ,product_line1                          string comment 'product line 1'
    ,product_line2                          string comment 'product line 2'
    ,product_line3                          string comment 'product line 3'
    ,product_line4                          string comment 'product line 4'
    ,product_line5                          string comment 'product line 5'
    ,item_type                              string comment 'Item type'
    ,so_customer_code                       string comment 'so customer code'
    ,cust_level1                            string comment 'cust level1'
    ,cust_level2                            string comment 'cust level2'
    ,cust_level3                            string comment 'cust level3'
    ,cust_level4                            string comment 'cust level4'
    ,so_create_dt                           string comment 'SO 创建时间'
    ,so_dn_create_dt                        string comment 'SO DN 创建时间'
    ,so_dn_pgi                              string comment 'SO PGI 时间'
    ,so_customer_receive_dt                 string comment 'SO 客户签收时间'
) comment 'Product Life Cycle Sales Order Daily Transation'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_plc_so_daily_trans/'
tblproperties ("parquet.compression"="lzo");
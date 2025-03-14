-- Hive SQL
-- Function： 批号主数据 （DWD 层）
-- History: 
-- 2021-05-21    Donny   v1.0    init
-- 2021-05-24    Donny   v1.1    add customer level fields

drop table if exists dwd_dim_customer;
create external table dwd_dim_customer
(
    cust_account      string,
    cust_name         string,
    cust_name2        string,
    city              string,
    post_code         string,
    rg                string,
    searchterm        string,
    street            string,
    telephone1        string,
    fax_number        string,
    tit               string,
    orblk             string,
    blb               string,
    cust_group        string,
    cl                string,
    dlv               string,
    del               string,
    cust_name3        string,
    cust_name4        string,
    distr             string,
    cust_b            string,
    transp_zone       string,
    country           string,
    delete_flag       string,
    tfn               string,
    level1_code       string,
    level1_english_name     string,
    level2_code             string,
    level2_english_name     string,
    level3_code             string,
    level3_english_name     string,
    level4_code             string,
    business_category       string comment 'Business Category'
) comment 'Customer master data'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_customer/'
tblproperties ("parquet.compression"="lzo");
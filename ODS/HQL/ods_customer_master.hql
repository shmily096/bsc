-- Hive SQL
-- Function： 客户主数据 （ODS 层）
-- History: 
-- 2021-05-20    Donny   v1.0    draft

drop table if exists ods_customer_master;
create external table ods_customer_master
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
    tfn               string
) comment '客户主数据'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_customer_master/';

-- History: 
-- 2021-08-12    Donny   v1.0    add fields

alter table ods_customer_master 
add columns (
telebox_nr string comment "TeleboxNumber"
,payment_block string comment "PaymentBlock"
,master_record string comment "MasterRecord"
,type_of_business string comment "TypeofBusiness"
,created_by string comment "CreatedBy"
,create_dt string comment "CreateDT"
,customer_sales string comment "CustomerSales"
)
;
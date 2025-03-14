-- Hive SQL
-- Function： 经销商采购报价（ODS 层）
-- History: 
-- 2021-06-07    Donny   v1.0    draft

drop table if exists ods_dealer_purchase_quotation;
create external table ods_dealer_purchase_quotation
(
    update_dt         string,
    fin_year          string,
    division          string,
    sub_buname        string,
    sapi_d            string,
    dealer_name       string,
    parent_sapid      string,
    parent_dealer_name string,
    dealer_type       string,
    rsm               string,
    zsm               string,
    tsm               string,
    contract_start_date string,
    contract_end_date string,
    market_type       string,
    contract_status   string,
    new_old_dealer_by_bu string,
    new_old_dealer_by_bsc string,
    aop_type          string,
    month1_amount     decimal(18,4),
    month2_amount     decimal(18,4),
    month3_amount     decimal(18,4),
    q1_amount         decimal(18,4),
    month4_amount     decimal(18,4),
    month5_amount     decimal(18,4),
    month6_amount     decimal(18,4),
    q2_amount         decimal(18,4),
    month7_amount     decimal(18,4),
    month8_amount     decimal(18,4),
    month9_amount     decimal(18,4),
    q3_amount         decimal(18,4),
    month10_amount    decimal(18,4),
    month11_amount    decimal(18,4),
    month12_amount    decimal(18,4),
    q4_amount         decimal(18,4),
    year_total_amount decimal(18,4),
    bi_code           string,
    bi_name           string
) comment 'The dealer purchase quotation'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_dealer_purchase_quotation/';
-- Hive SQL
-- Function： 销售订单的发票信息
-- History: 
--  2021-05-21    Donny   v1.0    init

drop table if exists ods_so_invoice;
create external table ods_so_invoice
(
       bill_id           string,
       item_no           bigint,
       accounting_no     string,
       bill_date         string,
       bill_type         string,
       sales_id          string,
       order_reason      string,
       item_category     string,
       purchase_order    string,
       material          string,
       profit_center     string,
       batch             string,
       bill_qty          bigint,
       net_amount        decimal(18,2),
       currency          string,
       payer             string,
       sold_to_pt        string,
       customer_name     string,
       classification    string,
       city              string,
       sales_rep_id      string,
       name3             string,
       desc1             string,
       desc2             string,
       sale              string,
       manufactory_date  string,
       expired_date      string,
       sales_line        bigint,
       delivery          string,
       ship_to           string,
       stock_location_pt string,
       stock_location_nm string,
       tax_amount        decimal(18,2),
       tax_rate          decimal(18,2),
       sales_type        string
) comment 'SO Invoice'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_so_invoice/';
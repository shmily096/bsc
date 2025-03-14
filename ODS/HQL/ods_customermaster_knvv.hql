drop table if exists ods_customermaster_knvv ;
create external table ods_customermaster_knvv
(
    cust_account     string comment 'CustomerNumber',
    currency         string comment 'Currency',
    delivering_plant string comment 'DeliveringPlant',
    shipping_conditions  string comment 'ShippingConditions',
    sales_organization   string comment 'SalesOrganization',
    delivery_priority    string comment 'DeliveryPriority',
    order_combination_indicator   string comment 'OrderCombinationIndicator',
    incoterms_part1  string comment 'IncotermsPart1',
    incoterms_part2   string comment 'IncotermsPart2'

) comment 'KNVI'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_customermaster_knvv/';
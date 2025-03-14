-- Hive SQL
-- Function： WMS 入库上架信息
-- History: 
--  2021-05-21    Donny   v1.0    init

drop table if exists ods_putaway_info;
create external table ods_putaway_info
(
    update_dt         string,
    invoice           string,
    delivery_no       string,
    putaway_date      string,
    upn               string,
    qty               bigint,
    batch             string,
    plant             string,
    sl                string,
    unit              string,
    from_slocation    string,
    to_location       string
) comment 'Inventory transactions'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_putaway_info/';
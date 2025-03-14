drop table if exists ods_demo;
create external table ods_demo
(
    ID                string comment 'ID',
    name              string comment 'name',
    age               int comment 'Age'
) comment 'DEMO '
partitioned by (dt string)
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_demo/';
-- Hive SQL
-- Function： 日历主数据 （ODS 层）
-- History: 
-- 2021-05-08    Donny   v1.0    draft

drop table if exists ods_calendar_master;
create external table ods_calendar_master
(
    cal_date          string comment '日',
    cal_month         string comment '月',
    cal_year          string comment '年',
    cal_quarter       string comment '季度',
    weeknum_m1        string,
    weeknum_y1        string,
    weeknum_m2        string,
    weeknum_y2        string,
    weekday           string,
    workday           string,
    workday_flag      string comment '是否工作日',
    po_pattern        string,
    year_month_date   string comment '年月日',
    month_weeknum     string,
    day_of_week       string
) comment '日历主数据'
--partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_calendar_master/';

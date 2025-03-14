-- Hive SQL
-- Function： 时间维度 (DWD)
-- History: 
-- 2021-05-12    Donny   v1.0    init

drop table if exists dwd_dim_calendar;
create external table dwd_dim_calendar
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
) comment 'calendar dimension'
--partitioned by (dt string)
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_calendar/'
tblproperties ("parquet.compression"="lzo");
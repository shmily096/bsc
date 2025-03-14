

-- Hive SQL
-- Function： CFDA数据 （ODS 层）
-- History: 
-- 2021-11-17    Amanda   v1.0    draft

drop table if exists ods_add_on_duty_list;
create external table ods_add_on_duty_list
(
    hs_code             string
    ,COO                 string
    ,add_on_rate         decimal(9,2)
) comment '申请加征关税清单'
partitioned by(dt string)
row format delimited fields terminated by ',' 
location '/bsc/opsdw/ods/ods_add_on_duty_list/'
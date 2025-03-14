-- Hive SQL
-- Function： CFDA数据 （ODS 层）
-- History: 
-- 2021-11-17    Amanda   v1.0    draft

drop table if exists ods_saving_duty_list;
create external table ods_saving_duty_list
(
    hs_code                  string
    ,COO                      string
    ,saving_duty_rate         decimal(9,2)
) comment '申请加征关税清单'
partitioned by(dt string)
row format delimited fields terminated by ',' 
location '/bsc/opsdw/ods/ods_saving_duty_list/'
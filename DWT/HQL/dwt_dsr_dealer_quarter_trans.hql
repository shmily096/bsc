-- Hive SQL
-- Function：by季度经销商完成额及指标
-- 行数据表示：一个季度下的经销商完成额及对应的指标
-- History:
-- 2021-06-29    Winter   v1.0    init

drop table if exists dwt_dsr_dealer_quarter_trans;
create external table dwt_dsr_dealer_quarter_trans
(
     division                       string comment 'Division'
    ,sub_division                   string comment 'sub_bu'
    ,cust_level3                    string comment 'cust_level3'
    ,bill_year                      string comment '年'
    ,`quarter`                      string comment '季度'
    ,dealer_name                    string comment '经销商名称'
    ,dealer_quar_complete                decimal(16,2) comment '经销商已完成'
    ,dealer_quarter_target              decimal(16,2) comment '经销商指标'
) comment 'DSR Dearler Quarter Transation'
partitioned by(dt_year string,dt_quarter string)
stored as parquet
location '/bsc/opsdw/dwt/dwt_dsr_dealer_quarter_trans/'
tblproperties ("parquet.compression"="lzo");
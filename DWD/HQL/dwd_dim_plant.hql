-- Hive SQL
-- Function： Plant维度表
-- History: 
-- 2021-05-07    Donny   v1.0    draft

drop table if exists dwd_dim_plant;
create external table dwd_dim_plant (
    id                string COMMENT 'Plant 编号',
    name              string COMMENT '名称',
    second_name       string COMMENT '第二个名称',
    postcode          string COMMENT '邮政编码',
    city              string COMMENT '城市',
    search_term1      string COMMENT '检索词1',
    search_term2      string COMMENT '检索词2'
) COMMENT 'Plant维度表'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_plant/'
tblproperties ("parquet.compression"="lzo");
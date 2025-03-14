

-- opsdw.ods_buffer_hold_list definition

drop table if exists `opsdw`.`dwd_dim_buffer_hold_list`;
create external table `opsdw`.`dwd_dim_buffer_hold_list` (
  `division` STRING,
  `upn` STRING,
  `description` STRING,
  `vaild_from` STRING,
  `vaild_to` STRING,
  `buffer_flag` STRING,
    `dt` STRING)
USING parquet
PARTITIONED BY (dt)
LOCATION '/bsc/opsdw/dwd/dwd_dim_buffer_hold_list';
  
  
  
  
  
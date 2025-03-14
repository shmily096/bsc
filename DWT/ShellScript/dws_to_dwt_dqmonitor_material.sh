#!/bin/bash
# Function:
#   sync up dwt_dqmonitor_opsmaterial data to dwt layer
# History:
# 2021-11-05    Donny   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# 默认取当前时间的前一天 
if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date  +%F)
fi


echo "start syncing data into dws layer from: ${start_date} to: ${sync_date} .................."

dwt_sql="
use ${target_db_name};
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.reducers.max=8; 
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;
set hive.exec.dynamic.partition.mode=nonstrict;


insert overwrite table dwt_dqmonitor_material partition(dt)
select mat.mid
     , mat.material_code
     , mat.material_type
     , mat.delivery_plant
     , mat.default_location
     , mat.division_id
     , mat.profit_center
     , mat.standard_cost
     , mat.sap_upl_level4_code
     , mat.sap_upl_level4_name
     , mat.sap_upl_level5_code
     , mat.sap_upl_level5_name
     , mat.source_type
     , mat.chinese_name
     , mat.english_name
     , mat.legal_entity
     , divi.display_name as division_name
     , mat.dt
from (
    select material_code as mid
         , material_code
         , material_type
         , delivery_plant
         , default_location
         , division_id
         , profit_center
         , standard_cost
         , sap_upl_level4_code
         , sap_upl_level4_name
         , sap_upl_level5_code
         , sap_upl_level5_name
         , 'UPN' as source_type
         , chinese_name
         , english_name
         , ' ' as legal_entity
         , dt
      from dwd_dim_material
     where dt = '${sync_date}'
       and material_code is not null
       and trim(material_code) <> ''
      union all 
      select old_code as mid
           , material_code
           , material_type
           , delivery_plant
           , default_location
           , division_id
           , profit_center
           , standard_cost
           , sap_upl_level4_code
           , sap_upl_level4_name
           , sap_upl_level5_code
           , sap_upl_level5_name
           , 'old_code' as source_type
           , chinese_name
           , english_name
           , ' ' as legal_entity
           , dt
        from dwd_dim_material
       where dt = '${sync_date}'
         and old_code is not null
         and trim(old_code) <> ''
      union all 
       select sap_upl_level5_code as mid
            , material_code
            , material_type
            , delivery_plant
            , default_location
            , division_id
            , profit_center
            , standard_cost
            , sap_upl_level4_code
            , sap_upl_level4_name
            , sap_upl_level5_code
            , sap_upl_level5_name
            , 'sap_upl_level5_code' as source_type
            , chinese_name
            , english_name
            , ' ' as legal_entity
            , dt
         from dwd_dim_material
        where dt = '${sync_date}'
          and sap_upl_level5_code is not null
          and trim(sap_upl_level5_code) <> '' ) mat
left outer join (
              select id 
                   , display_name
                from dwd_dim_division
               where dt = '${sync_date}'  ) divi on mat.division_id = divi.id
; 
"
# 2. 执行加载数据SQL
$hive -e "$dwt_sql"

echo "End syncing dwt_dqmonitor_opsmaterial data into DWT layer on $sync_date ....."

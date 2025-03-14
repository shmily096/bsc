
#!/bin/bash
#   sync up sales order data from ods to dwd layer
# History:
# 2021-11-16    Amanda   v1.0    init
#文件路径

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

if [ -n "$2" ] ;then 
    sync_year=$2
else
    sync_year=$(date  +'%Y')
fi

echo "start syncing so into DWD layer on ${sync_date} .................."

# 1 Hive SQL string
so_sql="
use ${target_db_name};
-- 参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;

-- sync up SQL string
insert overwrite table ${target_db_name}.dwd_ctm_customer_master partition(dt='$sync_date')
select  material
       ,product_type
       ,chinese_name
       ,english_name
       ,hs_code
       ,hs_additional_code
       ,enterprise_unit
       ,delcare_unit
       ,declaration_scale_factor
       ,'CNY' as currency 
       ,unit_price * fx_rate_cny as unit_price
       ,origin_country
       ,first_legal_unit
       ,first_scale_factor
       ,second_legal_unit
       ,second_scale_factor
       ,start_expiry_date
       ,bonded_property
       ,materials_flag
       ,special_remark
       ,quantity
       ,netweight
       ,grossweight
       ,length
       ,width
       ,height
       ,distribution_properties
       ,import_documents_requirements
       ,export_documents_requirements
       ,end_expiry_date
       ,supervision_certificate
       ,inspection_requirements
       ,MFN_tax_rate
       ,provisional_tax_rate
       ,creation_date
       ,last_modify_date
from( select * from ${target_db_name}.ods_ctm_customer_master where dt='$sync_date' and creation_date is not null )ctm  --MDM_CTMCustomerMaster 全量
left join (
     select to_usd.from_currency
          , usd_cny.to_currency
          , to_usd.fx_rate
          , usd_cny.fx_2rate
          , round(to_usd.fx_rate*usd_cny.fx_2rate,3) as fx_rate_cny
      from (
             select cast(rate as decimal(9,2))/cast(ratio_from as decimal(5,1))*cast(ratio_to as decimal(5,1)) as fx_rate
                  , from_currency
                  , to_currency 
              from opsdw.dwd_dim_exchange_rate
             where to_currency = 'USD'
               and dt in (select max(dt) from opsdw.dwd_dim_exchange_rate) 
               and year(valid_from) = year(CURRENT_DATE()) ) to_usd 
      left join (
                select cast(rate as decimal(9,2))/cast(ratio_from as decimal(5,1))*cast(ratio_to as decimal(5,1)) as fx_2rate
                     , from_currency
                     , to_currency 
                from opsdw.dwd_dim_exchange_rate
               where from_currency = 'USD'
                 and to_currency = 'CNY'
                 and dt in (select max(dt) from opsdw.dwd_dim_exchange_rate) 
                 and year(valid_from) = year(CURRENT_DATE()) ) usd_cny on to_usd.to_currency = usd_cny.from_currency
  ) fx  on ctm.currency = fx.from_currency
"
# 2. 执行SQL
$hive -e "$so_sql"

echo "End syncing Sales order data into DWD layer on ${sync_date} .................."

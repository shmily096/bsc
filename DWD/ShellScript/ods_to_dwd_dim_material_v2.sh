#!/bin/bash
# Function:
#   sync up dwd_dim_material data to dwd layer
# History:
# 2021-05-18    Donny   v1.0    init
export LANG='zh_CN.UTF-8'
# export LC_ALL=zh_CN.GB2312;
# export LANG=zh_CN.GBK
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

echo "start syncing dwd_dim_material data into DWD layer on ${sync_date} .................."

# 1 Hive SQL string
sku_sql="
use ${target_db_name};
-- 参数
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.reducers.max=8; 
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;

-- material master data
insert overwrite table ${target_db_name}.dwd_dim_material partition(dt='$sync_date')
select distinct chinese_name 
       ,material 
       ,old_code 
       ,english_name 
       ,profit_center 
       ,gtin 
       ,sap_upl_level1_code 
       ,sap_upl_level1_name 
       ,sap_upl_level2_code 
       ,sap_upl_level2_name 
       ,sap_upl_level3_code 
       ,sap_upl_level3_name 
       ,sap_upl_level4_code 
       ,sap_upl_level4_name 
       ,sap_upl_level5_code 
       ,sap_upl_level5_name 
       ,special_procurement 
       ,latest_cfda 
       ,sheets 
       ,mmpp 
       ,d_chain 
       ,spk 
       ,default_location 
       ,source_list_indicator 
       ,pdt 
       ,grt 
       ,qi_flag 
       ,loading_group 
       ,legal_status 
       ,delivery_plant 
       ,shelf_life_sap 
       ,standard_cost 
       ,transfer_price 
       ,pra 
       ,material_type 
       ,pra_valid_from 
       ,pra_valid_to 
       ,pra_status 
       ,pra_release_status_code 
       ,standard_cost_usd 
       ,abc_class 
       ,sku.division 
       ,divi.display_name
       ,product_line1_code 
       ,product_line1_name 
       ,product_line2_code 
       ,product_line2_name 
       ,product_line3_code 
       ,product_line3_name 
       ,product_line4_code 
       ,product_line4_name 
       ,product_line5_code 
       ,product_line5_name 
       ,m_shelf_life_for_oboselete 
       ,xyz 
       ,m_isv_bp
       ,case when sku.material like 'SRV%' then '\\u670d\\u52a1'
            when sku.material like 'BM%' then '\\u865a\\u62df\\u7269\\u6599'
            when instr(trim(sku.material), ' ') > 0 then '\\u865a\\u62df\\u7269\\u6599'
            when length(sku.material) = 11 and instr(trim(sku.material), 'P')=1 and instr(trim(sku.material), '-')=8 then '\\u865a\\u62df\\u7269\\u6599'
        else 
            case when sku.product_line1_name like '%\\u8bbe\\u5907%' then 
                case when sku.product_line2_name like '%\\u8bbe\\u5907\\u5149\\u7ea4%' then '\\u8017\\u6750'
                    when sku.product_line2_name like '%\\u8bbe\\u5907\\u6613\\u635f\\u914d\\u4ef6%' then '\\u5907\\u4ef6'
                    when sku.product_line2_name like '%\\u58c1\\u6302\\u7cfb\\u7edf\\u0026\\u4fdd\\u4fee\\u5408\\u540c%' then '\\u5907\\u4ef6'
                    when sku.product_line2_name like '%\\u8bbe\\u5907\\u4fdd\\u4fee\\u670d\\u52a1%' then '\\u670d\\u52a1'
                    when sku.product_line2_name like '%\\u7ef4\\u4fdd%' then '\\u670d\\u52a1'
                else '\\u8bbe\\u5907'
                end
            else 
                case when sku.product_line2_name like '%Equipment%' then '\\u8bbe\\u5907'
                    when sku.product_line2_name like '%\\u8bbe\\u5907%' then '\\u8bbe\\u5907'
                else
                    case when sku.product_line2_name like '%spare parts%' then '\\u5907\\u4ef6'
                    else '\\u8017\\u6750'
                    end
                end
            end
        end
        ,case divi.display_name
             when 'PION' then sku.product_line4_name    --'product_line4_name'
             when 'IC' then sku.product_line3_name      --'product_line3_name'
             when 'URO' then sku.product_line2_name     --'product_line2_name'
             when 'PUL' then sku.product_line2_name     --'product_line2_name'
       else sku.product_line1_name  --'product_line1_name'
       end  
from ${target_db_name}.ods_material_master sku  --全量更新MDM_MaterialMaster
left outer join ${target_db_name}.ods_division_master divi on sku.division=divi.id   
    where sku.dt in (select max(dt) from ods_material_master); 
"

echo "$sku_sql"
# 2. 执行加载数据SQL
$hive -e "$sku_sql"

echo "End syncing dwd_dim_material data into DWD layer on ${sync_date} .................."
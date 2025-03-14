#!/bin/bash
# Function:
#   sync up dwd_dim_material data to dwd layer
# History:
# 2021-05-18    Donny   v1.0    init
export LANG="en_US.UTF-8"
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
# 获取当前月份  
current_month=$(date +%m)  
current_year=$(date +%Y)  
current_date=$(date +%Y-%m-%d)  
#取上个月。
previous_month=$(date -d "$current_date - 1 month" +%Y-%m)  

echo "start syncing dwd_dim_material data into DWD layer on ${sync_date} .................."

# 1 Hive SQL string
sku_sql="
use ${target_db_name};
-- 参数
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.reducers.max=8; 
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;

with material_master as (
    select *
    from ${target_db_name}.ods_material_master  --全量更新MDM_MaterialMaster
    where dt ='$sync_date'
    ) ,
division_master as (select id,display_name
                from opsdw.dwd_dim_division  --全量更新MDM_DivisionMaster
               where dt ='$sync_date'
    ),
change_upn as (
    select upn,correctedbu from opsdw.dwd_upn_change_bu  --全量更新pg的upn_recategorized
    where dt in (select max(dt) from opsdw.dwd_upn_change_bu)
    and startdate<='$sync_date'
    and enddate>='$sync_date'
)

-- material master data
insert overwrite table ${target_db_name}.dwd_dim_material partition(dt='$sync_date')
select chinese_name 
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
       ,coalesce(change_upn.correctedbu ,divi.display_name) as display_name
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
       ,product_model
       ,case when sku.material like 'SRV%' then 'Service'
            when sku.material like 'BM%' then 'VirtualMaterial'
            when instr(trim(sku.material), ' ') > 0 then 'VirtualMaterial'
            when length(sku.material) = 11 and instr(trim(sku.material), 'P')=1 and instr(trim(sku.material), '-')=8 then 'VirtualMaterial'
        else 
            case when sku.product_line1_name like '%设备%' then 
                case when sku.product_line2_name like '%设备光纤%' then 'Consumables'
                    when sku.product_line2_name like '%设备易损配件%' then 'SpareParts'
                    when sku.product_line2_name like '%壁挂系统&保修合同%' then 'SpareParts'
                    when sku.product_line2_name like '%设备保修服务%' then 'Service'
                    when sku.product_line2_name like '%维保%' then 'Service'
                else 'Equipment'
                end
            else 
                case when sku.product_line2_name like '%Equipment%' then 'Equipment'
                    when sku.product_line2_name like '%设备%' then 'Equipment'
                else
                    case when sku.product_line2_name like '%spare parts%' then 'SpareParts'
                    else 'Consumables'
                    end
                end
            end
        end
        ,TRIM(sku.subbu_name)AS sub_division
        ,split(
           concat_ws(',', trim(sku.material), trim(sku.sap_upl_level5_code), trim(sku.sap_upl_level4_code), trim(sku.old_code)), ',')
        ,sku.dlr_quota_product_line_id
        ,sku.dlr_quota_product_line
        ,sku.dlr_auth_productline_id
        ,sku.dlr_auth_productline
        ,sku.sales_status
        ,sku.is_active_for_sale
        ,sku.subbu_code
        ,sku.subbu_name
        ,sku.lp_subbu_code
        ,sku.lp_subbu_name
        ,case divi.display_name
             when 'PION' then sku.product_line3_name    --'product_line4_name'-->product_line3_name
             when 'PI' then sku.product_line3_name    --'product_line4_name'-->product_line3_name
             when 'IC' then sku.product_line2_name      --'product_line3_name'-->product_line2_name
             when 'URO' then sku.product_line1_name     --'product_line1_name'
             when 'PUL' then sku.product_line3_name     --'product_line2_name'-->product_line3_name
             when 'CRM' then sku.product_line2_name     --'product_line2_name'
             when 'IO' then sku.product_line4_name      --'product_line4_name'
             when 'ENDO' then sku.product_line3_name      --'product_line3_name'
         else sku.product_line1_name  --'product_line1_name'
       end  -- sub_division_bak  
from material_master sku    --ods_material_master
left outer join division_master divi on sku.division=divi.id    --ods_division_master
left outer join change_upn on sku.material=change_upn.upn --dwd_upn_change_bu
    
"
dws_sql="
use ${target_db_name};
-- 参数
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.reducers.max=8; 
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;

insert overwrite table ${target_db_name}.dws_dim_material_by_mon partition(months='$previous_month')
SELECT chinese_name, material_code, old_code, english_name, profit_center, gtin, sap_upl_level1_code, sap_upl_level1_name, sap_upl_level2_code, sap_upl_level2_name, sap_upl_level3_code, sap_upl_level3_name, sap_upl_level4_code, sap_upl_level4_name, sap_upl_level5_code, sap_upl_level5_name, special_procurement, latest_cfda, sheets, mmpp, d_chain, spk, default_location, source_list_indicator, pdt, grt, qi_flag, loading_group, legal_status, delivery_plant, shelf_life_sap, standard_cost, transfer_price, pra, material_type, pra_valid_from, pra_valid_to, pra_status, pra_release_status_code, standard_cost_usd, abc_class, division_id, division_display_name, product_line1_code, product_line1_name, product_line2_code, product_line2_name, product_line3_code, product_line3_name, product_line4_code, product_line4_name, product_line5_code, product_line5_name, m_shelf_life_for_oboselete, xyz, m_isv_bp, product_model, business_group, sub_division, mit, dlr_quota_product_line_id, dlr_quota_product_line, dlr_auth_productline_id, dlr_auth_productline, sales_status, is_active_for_sale, subbu_code, subbu_name, lp_subbu_code, lp_subbu_name, sub_division_bak,dt
FROM opsdw.dwd_dim_material where dt='$sync_date';
"
# 2. 执行加载数据SQL
$hive -e "$sku_sql"
# 3. 执行SQL，并判断查询结果是否为空
count=`$hive -e "select count(*) from dwd_dim_material where dt='$sync_date'" | tail -n1`

if [ $count -eq 0 ]; then
  echo "Error: Failed to import data, count is zero."
  exit 1
fi
echo "End syncing dwd_dim_material data into DWD layer on ${sync_date} .................."
#大概是太占内存所以执行完成之后会把ods的一周前的一周的数据删除
sh /bscflow/ods/remove_a_week_ago_ods_material.sh
# 判断是否为当月1号  
if [ "$sync_date" == "$current_year-$current_month-01" ]; then  
    # 执行脚本  
    echo "Today is the 1st of the month. Executing script..."  
    $hive -e "$dws_sql"
else  
    echo "Today is not the 1st of the month. Not executing script."  
fi
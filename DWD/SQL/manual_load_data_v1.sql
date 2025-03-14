--1. Customer Data
insert overwrite table opsdw.dwd_dim_customer partition(dt='2021-06-21')
select  cust.cust_account
       ,cust_name
       ,cust_name2
       ,city
       ,post_code
       ,rg
       ,searchterm
       ,street
       ,telephone1
       ,fax_number
       ,tit
       ,orblk
       ,blb
       ,cust_group
       ,cl
       ,dlv
       ,del
       ,cust_name3
       ,cust_name4
       ,distr
       ,cust_b
       ,transp_zone
       ,country
       ,delete_flag
       ,tfn
        ,cust_level.level1_code -- level1
       ,cust_level.level1_english_name -- level1_name
       ,cust_level.level2_code -- level2
       ,cust_level.level2_english_name  -- level2 name
       ,cust_level.level3_code  -- level3
       ,cust_level.level3_english_name  -- level3 name
       ,cust_level.level4_code  -- level4
       ,cust_level.business_category -- Category
from 
(
    select * from opsdw.ods_customer_master
       where dt='2021-06-21'
           and country='CN'
) cust
left outer join 
(
       select  c_type.cust_account 
              ,c_level.level1_code 
              ,c_level.level1_english_name
              ,c_level.level2_code 
              ,c_level.level2_english_name 
              ,c_level.level2_chinese_name 
              ,c_level.level3_code 
              ,c_level.level3_english_name 
              ,c_level.level3_chinese_name 
              ,c_level.level4_code 
              ,c_level.level4_chinese_name 
              ,c_level.business_category
       from opsdw.ods_cust_business_type c_type
       join opsdw.ods_customer_level c_level
       on c_type.business_type=c_level.level4_code
) cust_level
on cust.cust_account=cust_level.cust_account;

-- Material Data
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${target_db_name}.dwd_dim_material partition(dt='2021-06-23')
select  chinese_name 
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
       ,case when sku.material like 'SRV%' then '服务'
            when sku.material like 'BM%' then '虚拟物料'
            when instr(trim(sku.material), ' ') > 0 then '虚拟物料'
            when length(sku.material) = 11 and instr(trim(sku.material), 'P')=1 and instr(trim(sku.material), '-')=8 then '虚拟物料'
        else 
            case when sku.product_line1_name like '%设备%' then 
                case when sku.product_line2_name like '%设备光纤%' then '耗材'
                    when sku.product_line2_name like '%设备易损配件%' then '备件'
                    when sku.product_line2_name like '%壁挂系统&保修合同%' then '备件'
                    when sku.product_line2_name like '%设备保修服务%' then '服务'
                    when sku.product_line2_name like '%维保%' then '服务'
                else '设备'
                end
            else 
                case when sku.product_line2_name like '%Equipment%' then '设备'
                    when sku.product_line2_name like '%设备%' then '设备'
                else
                    case when sku.product_line2_name like '%spare parts%' then '备件'
                    else '耗材'
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
from ${target_db_name}.ods_material_master sku
left outer join ${target_db_name}.ods_division_master divi on sku.division=divi.id
    where sku.dt='2021-06-21' ; 
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-- material master data
insert overwrite table opsdw.dwd_dim_material partition(dt='2021-05-26')
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
       ,case when sku.material like 'SRV%' then '����'
            when sku.material like 'BM%' then '��������'
            when instr(trim(sku.material), ' ') > 0 then '��������'
            when length(sku.material) = 11 and instr(trim(sku.material), 'P')=1 and instr(trim(sku.material), '-')=8 then '��������'
        else 
            case when sku.product_line1_name like '%�豸%' then 
                case when sku.product_line2_name like '%�豸����%' then '�Ĳ�'
                    when sku.product_line2_name like '%�豸�������%' then '����'
                    when sku.product_line2_name like '%�ڹ�ϵͳ&���޺�ͬ%' then '����'
                    when sku.product_line2_name like '%�豸���޷���%' then '����'
                    when sku.product_line2_name like '%ά��%' then '����'
                else '�豸'
                end
            else 
                case when sku.product_line2_name like '%Equipment%' then '�豸'
                    when sku.product_line2_name like '%�豸%' then '�豸'
                else
                    case when sku.product_line2_name like '%spare parts%' then '����'
                    else '�Ĳ�'
                    end
                end
            end
        end  
from opsdw.ods_material_master sku
left outer join opsdw.ods_division_master divi on sku.division=divi.id
    where sku.dt='2021-05-26' ; 
-- Hive SQL
-- Function： 物料主数据 （ODS 层）
-- History: 
-- 2021-05-08    Donny   v1.0    draft

drop table if exists ods_material_master;
create external table ods_material_master
(
    chinese_name      string,
    material          string,
    old_code          string,
    english_name      string,
    profit_center     string,
    gtin              string,
    sap_upl_level1_code string,
    sap_upl_level1_name string,
    sap_upl_level2_code string,
    sap_upl_level2_name string,
    sap_upl_level3_code string,
    sap_upl_level3_name string,
    sap_upl_level4_code string,
    sap_upl_level4_name string,
    sap_upl_level5_code string,
    sap_upl_level5_name string,
    special_procurement string,
    latest_cfda       string,
    sheets            string,
    mmpp              string,
    d_chain           string,
    spk               string,
    default_location  string,
    source_list_indicator string,
    pdt               string,
    grt               string,
    qi_flag           string,
    loading_group     string,
    legal_status      string,
    delivery_plant    string,
    shelf_life_sap    string,
    standard_cost     string,
    transfer_price    string,
    pra               string,
    material_type     string,
    pra_valid_from    string,
    pra_valid_to      string,
    pra_status        string,
    pra_release_status_code string,
    standard_cost_usd string,
    abc_class         string,
    division          string,
    product_line1_code string,
    product_line1_name string,
    product_line2_code string,
    product_line2_name string,
    product_line3_code string,
    product_line3_name string,
    product_line4_code string,
    product_line4_name string,
    product_line5_code string,
    product_line5_name string,
    m_shelf_life_for_oboselete string,
    xyz               string,
    m_isv_bp          string,
    product_model     string,
    dlr_quota_product_line_id int,
    dlr_quota_product_line string,
    dlr_auth_productline_id int,
    dlr_auth_productline string,
    sales_status string,
    is_active_for_sale int,
    subbu_code string,
    subbu_name string,
    lp_subbu_code string,
    lp_subbu_name string
) comment '物料主数据'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_material_master/';


-- add new columns
alter table ods_material_master add columns (
    dlr_quota_product_line_id int,
    dlr_quota_product_line string,
    dlr_auth_productline_id int,
    dlr_auth_productline string,
    sales_status string,
    is_active_for_sale int,
    subbu_code string,
    subbu_name string,
    lp_subbu_code string,
    lp_subbu_name string
    )
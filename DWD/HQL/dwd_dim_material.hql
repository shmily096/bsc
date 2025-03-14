-- Hive SQL
-- Function： 产品维度（DWD 层）
-- History: 
-- 2021-05-12    Donny   v1.0    draft
-- 2021-05-24    Donny   v1.1    new field business_group 
-- 2021-06-24    Donny   v1.2     alter table dwd_dim_material add columns (mit array<string>)
-- 2021-12-20    Donny   v1.3    add new field mit alter table dwd_dim_material add columns (mit array<string>)

drop table if exists dwd_dim_material;
create external table dwd_dim_material
(
    chinese_name              string,
    material_code             string,
    old_code                  string,
    english_name              string,
    profit_center             string,
    gtin                      string,
    sap_upl_level1_code       string,
    sap_upl_level1_name       string,
    sap_upl_level2_code       string,
    sap_upl_level2_name       string,
    sap_upl_level3_code       string,
    sap_upl_level3_name       string,
    sap_upl_level4_code       string,
    sap_upl_level4_name       string,
    sap_upl_level5_code       string,
    sap_upl_level5_name       string,
    special_procurement       string,
    latest_cfda               string,
    sheets                    string,
    mmpp                      string,
    d_chain                   string,
    spk                       string,
    default_location          string,
    source_list_indicator     string,
    pdt                       string,
    grt                       string,
    qi_flag                   string,
    loading_group             string,
    legal_status              string,
    delivery_plant            string,
    shelf_life_sap            string,
    standard_cost             string,
    transfer_price            string,
    pra                       string,
    material_type             string,
    pra_valid_from            string,
    pra_valid_to              string,
    pra_status                string,
    pra_release_status_code   string,
    standard_cost_usd         string,
    abc_class                 string,
    division_id               string comment 'BU ID',
    division_display_name     string comment 'BU Name', 
    product_line1_code        string,
    product_line1_name        string,
    product_line2_code        string,
    product_line2_name        string,
    product_line3_code        string,
    product_line3_name        string,
    product_line4_code        string,
    product_line4_name        string,
    product_line5_code        string,
    product_line5_name        string,
    m_shelf_life_for_oboselete string,
    xyz                       string,
    m_isv_bp                  string,
    product_model             string,
    business_group            string,
    sub_division              string,
    mit                       array<string>,
    dlr_quota_product_line_id int,
    dlr_quota_product_line string,
    dlr_auth_productline_id int,
    dlr_auth_productline string,
    sales_status string,
    is_active_for_sale int,
    subbu_code string,
    subbu_name string,
    lp_subbu_code string,
    lp_subbu_name string,
    sub_division_bak string
) comment '产品维度'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_material/'
tblproperties ("parquet.compression"="lzo");



-- add new columns
alter table dwd_dim_material add columns (
    dlr_quota_product_line_id int,
    dlr_quota_product_line string,
    dlr_auth_productline_id int,
    dlr_auth_productline string,
    sales_status string,
    is_active_for_sale int,
    subbu_code string,
    subbu_name string,
    lp_subbu_code string,
    lp_subbu_name string,
    sub_division_bak string
    ) cascade


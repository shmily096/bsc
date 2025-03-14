-- Hive SQL
-- Function： CFDA数据 （DWD 层）
-- History: 
-- 2021-11-16    Amanda   v1.0    draft

drop table if exists dwd_ctm_customer_master;
create external table dwd_dim_ctm_customer_maste
(
        material                         string
       ,product_type                     string
       ,chinese_name                     string
       ,english_name                     string
       ,hs_code                          string
       ,hs_additional_code               string
       ,enterprise_unit                  string
       ,delcare_unit                     string
       ,declaration_scale_factor         string
       ,currency                         string
       ,unit_price                       decimal(9,2)
       ,origin_country                   string
       ,first_legal_unit                 string
       ,first_scale_factor               string
       ,second_legal_unit                string
       ,second_scale_factor              string
       ,start_expiry_date                string
       ,bonded_property                  string
       ,materials_flag                   string
       ,special_remark                   string
       ,quantity                         decimal(9,2)
       ,netweight                        decimal(9,2)
       ,grossweight                      decimal(9,2)
       ,length                           decimal(9,2)
       ,width                            decimal(9,2)
       ,height                           decimal(9,2)
       ,distribution_properties          string
       ,import_documents_requirements    string
       ,export_documents_requirements    string
       ,end_expiry_date                  string
       ,supervision_certificate          string
       ,inspection_requirements          string
       ,MFN_tax_rate                     decimal(9,2)
       ,provisional_tax_rate             decimal(9,2)
       ,creation_date                    string
       ,last_modify_date                 string
  ) comment 'ctmcustomer_master'
partitioned by(dt string)
stored as parquet
location '/bsc/opsdw/dwd/dwd_ctm_customer_master/'
tblproperties ("parquet.compression"="lzo");
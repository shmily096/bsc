-- Hive SQL
-- Function�� ���������� ��DWD �㣩
-- History: 
-- 2021-05-08    Donny   v1.0    draft

drop table if exists dwd_dim_batch;
create external table dwd_dim_batch
(
    material          string comment '���ϱ���',
    batch             string comment '����',
    shelf_life_exp_date string comment '��Ч��',
    country_of_origin string comment '����',
    date_of_manuf     string comment '��������',
    cfda              string comment '����֤����'
) comment '����������'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_batch/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� ʱ��ά�� (DWD)
-- History: 
-- 2021-05-12    Donny   v1.0    init

drop table if exists dwd_dim_calendar;
create external table dwd_dim_calendar
(
    cal_date          string comment '��',
    cal_month         string comment '��',
    cal_year          string comment '��',
    cal_quarter       string comment '����',
    weeknum_m1        string,
    weeknum_y1        string,
    weeknum_m2        string,
    weeknum_y2        string,
    weekday           string,
    workday           string,
    workday_flag      string comment '�Ƿ�����',
    po_pattern        string,
    year_month_date   string comment '������',
    month_weeknum     string,
    day_of_week       string
) comment 'calendar dimension'
partitioned by (dt string)
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_calendar/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� customer business type
-- History: 
-- 2021-05-25    Donny   v1.0    init

drop table if exists dwd_dim_cust_business_type;
create external table dwd_dim_cust_business_type
(
    cust_account            string comment 'Customer Account', 
    country                 string comment 'Country', 
    cust_group              string comment 'Customer Group',
    business_type           string comment 'Type of Business', 
    cust_ci                 string comment 'CI'
) comment 'Customer business type'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_cust_business_type/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� �ͻ������ݵĲ�νṹ ��DWD �㣩
-- History: 
-- 2021-05-24    Donny   v1.0    init

drop table if exists dwd_dim_customer_level;
create external table dwd_dim_customer_level
(
    level1_code             string comment 'Level1 Code',
    level1_english_name     string comment 'Level1 English Name',
    level1_chinese_name     string comment 'Level1 Chinese Name',
    level2_code             string comment 'Level2 Code',
    level2_english_name     string comment 'Level2 English Name',
    level2_chinese_name     string comment 'Level2 Chinese Name',
    level3_code             string comment 'Level3 Code',
    level3_english_name     string comment 'Level3 English Name',
    level3_chinese_name     string comment 'Level3 Chinese Name',
    level4_code             string comment 'Level4 Code',
    level4_chinese_name     string comment 'Level4 Chinese Name',
    business_category       string comment 'Business Category'
) comment 'Customer level master data'
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_customer_level/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� ���������� ��DWD �㣩
-- History: 
-- 2021-05-21    Donny   v1.0    init
-- 2021-05-24    Donny   v1.1    add customer level fields

drop table if exists dwd_dim_customer;
create external table dwd_dim_customer
(
    cust_account      string,
    cust_name         string,
    cust_name2        string,
    city              string,
    post_code         string,
    rg                string,
    searchterm        string,
    street            string,
    telephone1        string,
    fax_number        string,
    tit               string,
    orblk             string,
    blb               string,
    cust_group        string,
    cl                string,
    dlv               string,
    del               string,
    cust_name3        string,
    cust_name4        string,
    distr             string,
    cust_b            string,
    transp_zone       string,
    country           string,
    delete_flag       string,
    tfn               string,
    level1_code       string,
    level1_english_name     string,
    level2_code             string,
    level2_english_name     string,
    level3_code             string,
    level3_english_name     string,
    level4_code             string,
    business_category       string comment 'Business Category'
) comment 'Customer master data'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_customer/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� Division Rebate Rate ��DWD �㣩
-- History: 
-- 2021-06-08    Donny   v1.0    inti

drop table if exists dwd_dim_division_rebate_rate;
create external table dwd_dim_division_rebate_rate
(
    id                      string comment 'division ID',
    division                string comment 'master division', 
    cust_business_type      array<string> comment 'Customer type of business rule 1', 
    upn_source_field1       string comment 'UPN PL1, PL2, PL3, Level1, Level2 rule 2',
    upn_except_value1       map<string,float> comment 'UPN except value for rule2',
    upn_source_field2       string comment 'UPN PL1, PL2, PL3, Level1, Level2 rule3',
    upn_except_value2       map<string,float> comment 'UNP except value for rule3',
    default_rebate_rate     float comment 'Sub BU'
) comment 'Division Rebate Rate'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_division_rebate_rate/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� BU ������ ��DWD �㣩
-- History: 
-- 2021-05-23    Donny   v1.0    init

drop table if exists dwd_dim_division;
create external table dwd_dim_division
(
    id          string
    ,division   string
    ,short_name string
    ,cn_name    string
    ,display_name string
) comment 'BU������'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_division/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� ���������� ��DWD �㣩
-- History: 
-- 2021-05-23    Donny   v1.0    init

drop table if exists dwd_dim_exchange_rate;
create external table dwd_dim_exchange_rate
(
    from_currency   string
    ,to_currency    string
    ,valid_from     string
    ,rate           string
    ,ratio_from     string
    ,ratio_to       string
) comment '����������'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_exchange_rate/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� Locationά�ȱ�
-- History: 
-- 2021-05-07    Donny   v1.0    draft

drop table if exists dwd_dim_locaiton;
create external table dwd_dim_locaiton
(
    d_plant           string,
    plant_name        string,
    location_id       string,
    location_status   string,
    storage_location  string,
    storage_definition string
) COMMENT '��λά����Ϣ'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_locaiton/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� ��Ʒά�ȣ�DWD �㣩
-- History: 
-- 2021-05-12    Donny   v1.0    draft
-- 2021-05-24    Donny   v1.1    new field business_group 

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
    business_group            string
) comment '��Ʒά��'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_material/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� SO order operation type ��DWD �㣩
-- History: 
-- 2021-05-25    Donny   v1.0    init

drop table if exists dwd_dim_order_operation_type;
create external table dwd_dim_order_operation_type
(
    master_id                   string comment 'Customer Type+Order Type + Order Reason', 
    operation_type              string comment 'order operation type chinese description', --������������
    order_type                  string comment 'SAP Order Type', 
    order_reason                string comment 'SAP Order Reason', 
    order_reason_description    string comment 'SAP Order Reason description', 
    customer_type               string comment 'Customer type', --�ͻ�����
    business_type               string comment 'Business Type', --ҵ������
    flow_direction              string comment 'Flow Direction', --����
    order_type_and_reason       string comment 'SAP Order Type & Order Reason', 
    wms_operation               string comment 'WMS Operation' -- �ֿ�WMS�߼�������
) comment 'Order operation type'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_order_operation_type/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� Plantά�ȱ�
-- History: 
-- 2021-05-07    Donny   v1.0    draft

drop table if exists dwd_dim_plant;
create external table dwd_dim_plant (
    id                string COMMENT 'Plant ���',
    name              string COMMENT '����',
    second_name       string COMMENT '�ڶ�������',
    postcode          string COMMENT '��������',
    city              string COMMENT '����',
    search_term1      string COMMENT '������1',
    search_term2      string COMMENT '������2'
) COMMENT 'Plantά�ȱ�'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_plant/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� �����̲ɹ����ۣ�ODS �㣩
-- History: 
-- 2021-06-07    Donny   v1.0    draft

drop table if exists dwd_fact_dealer_purchase_quotation;
create external table dwd_fact_dealer_purchase_quotation
(
    fin_year          string,
    division          string,
    sub_buname        string,
    sapi_d            string,
    dealer_name       string,
    parent_sapid      string,
    parent_dealer_name string,
    dealer_type       string,
    rsm               string,
    zsm               string,
    tsm               string,
    contract_start_date string,
    contract_end_date string,
    market_type       string,
    contract_status   string,
    new_old_dealer_by_bu string,
    new_old_dealer_by_bsc string,
    aop_type          string,
    month1_amount     decimal(18,4),
    month2_amount     decimal(18,4),
    month3_amount     decimal(18,4),
    q1_amount         decimal(18,4),
    month4_amount     decimal(18,4),
    month5_amount     decimal(18,4),
    month6_amount     decimal(18,4),
    q2_amount         decimal(18,4),
    month7_amount     decimal(18,4),
    month8_amount     decimal(18,4),
    month9_amount     decimal(18,4),
    q3_amount         decimal(18,4),
    month10_amount    decimal(18,4),
    month11_amount    decimal(18,4),
    month12_amount    decimal(18,4),
    q4_amount         decimal(18,4),
    year_total_amount decimal(18,4),
    bi_code           string,
    bi_name           string
) comment 'The dealer purchase quotation'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_dealer_purchase_quotation/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� ����ת�ַ���������
-- History: 
-- 2021-05-07    Donny   v1.0    draft

drop table if exists dwd_fact_domestic_sto_dn_detail;
create external table dwd_fact_domestic_sto_dn_detail
(
    delivery_no       string comment '���ڷ��������',
    line_number       string comment '�������к�',
    material          string comment '��Ʒ����',
    qty               bigint comment '����',
    batch             string comment '����',
    qr_code           string comment 'QR code'
) comment '����ת�ַ���������'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_domestic_sto_dn_detail/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� ����ת�ַ�������ʵ��
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-18    Donny   v1.1    update the name and the order
-- 2021-05-18    Donny   v1.2    add pgi date

drop table if exists dwd_fact_domestic_sto_dn_info;
create external table dwd_fact_domestic_sto_dn_info
(
    sto_no            string comment 'T2-T3ת�ֵ����',
    delivery_no       string comment '����Outbound ���������',
    reference_dn_number string comment '����Inbound DN��',
    create_datetime   string comment '��������������',
    create_by         string comment '������',
    update_datetime   string comment '��������������',
    update_by         string comment '�޸���',
    delivery_mode     string comment '����ģʽ',
    dn_status         string comment '������״̬',
    ship_from_location string comment 'ʼ��Location',
    ship_from_plant   string comment '����plant����',
    ship_to_plant     string comment '�ջ�plant����',
    ship_to_location  string comment '�ջ���λ',
    carrier           string comment '������',
    actual_migo_date  string comment 'ʵ���ջ�����',
    planned_good_issue_datetime string comment '�ƻ�����ʱ��',
    actua_good_issue_datetime string comment 'ʵ�ʷ���ʱ��',
    total_qty         bigint comment '�ϼ�����',
    actual_putaway_datetime string comment 'ʵ���ϼ�ʱ��',
    pgi_datetime string comment 'post goods issue'
) comment '����ת�ַ�����'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_domestic_sto_dn_info/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� ����ת�ֶ�����ʵ��
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-17    Donny   v1.1    update the field name
-- 2021-05-24    Donny   v1.2    update the field
-- 2021-06-18    Donny   v1.3    add field:default location

drop table if exists dwd_fact_domestic_sto_info;
create external table dwd_fact_domestic_sto_info
(
       sto_no            string comment 'T2-T3ת�ֵ����',
       create_datetime   string comment 'ת�ֵ��������ڣ�ʱ�䣩',
       created_by        string comment '������',
       update_datetime   string comment 'ת�ֵ��������ڣ�ʱ�䣩',
       updated_by        string comment '������',
       ship_from_plant   string comment '�����ִ���',
       ship_to_plant     string comment '�ջ��ִ���',
       order_status      string comment 'ת�ֵ�״̬',
       remarks           string comment 'ת�ֵ���ע',
       order_type        string comment 'ת�ֵ�����',
       order_reason      string comment 'ת�ֵ�����ԭ��',
       line_number       string comment 'ת�ֵ��к�',
       material          string comment '��Ʒ����',
       qty               bigint comment '����',
       unit              string comment '��λ',
       financial_dimension_id string comment 'BUά��',
       net_amount        decimal(16,2)  comment '���',
       default_location  string comment 'Ĭ�ϵ�Storage Location'
) comment '����ת�ֶ���'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_domestic_sto_info/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� IDD Tracking Information
-- History: 
-- 2021-05-07    Donny   v1.0    draft

drop table if exists dwd_fact_idd_tracking;
create external table dwd_fact_idd_tracking
(
    idd_delivery      string comment 'DN���',
    material_code     string comment '��Ʒ���',
    batch             string comment '����',
    Declaration_state string comment '�걨״̬',
    report_date       string comment 'IDD��������',
    idd_type          string comment 'IDD����',
    qty               string comment 'IDD����',
    packing_list_datetime string comment 'BSC�ṩPackingList����',
    slc_response_datetime string comment 'SLC���������Ϣʱ��',
    submit_datetime   string comment '�ύIDDʱ��',
    t1_respone_datetime string comment 'T1����ʱ��',
    related_dn        string comment '���ݲ���Delivery',
    idd_status        string comment '���״̬',
    putaway_datetime  string comment '�ϼ�ʱ��',
    remark            string comment '��ע',
    receiving_plant   string comment '���ղֿ�'
) comment 'IDD Tracking Information'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_idd_tracking/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� ������״̬��ʵ�� 
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-14    Donny   v1.1    update fields information
-- 2021-05-24    Donny   v1.2    update fields information

drop table if exists dwd_fact_import_export_declaration_info;
create external table dwd_fact_import_export_declaration_info
(
    commercial_invoice string,
    update_date       string,
    bsc_inform_slc_date string comment '֪ͨSLCʱ��',
    t1_pick_up_date   string,
    actual_arrival_time string comment '���ൽ��ʱ��',
    dock_warrant_date string comment '�ֵ�ȷ��ʱ��',
    invoice_receiving_date string comment '�ʼ�֪ͨԤ��ʱ��',
    forwording        string,
    into_inventory_date string comment 'ʵ�ʵ���ʱ��',
    shipment_internal_number string,
    master_bill_no    string,
    house_waybill_no  string,
    import_export_flag string,
    shipment_type     string,
    quantity          string,
    gross_weight      string,
    department        string,
    country_area      string,
    transportation_type string,
    etd               string comment 'Ԥ�ƺ������ʱ��',
    eta               string comment 'Ԥ�ƺ��ൽ��ʱ��',
    revise_etd        string,
    revise_eta        string,
    commodity_inspection string,
    customs_inspection string,
    declaration_start_date string comment '���ؿ�ʼʱ��', -- ods forwording_inform_slc_pick
    declaration_completion_date string comment '���ؽ���ʱ��',
    related_delivery_no string
) comment '������״̬'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_import_export_declaration_info/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� �����ڷ���������ʵ�±�
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-13    Donny   v1.1    update fields

drop table if exists dwd_fact_import_export_dn_detail;
create external table dwd_fact_import_export_dn_detail
(
    sto_no                  string COMMENT '������ת�ֵ�����',
    delivery_no             string COMMENT '�����ڷ���������',
    line_number             string COMMENT '�������к�',
    material_code           string COMMENT '��Ʒ����',
    qty                     bigint COMMENT '��������',
    batch_number            string COMMENT '����',
    ship_from_location      string COMMENT '������λ',
    ship_to_location        string COMMENT '�ջ���λ'
) COMMENT '�����ڷ���������'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_import_export_dn_detail/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� �����ڷ�����ʵ�±�
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-13    Donny   v1.1    update the name and fields
-- 2021-06-10    Donny   V1.2    add SKU type
    --alter table dwd_fact_import_export_dn_info add columns (item_business_group string);

drop table if exists dwd_fact_import_export_dn_info;
create external table dwd_fact_import_export_dn_info
(
    sto_no                  string COMMENT '������ת�ֵ�����',
    delivery_no             string COMMENT 'Outbound����������',
    reference_dn_no         string COMMENT '����Inbound DN��',
    created_datetime        string COMMENT '����������ʱ��',
    updated_datetime        string COMMENT '����������ʱ��',
    created_by              string COMMENT '������������',
    updated_by              string COMMENT '������������',
    receiver_customer_code  string COMMENT '�ջ����ͻ�����',
    delivery_mode           string COMMENT '���˷�ʽ',
    order_status            string COMMENT '״̬',
    ship_from_plant         string COMMENT '����plant����',
    ship_to_plant           string COMMENT '�ջ�plant����',
    total_qty               bigint COMMENT '��������',
    planned_good_issue_datetime string COMMENT '�ƻ�����ʱ��',
    actual_good_issue_datetime string COMMENT 'ʵ�ʷ���ʱ��',
    actual_migo_datetime    string COMMENT 'ʵ���ջ�ʱ��',
    actual_putaway_datetime string COMMENT 'ʵ���ϼ�ʱ��',
    fin_dim_id              string COMMENT 'BUά��',
    item_business_group     string COMMENT '��Ʒҵ�����'
) COMMENT '�����ڷ�����'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_import_export_dn_info/'
tblproperties ("parquet.compression"="lzo");

 -- Hive SQL 
 -- Function�� ������ת�ֵ�  
 -- History: 
 -- 2021-05-10 Rebecca v1.0 draft
 -- 2021-05-12 donny    v1.1    update the table name & type

drop table if exists dwd_fact_import_export_sto;
create external table dwd_fact_import_export_sto
(
    sto_id             string comment '������ת�ֱ��',
    created_datetime   string comment '����ʱ��',
    updated_datetime   string comment '���¸���ʱ��',
    created_by         string comment '������',
    updated_by         string comment '������',
    order_status       string comment 'ת�ֵ�״̬',
    order_type_id      string comment 'ת�ֵ�����',
    order_reason       string comment 'ת�ֵ�����ԭ��',
    order_remarks      string comment 'ת�ֵ�������ע',
    ship_from_plant_id string comment '����plant����',
    ship_to_plant_id   string comment 'Ŀ��plant����',
    line_number        string comment '�к�',
    material_code      string comment '��Ʒ����',
    qty                string comment '��Ʒ����',
    unit               string comment '��λ',
    unit_price         string comment '���� MVP2��չ�ֶ�',
    net_amount         string comment '��� MVP2��չ�ֶ�'
) comment '������ת�ֵ�'
partitioned by(dt string)
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_import_export_sto/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� ��潻�׼�¼ ��DWD �㣩
-- History: 
-- 2021-05-28    Donny   v1.0    init

drop table if exists dwd_fact_inventory_movement_trans;
create external table dwd_fact_inventory_movement_trans
(
    movement_type     string,
    reason_code       string,
    special_stock     string,
    material_doc      string,
    mat_item          bigint,
    stock_location    string,
    plant             string,
    material          string,
    batch             string,
    qty               bigint,
    sle_dbbd          string,
    posting_date      string,
    mov_time          string,
    user_name         string,
    delivery_no       string,
    po_number         string,
    po_item           bigint,
    header_text       string,
    original_reference string,
    enter_date        string
) comment 'Inventory movement transactions'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_inventory_movement_trans/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� �������� ��DWD �㣩
-- History: 
-- 2021-05-26    Donny   v1.0    init

drop table if exists dwd_fact_inventory_onhand;
create external table dwd_fact_inventory_onhand
(
    trans_date        string,
    inventory_type    string,
    plant             string,
    storage_loc       string,
    profic_center     string,
    material          string,
    batch             string,
    quantity          bigint,
    unrestricted      bigint,
    inspection        bigint,
    blocked_material  bigint,
    expiration_date   string,
    standard_cost     decimal(18,2),
    extended_cost     decimal(18,2),
    update_date       string
) comment 'Daily Inventory Onhand'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_inventory_onhand/'
tblproperties ("parquet.compression"="lzo");


-- Hive SQL
-- Function�� ������Ʒ�ɹ�������ʵ�� 
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-18    Donny   v1.1    update the field information

drop table if exists dwd_fact_purchase_order_info;
create external table dwd_fact_purchase_order_info
(
    purch_id           string comment '�ɹ������',
    po_status          string comment '�ɹ���״̬',
    created_datetime   string comment '�ɹ����������ڣ�ʱ�䣩',
    updated_datetime   string comment '��������',
    created_by         string comment '������',
    updated_by         string comment '������',
    line_number        string comment '�ɹ�����',
    material           string comment '��Ʒ����',
    qty                bigint comment '�ɹ�����',
    received_qty       bigint comment '�ջ�����',
    unit               string comment '��λ',
    purch_price        decimal(16,2) comment '�ɹ��۸�',
    currency           string comment '����',
    migo_date          string comment '�ջ�����',
    to_plant_id        string comment '�ջ�plant����',
    to_locatoin_id     string comment '�ջ���λ����',
    financial_dimension_id string comment '����ά��'
) comment '������Ʒ�ɹ�����'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_purchase_order_info/'
tblproperties ("parquet.compression"="lzo");


-- Hive SQL
-- Function�� �������۷�����������ʵ�� 
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-18    Donny   v1.1    update field name

drop table if exists dwd_fact_sales_order_dn_detail;
create external table dwd_fact_sales_order_dn_detail
(
   so_no             string comment '���۶�������',
   delivery_id       string comment '����������',
   line_number       string comment '�������к�',
   material          string comment '��Ʒ����',
   qty               string comment '��������',
   batch             string comment '����',
   qr_code           string comment '����QR'
) comment '�������۷���������'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_sales_order_dn_detail/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� �������۷�������ʵ�� 
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-28    Donny   v1.1    change table name
-- 2021-06-08    Donny   v1.2    add field receiving_confirmation_date

drop table if exists dwd_fact_sales_order_dn_info;
create external table dwd_fact_sales_order_dn_info
(
    so_no               string comment '���۶�������',
    delivery_id         string comment '����������',
    created_datetime    string comment '��������������',
    updated_datetime    string comment '��������������',
    created_by          string comment '������������',
    updated_by          string comment '������������',
    ship_to_address     string comment '��ַ����',
    real_shipto_address string comment 'ʵ���ͻ���ַ',
    planned_gi_date     string comment '�ƻ�����ʱ��',
    actual_gi_date      string comment 'ʵ�ʳ���ʱ��',
    receiving_confirmation_date string comment '�ͻ�ȷ���ջ�ʱ��',
    delivery_mode       string comment '������ʽ',
    carrier_id          string comment '������',
    pick_location_id    string comment '������λ',
    total_qty           bigint comment '�ϼ�����'
) comment '�������۷�����'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_sales_order_dn_info/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� ���۶�����ʵ��
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-07    Donny   v1.1    update the name of fields
-- 2021-05-24    Donny   v1.2    update the name of fields
-- 2021-06-18    Donny   v1.3    add division
-- TODO: add division, sub division,customer_level3
drop table if exists dwd_fact_sales_order_info;
create external table dwd_fact_sales_order_info
(
    so_no               string comment '���۶�������',
    order_type          string comment '��������',
    order_reason        string comment '����ԭ��',
    reject_reason       string comment '�ܾ�ԭ��',
    order_remarks       string comment '������ע',
    created_datetime    string comment '������������',
    updated_datetime    string comment '������������',
    created_by          string comment '����������',
    updated_by          string comment '����������',
    order_status        string comment '����״̬',
    reference_po_number string comment '����PO',
    line_number         string comment '�����к�',
    material            string comment '��Ʒ����',
    batch               string comment '����', 
    qty                 bigint comment '����',
    unit                string comment '��λ',
    net_value           decimal(16,2)  comment '���',
    currency            string comment '����',
    request_delivery_date string comment '�ͻ�Ҫ�󵽻�ʱ��',
    pick_up_plant       string comment '����plant',
    customer_code       string comment 'Sold To',
    ship_to_code        string comment '�ͻ���ַ����',
    division_id         string comment 'BUά��',
    customer_level3     string comment  'Customer Level3',
    customer_type       string comment 'Customer Type-Customer Level 4 code',
    order_operation_type string comment 'Order Operation Type'
) comment 'Sales Order Fact Table'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_sales_order_info/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� ���۶����ķ�Ʊ��ʵ��
-- History: 
--  2021-05-27    Donny   v1.0    init

drop table if exists dwd_fact_sales_order_invoice;
create external table dwd_fact_sales_order_invoice
(
    bill_id           string,
    accounting_no     string,
    bill_date         string,
    bill_type         string,
    sales_id          string,
    delivery_no       string,
    material          string,
    sales_line_no     bigint,
    batch             string,
    bill_qty          bigint,
    net_amount        decimal(18,2),
    currency          string,
    ship_to           string,
    sold_to           string,
    tax_amount        decimal(18,2),
    tax_rate          decimal(18,2),
    purchase_order    string
) comment 'Sales Order Invoice'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_sales_order_invoice/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function�� ���ػ�������Ϣ��ʵ��
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-24    Donny   v1.1    update table schema
-- 2021-05-29    Donny   v1.2    fix typo issue

drop table if exists dwd_fact_work_order;
create external table dwd_fact_work_order
(
    plant_id                string comment '���ػ����������ֿ�',
    commercial_invoice_no   string comment '��ҵ��ƱID',
    delivery_no             string comment '���������',
    work_order_no           string comment '�������',
    created_datetime        string comment '�����������ڣ�ʱ�䣩',
    created_by              string comment '����������',
    started_datetime        string comment '����ִ�п�ʼ���ڣ�ʱ�䣩',
    started_by              string comment '����ִ����',
    completed_datetime      string comment '����������ڣ�ʱ�䣩',
    released_datetime       string comment '���������������ڣ�ʱ�䣩',
    released_by             string comment '��������������',
    line_no                 string comment '�����к�',
    material                string comment '��Ʒ����',
    batch                   string comment '����',
    current_qty             string comment '�ƻ����ػ�����',
    processed_qty           string comment 'ִ������',
    release_qty             string comment '��������',
    qr_code                 string comment 'QR Code',
    work_order_status       string COMMENT '����״̬'
) COMMENT 'Work order loclization fact information'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_work_order/'
tblproperties ("parquet.compression"="lzo");

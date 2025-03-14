-- Hive SQL
-- Function： 批号主数据 （DWD 层）
-- History: 
-- 2021-05-08    Donny   v1.0    draft

drop table if exists dwd_dim_batch;
create external table dwd_dim_batch
(
    material          string comment '物料编码',
    batch             string comment '批号',
    shelf_life_exp_date string comment '有效期',
    country_of_origin string comment '产地',
    date_of_manuf     string comment '生产日期',
    cfda              string comment '质量证书编号'
) comment '批号主数据'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_batch/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function： 时间维度 (DWD)
-- History: 
-- 2021-05-12    Donny   v1.0    init

drop table if exists dwd_dim_calendar;
create external table dwd_dim_calendar
(
    cal_date          string comment '日',
    cal_month         string comment '月',
    cal_year          string comment '年',
    cal_quarter       string comment '季度',
    weeknum_m1        string,
    weeknum_y1        string,
    weeknum_m2        string,
    weeknum_y2        string,
    weekday           string,
    workday           string,
    workday_flag      string comment '是否工作日',
    po_pattern        string,
    year_month_date   string comment '年月日',
    month_weeknum     string,
    day_of_week       string
) comment 'calendar dimension'
partitioned by (dt string)
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_calendar/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function： customer business type
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
-- Function： 客户主数据的层次结构 （DWD 层）
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
-- Function： 批号主数据 （DWD 层）
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
-- Function： Division Rebate Rate （DWD 层）
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
-- Function： BU 主数据 （DWD 层）
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
) comment 'BU主数据'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_division/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function： 汇率主数据 （DWD 层）
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
) comment '汇率主数据'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_exchange_rate/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function： Location维度表
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
) COMMENT '库位维度信息'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_locaiton/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function： 产品维度（DWD 层）
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
) comment '产品维度'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_material/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function： SO order operation type （DWD 层）
-- History: 
-- 2021-05-25    Donny   v1.0    init

drop table if exists dwd_dim_order_operation_type;
create external table dwd_dim_order_operation_type
(
    master_id                   string comment 'Customer Type+Order Type + Order Reason', 
    operation_type              string comment 'order operation type chinese description', --订单操作类型
    order_type                  string comment 'SAP Order Type', 
    order_reason                string comment 'SAP Order Reason', 
    order_reason_description    string comment 'SAP Order Reason description', 
    customer_type               string comment 'Customer type', --客户类型
    business_type               string comment 'Business Type', --业务类型
    flow_direction              string comment 'Flow Direction', --流向
    order_type_and_reason       string comment 'SAP Order Type & Order Reason', 
    wms_operation               string comment 'WMS Operation' -- 仓库WMS逻辑及操作
) comment 'Order operation type'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_order_operation_type/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function： Plant维度表
-- History: 
-- 2021-05-07    Donny   v1.0    draft

drop table if exists dwd_dim_plant;
create external table dwd_dim_plant (
    id                string COMMENT 'Plant 编号',
    name              string COMMENT '名称',
    second_name       string COMMENT '第二个名称',
    postcode          string COMMENT '邮政编码',
    city              string COMMENT '城市',
    search_term1      string COMMENT '检索词1',
    search_term2      string COMMENT '检索词2'
) COMMENT 'Plant维度表'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_plant/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function： 经销商采购报价（ODS 层）
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
-- Function： 国内转仓发货单详情
-- History: 
-- 2021-05-07    Donny   v1.0    draft

drop table if exists dwd_fact_domestic_sto_dn_detail;
create external table dwd_fact_domestic_sto_dn_detail
(
    delivery_no       string comment '国内发货单编号',
    line_number       string comment '发货单行号',
    material          string comment '产品代码',
    qty               bigint comment '数量',
    batch             string comment '批次',
    qr_code           string comment 'QR code'
) comment '国内转仓发货单详情'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_domestic_sto_dn_detail/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function： 国内转仓发货单事实表
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-18    Donny   v1.1    update the name and the order
-- 2021-05-18    Donny   v1.2    add pgi date

drop table if exists dwd_fact_domestic_sto_dn_info;
create external table dwd_fact_domestic_sto_dn_info
(
    sto_no            string comment 'T2-T3转仓单编号',
    delivery_no       string comment '国内Outbound 发货单编号',
    reference_dn_number string comment '关联Inbound DN单',
    create_datetime   string comment '发货单创建日期',
    create_by         string comment '创建人',
    update_datetime   string comment '发货单更新日期',
    update_by         string comment '修改人',
    delivery_mode     string comment '发货模式',
    dn_status         string comment '发货单状态',
    ship_from_location string comment '始发Location',
    ship_from_plant   string comment '发货plant代码',
    ship_to_plant     string comment '收货plant代码',
    ship_to_location  string comment '收货仓位',
    carrier           string comment '承运人',
    actual_migo_date  string comment '实际收货日期',
    planned_good_issue_datetime string comment '计划发货时间',
    actua_good_issue_datetime string comment '实际发货时间',
    total_qty         bigint comment '合计数量',
    actual_putaway_datetime string comment '实际上架时间',
    pgi_datetime string comment 'post goods issue'
) comment '国内转仓发货单'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_domestic_sto_dn_info/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function： 国内转仓订单事实表
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-17    Donny   v1.1    update the field name
-- 2021-05-24    Donny   v1.2    update the field
-- 2021-06-18    Donny   v1.3    add field:default location

drop table if exists dwd_fact_domestic_sto_info;
create external table dwd_fact_domestic_sto_info
(
       sto_no            string comment 'T2-T3转仓单编号',
       create_datetime   string comment '转仓单创建日期（时间）',
       created_by        string comment '创建人',
       update_datetime   string comment '转仓单更新日期（时间）',
       updated_by        string comment '更新人',
       ship_from_plant   string comment '发货仓代码',
       ship_to_plant     string comment '收货仓代码',
       order_status      string comment '转仓单状态',
       remarks           string comment '转仓单备注',
       order_type        string comment '转仓单类型',
       order_reason      string comment '转仓单类型原因',
       line_number       string comment '转仓单行号',
       material          string comment '产品代码',
       qty               bigint comment '数量',
       unit              string comment '单位',
       financial_dimension_id string comment 'BU维度',
       net_amount        decimal(16,2)  comment '金额',
       default_location  string comment '默认的Storage Location'
) comment '国内转仓订单'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_domestic_sto_info/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function： IDD Tracking Information
-- History: 
-- 2021-05-07    Donny   v1.0    draft

drop table if exists dwd_fact_idd_tracking;
create external table dwd_fact_idd_tracking
(
    idd_delivery      string comment 'DN编号',
    material_code     string comment '产品编号',
    batch             string comment '批号',
    Declaration_state string comment '申报状态',
    report_date       string comment 'IDD发现日期',
    idd_type          string comment 'IDD种类',
    qty               string comment 'IDD数量',
    packing_list_datetime string comment 'BSC提供PackingList日期',
    slc_response_datetime string comment 'SLC反馈表格信息时间',
    submit_datetime   string comment '提交IDD时间',
    t1_respone_datetime string comment 'T1反馈时间',
    related_dn        string comment '数据补充Delivery',
    idd_status        string comment '完成状态',
    putaway_datetime  string comment '上架时间',
    remark            string comment '备注',
    receiving_plant   string comment '接收仓库'
) comment 'IDD Tracking Information'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_idd_tracking/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function： 进出口状态事实表 
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-14    Donny   v1.1    update fields information
-- 2021-05-24    Donny   v1.2    update fields information

drop table if exists dwd_fact_import_export_declaration_info;
create external table dwd_fact_import_export_declaration_info
(
    commercial_invoice string,
    update_date       string,
    bsc_inform_slc_date string comment '通知SLC时间',
    t1_pick_up_date   string,
    actual_arrival_time string comment '航班到港时间',
    dock_warrant_date string comment '仓单确认时间',
    invoice_receiving_date string comment '邮件通知预报时间',
    forwording        string,
    into_inventory_date string comment '实际到仓时间',
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
    etd               string comment '预计航班起飞时间',
    eta               string comment '预计航班到达时间',
    revise_etd        string,
    revise_eta        string,
    commodity_inspection string,
    customs_inspection string,
    declaration_start_date string comment '报关开始时间', -- ods forwording_inform_slc_pick
    declaration_completion_date string comment '报关结束时间',
    related_delivery_no string
) comment '进出口状态'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_import_export_declaration_info/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function： 进出口发货单详情实事表
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-13    Donny   v1.1    update fields

drop table if exists dwd_fact_import_export_dn_detail;
create external table dwd_fact_import_export_dn_detail
(
    sto_no                  string COMMENT '进出口转仓单编码',
    delivery_no             string COMMENT '进出口发货单编码',
    line_number             string COMMENT '发货单行号',
    material_code           string COMMENT '产品代码',
    qty                     bigint COMMENT '发货数量',
    batch_number            string COMMENT '批号',
    ship_from_location      string COMMENT '发货仓位',
    ship_to_location        string COMMENT '收货仓位'
) COMMENT '进出口发货单详情'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_import_export_dn_detail/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function： 进出口发货单实事表
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-13    Donny   v1.1    update the name and fields
-- 2021-06-10    Donny   V1.2    add SKU type
    --alter table dwd_fact_import_export_dn_info add columns (item_business_group string);

drop table if exists dwd_fact_import_export_dn_info;
create external table dwd_fact_import_export_dn_info
(
    sto_no                  string COMMENT '进出口转仓单编码',
    delivery_no             string COMMENT 'Outbound发货单编码',
    reference_dn_no         string COMMENT '关联Inbound DN单',
    created_datetime        string COMMENT '发货单创建时间',
    updated_datetime        string COMMENT '发货单更新时间',
    created_by              string COMMENT '发货单创建人',
    updated_by              string COMMENT '发货单更新人',
    receiver_customer_code  string COMMENT '收货方客户代码',
    delivery_mode           string COMMENT '发运方式',
    order_status            string COMMENT '状态',
    ship_from_plant         string COMMENT '发货plant代码',
    ship_to_plant           string COMMENT '收货plant代码',
    total_qty               bigint COMMENT '发货数量',
    planned_good_issue_datetime string COMMENT '计划发货时间',
    actual_good_issue_datetime string COMMENT '实际发货时间',
    actual_migo_datetime    string COMMENT '实际收货时间',
    actual_putaway_datetime string COMMENT '实际上架时间',
    fin_dim_id              string COMMENT 'BU维度',
    item_business_group     string COMMENT '产品业务类别'
) COMMENT '进出口发货单'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_import_export_dn_info/'
tblproperties ("parquet.compression"="lzo");

 -- Hive SQL 
 -- Function： 进出口转仓单  
 -- History: 
 -- 2021-05-10 Rebecca v1.0 draft
 -- 2021-05-12 donny    v1.1    update the table name & type

drop table if exists dwd_fact_import_export_sto;
create external table dwd_fact_import_export_sto
(
    sto_id             string comment '进出口转仓编号',
    created_datetime   string comment '创建时间',
    updated_datetime   string comment '最新更新时间',
    created_by         string comment '创建人',
    updated_by         string comment '更新人',
    order_status       string comment '转仓单状态',
    order_type_id      string comment '转仓单类型',
    order_reason       string comment '转仓单类型原因',
    order_remarks      string comment '转仓单订单备注',
    ship_from_plant_id string comment '发货plant编码',
    ship_to_plant_id   string comment '目的plant编码',
    line_number        string comment '行号',
    material_code      string comment '产品代码',
    qty                string comment '产品数量',
    unit               string comment '单位',
    unit_price         string comment '单价 MVP2扩展字段',
    net_amount         string comment '金额 MVP2扩展字段'
) comment '进出口转仓单'
partitioned by(dt string)
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_import_export_sto/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function： 库存交易记录 （DWD 层）
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
-- Function： 日现有量 （DWD 层）
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
-- Function： 三方成品采购订单事实表 
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-18    Donny   v1.1    update the field information

drop table if exists dwd_fact_purchase_order_info;
create external table dwd_fact_purchase_order_info
(
    purch_id           string comment '采购单编号',
    po_status          string comment '采购单状态',
    created_datetime   string comment '采购单创建日期（时间）',
    updated_datetime   string comment '更新日期',
    created_by         string comment '创建人',
    updated_by         string comment '更新人',
    line_number        string comment '采购单行',
    material           string comment '产品代码',
    qty                bigint comment '采购数量',
    received_qty       bigint comment '收货数量',
    unit               string comment '单位',
    purch_price        decimal(16,2) comment '采购价格',
    currency           string comment '币种',
    migo_date          string comment '收货日期',
    to_plant_id        string comment '收货plant代码',
    to_locatoin_id     string comment '收货仓位代码',
    financial_dimension_id string comment '财务维度'
) comment '三方成品采购订单'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_purchase_order_info/'
tblproperties ("parquet.compression"="lzo");


-- Hive SQL
-- Function： 国内销售发货单详情事实表 
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-18    Donny   v1.1    update field name

drop table if exists dwd_fact_sales_order_dn_detail;
create external table dwd_fact_sales_order_dn_detail
(
   so_no             string comment '销售订单编码',
   delivery_id       string comment '发货单编码',
   line_number       string comment '发货单行号',
   material          string comment '产品代码',
   qty               string comment '发货数量',
   batch             string comment '批次',
   qr_code           string comment '发货QR'
) comment '国内销售发货单详情'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_sales_order_dn_detail/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function： 国内销售发货单事实表 
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-28    Donny   v1.1    change table name
-- 2021-06-08    Donny   v1.2    add field receiving_confirmation_date

drop table if exists dwd_fact_sales_order_dn_info;
create external table dwd_fact_sales_order_dn_info
(
    so_no               string comment '销售订单编码',
    delivery_id         string comment '发货单编码',
    created_datetime    string comment '发货单创建日期',
    updated_datetime    string comment '发货单更新日期',
    created_by          string comment '发货单创建人',
    updated_by          string comment '发货单更新人',
    ship_to_address     string comment '地址编码',
    real_shipto_address string comment '实际送货地址',
    planned_gi_date     string comment '计划出库时间',
    actual_gi_date      string comment '实际出库时间',
    receiving_confirmation_date string comment '客户确认收货时间',
    delivery_mode       string comment '发货方式',
    carrier_id          string comment '承运人',
    pick_location_id    string comment '发货仓位',
    total_qty           bigint comment '合计数量'
) comment '国内销售发货单'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_sales_order_dn_info/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function： 销售订单事实表
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-07    Donny   v1.1    update the name of fields
-- 2021-05-24    Donny   v1.2    update the name of fields
-- 2021-06-18    Donny   v1.3    add division
-- TODO: add division, sub division,customer_level3
drop table if exists dwd_fact_sales_order_info;
create external table dwd_fact_sales_order_info
(
    so_no               string comment '销售订单编码',
    order_type          string comment '订单类型',
    order_reason        string comment '订单原因',
    reject_reason       string comment '拒绝原因',
    order_remarks       string comment '订单备注',
    created_datetime    string comment '订单创建日期',
    updated_datetime    string comment '订单更新日期',
    created_by          string comment '订单创建人',
    updated_by          string comment '订单更新人',
    order_status        string comment '订单状态',
    reference_po_number string comment '关联PO',
    line_number         string comment '订单行号',
    material            string comment '产品编码',
    batch               string comment '批号', 
    qty                 bigint comment '数量',
    unit                string comment '单位',
    net_value           decimal(16,2)  comment '金额',
    currency            string comment '币种',
    request_delivery_date string comment '客户要求到货时间',
    pick_up_plant       string comment '发出plant',
    customer_code       string comment 'Sold To',
    ship_to_code        string comment '客户地址代码',
    division_id         string comment 'BU维度',
    customer_level3     string comment  'Customer Level3',
    customer_type       string comment 'Customer Type-Customer Level 4 code',
    order_operation_type string comment 'Order Operation Type'
) comment 'Sales Order Fact Table'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_sales_order_info/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function： 销售订单的发票事实表
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
-- Function： 本地化工单信息事实表
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-24    Donny   v1.1    update table schema
-- 2021-05-29    Donny   v1.2    fix typo issue

drop table if exists dwd_fact_work_order;
create external table dwd_fact_work_order
(
    plant_id                string comment '本地化工单创建仓库',
    commercial_invoice_no   string comment '商业发票ID',
    delivery_no             string comment '发货单编号',
    work_order_no           string comment '工单编号',
    created_datetime        string comment '工单创建日期（时间）',
    created_by              string comment '工单创建人',
    started_datetime        string comment '工单执行开始日期（时间）',
    started_by              string comment '工单执行人',
    completed_datetime      string comment '工单完成日期（时间）',
    released_datetime       string comment '工单质量放行日期（时间）',
    released_by             string comment '工单质量放行人',
    line_no                 string comment '工单行号',
    material                string comment '产品代码',
    batch                   string comment '批次',
    current_qty             string comment '计划本地化数量',
    processed_qty           string comment '执行数量',
    release_qty             string comment '放行数量',
    qr_code                 string comment 'QR Code',
    work_order_status       string COMMENT '工单状态'
) COMMENT 'Work order loclization fact information'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_work_order/'
tblproperties ("parquet.compression"="lzo");

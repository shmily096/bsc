-- Hive SQL
-- Function： 批号主数据 （ODS 层）
-- History: 
-- 2021-05-08    Donny   v1.0    draft

drop table if exists ods_batch_master;
create external table ods_batch_master
(
    material          string comment '物料编码',
    batch             string comment '批号',
    shelf_life_exp_date string comment '有效期',
    country_of_origin string comment '产地',
    date_of_manuf     string comment '生产日期',
    cfda              string comment '质量证书编号'
) comment '批号主数据'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_batch_master/';

-- Hive SQL
-- Function： 日历主数据 （ODS 层）
-- History: 
-- 2021-05-08    Donny   v1.0    draft

drop table if exists ods_calendar_master;
create external table ods_calendar_master
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
) comment '日历主数据'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_calendar_master/';

-- Hive SQL
-- Function： ODS 商业发票与发货单对应表
-- History: 
-- 2021-05-08    Donny   v1.0    draft
-- 2021-05-20    Donny   v1.1    update field information

drop table if exists ods_commercial_invoice_dn_mapping;
create external table ods_commercial_invoice_dn_mapping
(
    id                int comment 'ID',
    update_dt         string comment 'UpdateDT',
    active            string comment 'Active',
    delivery          string comment '发货单',
    invoice           string comment '商业发票',
    qty               bigint,
    mail_received     string comment '预报时间',
    sap_migo_date     string comment 'MIGO'
) comment '商业发票与发货单对应表'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_commercial_invoice_dn_mapping/';

-- Hive SQL
-- Function： customer business type（ODS 层）
-- History: 
-- 2021-05-25    Donny   v1.0    init

drop table if exists ods_cust_business_type;
create external table ods_cust_business_type
(
    cust_account            string comment 'Customer Account', 
    country                 string comment 'Country', 
    cust_group              string comment 'Customer Group',
    business_type           string comment 'Type of Business', 
    cust_ci                 string comment 'CI'
) comment 'Customer Business Type'
row format delimited fields terminated by ',' 
location '/bsc/opsdw/ods/ods_cust_business_type/';

-- Hive SQL
-- Function： 客户层次结构主数据 （ODS 层）
-- History: 
-- 2021-05-24    Donny   v1.0    init

drop table if exists ods_customer_level;
create external table ods_customer_level
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
row format delimited fields terminated by '\t' 
location '/bsc/opsdw/ods/ods_customer_level/';

-- Hive SQL
-- Function： 客户主数据 （ODS 层）
-- History: 
-- 2021-05-20    Donny   v1.0    draft

drop table if exists ods_customer_master;
create external table ods_customer_master
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
    tfn               string
) comment '客户主数据'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_customer_master/';

-- Hive SQL
-- Function： 经销商采购报价（ODS 层）
-- History: 
-- 2021-06-07    Donny   v1.0    draft

drop table if exists ods_dealer_purchase_quotation;
create external table ods_dealer_purchase_quotation
(
    update_dt         string,
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
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_dealer_purchase_quotation/';

-- Hive SQL
-- Function： BU主数据 （ODS 层）
-- History: 
-- 2021-05-20    Donny   v1.0    draft

drop table if exists ods_division_master;
create external table ods_division_master
(
    id          string
    ,division   string
    ,short_name string
    ,cn_name    string
    ,display_name string
) comment 'BU主数据'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_division_master/';

-- Hive SQL
-- Function： Rebate Rate（ODS 层）
-- History: 
-- 2021-06-03    Donny   v1.0    init
-- 2021-06-07    Donny   v1.1    update the name

drop table if exists ods_division_rebate_rate;
create external table ods_division_rebate_rate
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
row format delimited fields terminated by '\t'
collection items terminated by ',' 
map keys terminated by ':'
location '/bsc/opsdw/ods/ods_division_rebate_rate/';

-- Hive SQL
-- Function： ODS 国内转仓发货单
-- History: 
-- 2021-05-08    Donny   v1.0    draft
-- 2021-05-21    Donny   v1.1    add new field:pgi_date

drop table if exists ods_domestic_delivery;
create external table ods_domestic_delivery
(
    id                bigint comment 'ID',
    update_dt         string comment '更新日期',
    active            string comment 'Active',
    sto_no            string comment 'T2-T3转仓单编号',
    sap_delivery_no_inbound string comment '国内发货单编号',
    sap_delivery_no_outbound string comment '国内发货单编号',
    dn_create_dt      string comment '发货单创建日期',
    dn_create_by      string comment '创建人',
    dn_update_dt      string comment '发货单更新日期',
    dn_update_by      string comment '修改人',
    dn_status         string comment '发货单状态',
    sap_delivery_line_no string comment '发货单行号',
    material          string comment '产品代码',
    qty               string comment '数量',
    batch             string comment '批次',
    ship_from_plant   string comment '转仓单发送plant',
    ship_from_location string comment '转仓单发出仓位',
    ship_to_plant     string comment '收货plant',
    ship_to_location  string comment '收货仓位',
    qr_code           string comment 'QR code',
    delivery_mode     string comment '发货模式',
    carrier           string comment '承运人',
    actual_migo_dt    string comment '实际收货日期',
    pgi_date          string COMMENT '计划发货时间'
) comment '国内转仓发货单'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_domestic_delivery/';

-- Hive SQL
-- Function： ODS 国内转仓订单
-- History: 
--  2021-05-08    Donny   v1.0    draft

drop table if exists ods_domestic_sto;
create external table ods_domestic_sto
(
    id                bigint comment 'ID',
    update_dt         string comment '更新时间',
    active            string comment 'Active',
    sto_no             string comment 'T2-T3转仓单编号',
    sto_ceate_dt      string comment '转仓单创建日期',
    sto_create_by     string comment '创建人',
    sto_update_dt     string comment '转仓单更新日期',
    sto_updated_by    string comment '更新人',
    sto_status        string comment '转仓单状态',
    remarks           string comment '转仓单备注',
    sto_type          string comment '转仓单类型',
    sto_reason        string comment '转仓单类型原因',
    ship_from_plant   string comment '发货仓代码',
    ship_to_plant     string comment '收货仓代码',
    stoline_no        string comment '转仓单行号',
    material          string comment '产品代码',
    qty               string comment '数量',
    unit              string comment '单位'
) comment '国内转仓订单'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_domestic_sto/';

-- Hive SQL
-- Function： 汇率主数据 （ODS 层）
-- History: 
-- 2021-05-20    Donny   v1.0    draft

drop table if exists ods_exchange_rate;
create external table ods_exchange_rate
(
    from_currency   string
    ,to_currency    string
    ,valid_from     string
    ,rate           string
    ,ratio_from     string
    ,ratio_to       string
) comment '汇率主数据'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_exchange_rate/';

-- Hive SQL
-- Function： IDD主数据 （ODS 层）
-- History: 
-- 2021-05-08    Donny   v1.0    draft

drop table if exists ods_idd_master;
create external table ods_idd_master
(
    idd_delivery      string comment 'IDD Delivery',
    material          string comment '产品',
    batch             string comment '批号',
    declare_status    string comment '申报状态',
    idd_date          string comment 'IDD发现日期',
    idd_type          string comment 'IDD种类',
    idd_quantity      string comment 'IDD数量',
    packing_list_date string comment 'BSC提供PackingList日期',
    slc_date          string comment 'SLC反馈表格信息时间',
    idd_submit_date   string comment '提交IDD时间',
    t1_date           string comment 'T1反馈时间',
    data_supplement_delivery string comment '数据补充Delivery',
    idd_status        string comment 'IDD完成状态',
    shelf_date        string comment '上架时间',
    remark            string comment '备注',
    receiving_plant   string comment '收货仓'
) comment 'IDD主数据'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_idd_master/';

-- Hive SQL
-- Function： ODS 进出口发货单
-- History: 
--  2021-05-07    Donny   v1.0    draft
drop table if exists ods_import_export_delivery;
create external table ods_import_export_delivery
(
    id                bigint,
    update_dt         string,
    active            string,
    sto_no            string comment '进出口转仓单编码',
    sap_delivery_no_inbound string comment '进出口发货单编码',
    sap_delivery_no_outbound string comment '进出口发货单编码',
    dn_create_dt      string comment '发货单创建日期',
    dn_status         string comment '发货单状态',
    dn_update_dt      string comment '发货单更新日期',
    dn_create_by      string comment '发货单创建人',
    dn_updated_by     string comment '发货单更新人',
    receiver_customer_code string comment '收货方客户代码',
    sap_delivery_line_no string comment '发货单行号',
    material          string comment '产品代码',
    qty               string comment '发货数量',
    batch             string comment '批号',
    ship_from_plant   string comment '发货plant',
    ship_from_location string comment '发货仓位',
    ship_to_plant     string comment '收货plant',
    ship_to_location  string comment '收货仓位',
    delivery_mode     string comment '发运方式',
    actual_migo_dt    string comment '实际收货日期',
    pgi_date          string COMMENT '计划发货时间'
) comment '进出口发货单'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_import_export_delivery/'
;

-- Hive SQL
-- Function： ODS 进出口转仓表
-- History: 
--  2021-05-07    Donny   v1.0    draft
--  2021-05-08    Donny   v1.0    update the field inforamtion

drop table if exists ods_import_export_transaction;
create external table ods_import_export_transaction
(
    id                bigint comment 'ID',
    update_dt         string comment '更新时间',
    active            string comment 'Active',
    sto_no            string comment '进出口转仓单编号',
    sto_create_dt     string comment '创建日期',
    sto_update_dt     string comment '修改日期',
    sto_created_by    string comment '创建人',
    sto_updated_by    string comment '修改人',
    sto_status        string comment '转仓单状态',
    sto_type          string comment '转仓单种类',
    sto_order_reason  string comment '转仓单order reason',
    order_remarks     string comment '转仓单订单备注',
    ship_from_plant   string comment '发货plant',
    ship_to_plant     string comment '目的地plant代码',
    sto_line_no       string comment '转仓点行号',
    material          string comment '产品代码',
    qty               bigint comment '产品数量',
    unit              string comment '单位'
) comment '进出口转仓单'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' -- 指定分割符为\t 
STORED AS -- 指定存储方式，读数据采用 LzoTextInputFormat；输出数据采用 TextOutputFormat
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_import_export_transaction/' -- 指定数据在 hdfs 上的存储位置
;


-- Hive SQL
-- Function： ODS 发货单对应表
-- History: 
--  2021-05-08    Donny   v1.0    draft

drop table if exists ods_inbound_outbound_dn_mapping;
create external table ods_inbound_outbound_dn_mapping
(
    id               bigint comment 'ID',
    update_dt        string comment 'UpdateDT',
    active           string comment 'Active',
    inbound_dn       string comment 'inbound发货单编码',
    outbond_dn       string comment 'outbound发货点编码'
) comment '发货单对应表'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_inbound_outbound_dn_mapping/';

-- Hive SQL
-- Function： 库存交易记录
-- History: 
--  2021-05-21    Donny   v1.0    init

drop table if exists ods_inventory_movement_trans;
create external table ods_inventory_movement_trans
(
    update_dt         string,
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
) comment 'Inventory transactions'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_inventory_transactions/';

-- Hive SQL
-- Function： 日现有量 （ODS 层）
-- History: 
-- 2021-05-26    Donny   v1.0    init

drop table if exists ods_inventory_onhand;
create external table ods_inventory_onhand
(
    update_dt         string,
    active            string,
    trans_date        string,
    inventory_type    string,
    plant_from        string,
    plant_to          string,
    storage_loc       string,
    pdt               bigint,
    pgi_date          string,
    delivery          string,
    delivery_line     bigint,
    marked_in_house   string,
    transport_order   string,
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
    qn_info           string,
    update_date       string,
    eom_ym            string,
    bu_flag           string,
    ur_qty_flag       string,
    qi_qty_flag       string,
    blk_qty_flag      string,
    expiration_date_flag string,
    short_dated_shelf_flag string,
    inbound_delivery  string
) comment 'Daily Inventory Onhand'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_inventory_onhand/';


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
    m_isv_bp          string
) comment '物料主数据'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_material_master/';

-- Hive SQL
-- Function： Material rebate rate（ODS 层）
-- History: 
-- 2021-06-01    Donny   v1.0    init
-- 2021-06-01    Donny   v1.1    update the fields

drop table if exists ods_material_rebate_rate;
create external table ods_material_rebate_rate
(
    id                      string comment 'BU ID',
    bu                      string comment 'master BU', 
    is_all_upn              boolean comment 'All SKUs have same rabate rate, true or false',
    is_all_cust             boolean comment 'All cust type have same rabate rate, true or false',
    cust_business_type      string comment 'Customer type of business', 
    upn_source_field        string comment 'SKU Group:PL1, PL2, PL3, Level1, Level2',
    rebate_rate             float  comment 'Rebate Rate'
) comment 'Material rebate rate'
row format delimited fields terminated by '\t' 
location '/bsc/opsdw/ods/ods_material_rebate_rate/';

-- Hive SQL
-- Function： SO order operation type（ODS 层）
-- History: 
-- 2021-05-25   Donny   v1.0    init

drop table if exists ods_order_operation_type;
create external table ods_order_operation_type
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
) comment 'Order operation type master data'
row format delimited fields terminated by '\t' 
location '/bsc/opsdw/ods/ods_order_operation_type/';

-- Hive SQL
-- Function： plant主数据 （ODS 层）
-- History: 
-- 2021-05-08    Donny   v1.0    draft

drop table if exists ods_plant_master;
create external table ods_plant_master
(
    plant_code        string,
    search_term2      string,
    search_term1      string,
    postl_code        string,
    city              string,
    name2             string,
    name1             string
) comment 'Plant主数据'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_plant_master/';


-- Hive SQL
-- Function： ODS 三方成品采购订单
-- History: 
-- 2021-05-08    Donny   v1.0    draft

drop table if exists ods_purchase_order;
create external table ods_purchase_order
(
    id                bigint comment 'ID',
    update_dt         string comment '更新时间',
    active            string comment 'Active',
    purchase_order_no string comment '采购单编号',
    po_create_dt      string comment '采购单创建日期',
    po_create_by      string comment '创建人',
    po_updated_dt     string comment '更新日期>',
    po_updated_by     string comment '更新人',
    po_status         string comment '采购单状态',
    po_line_no        string comment '采购单行',
    material          string comment '产品代码',
    qty               bigint comment '采购数量',
    unit              string comment '单位',
    purchase_price    decimal(16,2) comment '采购价格',
    currency          string comment '币种',
    migo_date         string comment '收货日期',
    batch             string comment '批次',
    received_qty      bigint comment '收货数量'
) comment '三方成品采购订单'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_purchase_order/';

-- Hive SQL
-- Function： WMS 入库上架信息
-- History: 
--  2021-05-21    Donny   v1.0    init

drop table if exists ods_putaway_info;
create external table ods_putaway_info
(
    update_dt         string,
    invoice           string,
    delivery_no       string,
    putaway_date      string,
    upn               string,
    qty               bigint,
    batch             string,
    plant             string,
    sl                string,
    unit              string,
    from_slocation    string,
    to_location       string
) comment 'Inventory transactions'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_putaway_info/';

-- Hive SQL
-- Function： ODS 销售发货单
-- History: 
-- 2021-05-08    Donny   v1.0    draft

drop table if exists ods_sales_delivery;
create external table ods_sales_delivery
(
    id                bigint comment 'ID',
    update_dt         string comment '更新时间',
    active            string comment 'Active',
    so_no              string comment '销售订单编码',
    sap_delivery_no   string comment '发货单编码',
    dn_create_dt      string comment '发货单创建日期',
    dn_update_dt      string comment '发货单更新日期',
    dn_create_by      string comment '发货单创建人',
    dn_updated_by     string comment '发货单更新人',
    ship_to           string comment '地址编码',
    real_ship_to_address string comment '实际送货地址',
    delivery_line     string comment '发货单行号',
    material          string comment '产品代码',
    qty               string comment '发货数量',
    qr_code           string comment '发货QRCode',
    batch             string comment '批次',
    planned_gi_date    string comment '计划出库时间',
    actual_gi_date     string comment '实际出库时间',
    delivery_mode     string comment '发货方式',
    carrier           string comment '承运人',
    pick_location     string comment '发货仓位'
) comment '销售发货单'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_sales_delivery/';

-- Hive SQL
-- Function： ODS 销售订单
-- History: 
-- 2021-05-08    Donny   v1.0    draft

drop table if exists ods_sales_order;
create external table ods_sales_order
(
    id                bigint,
    update_dt         string,
    active            string,
    so_no             string,
    order_type        string,
    order_reason      string,
    reject_reason     string,
    order_remarks     string,
    so_create_dt      string,
    so_update_dt      string,
    so_create_by      string,
    so_updated_by     string,
    so_status         string,
    po_number         string,
    sales_org         string,
    storage_loc       string,
    soline_no         string,
    material          string,
    batch             string,
    profit_center     string,
    delivery_date     string,
    qty               bigint,
    net_value         decimal(16,2)  comment '金额',
    currency          string,
    delivery_block    string,
    billing_block     string,
    unit              string,
    request_delivery_date string,
    pick_up_plant     string,
    customer_code     string,
    ship_to_code      string
) comment '销售订单'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_sales_order/';

-- Hive SQL
-- Function： ODS 进出口inbound_tracking
-- History: 
-- 2021-05-08    Donny   v1.0    draft

drop table if exists ods_shipment_status_inbound_tracking;
create external table ods_shipment_status_inbound_tracking
(
    id                bigint,
    update_dt         string,
    active            string,
    work_number       string,
    commercial_invoice string,
    bsc_inform_slc_date string,
    t1_pick_up_date   string,
    actual_arrival_time string,
    dock_warrant_date string,
    forwording_inform_slc_pick string,
    forwording        string,
    into_inventory_date string,
    update_date       string,
    shipment_internal_number string,
    master_bill_no    string,
    house_waybill_no  string,
    import_export_flag string,
    shipment_type     string,
    emergency_signs   string,
    merchandiser      string,
    voucher_maker     string,
    abnormal_causes1  string,
    abnormal_causes2  string,
    inspection_mark1  string,
    inspection_mark2  string,
    inspection_mark3  string,
    remark            string,
    quantity          string,
    gross_weight      string,
    forwarder_service_level string,
    department        string,
    country_area      string,
    transportation_type string,
    customs_supervision_certificate string,
    commodity_inspection_demand string,
    customized_certificate string,
    etd               string,
    eta               string,
    revise_etd        string,
    revise_eta        string,
    commodity_inspection string,
    customs_inspection string,
    declaration_completion_date string
) comment '进出口inbound_tracking'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_shipment_status_inbound_tracking/';

-- Hive SQL
-- Function： SO 客户签收信息 （ODS 层）
-- History: 
-- 2021-06-08    Donny   v1.0    init

drop table if exists ods_so_dn_receiving_confirmation;
create external table ods_so_dn_receiving_confirmation
(
    update_date             string comment 'APP_OPS updated date',
    delivery_no             string comment 'DN number',
    first_confirmation_date string comment 'confirmation date of first batch',
    last_confirmation_date  string comment 'confirmation date of last batch ' --used this date for SO dn
) comment 'SO DN Receiving Confirmation Date'
partitioned by (dt string) 
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_so_dn_receiving_confirmation/';

-- Hive SQL
-- Function： 销售订单的发票信息
-- History: 
--  2021-05-21    Donny   v1.0    init

drop table if exists ods_so_invoice;
create external table ods_so_invoice
(
       bill_id           string,
       item_no           bigint,
       accounting_no     string,
       bill_date         string,
       bill_type         string,
       sales_id          string,
       order_reason      string,
       item_category     string,
       purchase_order    string,
       material          string,
       profit_center     string,
       batch             string,
       bill_qty          bigint,
       net_amount        decimal(18,2),
       currency          string,
       payer             string,
       sold_to_pt        string,
       customer_name     string,
       classification    string,
       city              string,
       sales_rep_id      string,
       name3             string,
       desc1             string,
       desc2             string,
       sale              string,
       manufactory_date  string,
       expired_date      string,
       sales_line        bigint,
       delivery          string,
       ship_to           string,
       stock_location_pt string,
       stock_location_nm string,
       tax_amount        decimal(18,2),
       tax_rate          decimal(18,2),
       sales_type        string
) comment 'SO Invoice'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_so_invoice/';

-- Hive SQL
-- Function： location主数据 （ODS 层）
-- History: 
-- 2021-05-08    Donny   v1.0    draft

drop table if exists ods_storage_location_master;
create external table ods_storage_location_master
(
    d_plant           string,
    plant_name        string,
    location_id       string,
    location_status   string,
    storage_location  string,
    storage_definition string
) comment 'Location主数据'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_storage_location_master/';

-- Hive SQL
-- Function： Sub BU（ODS 层）
-- History: 
-- 2021-06-01    Donny   v1.0    init
-- 2021-06-01    Donny   v1.1    update the table name

drop table if exists ods_sub_bu;
create external table ods_sub_bu
(
    bu                      string comment 'master BU', 
    cust_business_type      string comment 'Customer type of business', 
    source_field            string comment 'Customer Group:PL1, PL2, PL3, Level1, Level2',
    sub_bu                  string comment 'Sub BU'
) comment 'Sub BU'
row format delimited fields terminated by '\t' 
location '/bsc/opsdw/ods/ods_sub_bu/';

-- Hive SQL
-- Function： ODS 发货单对应表
-- History: 
--  2021-05-08    Donny   v1.0    draft

drop table if exists ods_work_order;
create external table ods_work_order
(
    update_dt         string comment '更新时间',
    plant             string comment '本地化工单创建仓库',
    commercial_invoice_no string comment '商业发票',
    sap_delivery_no   string comment '发货单编号',
    work_order_no     string comment '工单编号',
    create_dt         string comment '工单创建日期',
    create_by         string comment '工单创建人',
    work_order_status string comment '工单状态',
    start_dt          string comment '工单执行开始日期',
    started_by        string comment '工单执行人',
    complete_dt       string comment '工单完成日期',
    release_dt        string comment '工单质量放行日期',
    release_by        string comment '工单质量放行人',
    line_no           string comment '工单行号',
    material          string comment '产品代码',
    batch             string comment '批次',
    current_qty       string comment '计划本地化数量',
    processed_qty     string comment '执行数量',
    qr_code           string comment 'QR Code',
    release_qty       string comment '放行数量'
) comment '本地化工单'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_work_order/';

-- Hive SQL
-- Function： Workorder and QR code mapping（ODS 层）
-- History: 
-- 2021-06-23    Donny   v1.0    init

drop table if exists ods_work_order_qr_code_mapping;
create external table ods_work_order_qr_code_mapping
(
    plant_id            string comment 'Plant Id', 
    work_order_no       string comment 'Work Order Number', 
    material            string comment 'Material',
    batch               string comment 'Batch Number', 
    qr_code             string comment 'QR Code',
    dn_no               string comment 'Related DN number'
) comment 'Work order and QR code Mapping'
row format delimited fields terminated by ',' 
location '/bsc/opsdw/ods/ods_work_order_qr_code_mapping/';


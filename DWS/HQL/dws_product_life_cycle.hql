-- Hive SQL
-- Function： 产品的生命周期
---行数据表示：一个产品一个批号的生命周期
-- History: 
-- 2021-06-15    Donny   v1.0    init

drop table if exists dws_product_life_cycle_daily_trans;
create external table dws_product_life_cycle_daily_trans
(
     material                               string comment 'STO' 
    ,batch                                  string comment 'DN'
    ,is_transfer_whs                        string comment 'Pick up plant:D838'
    ,division                               string comment 'Division'
    ,sub_division                           string comment 'Sub Division'
    ,product_line1                          string comment 'product line 1'
    ,product_line2                          string comment 'product line 2'
    ,product_line3                          string comment 'product line 3'
    ,product_line4                          string comment 'product line 4'
    ,product_line5                          string comment 'product line 5'
    ,item_type                              string comment 'Item type'
    ,so_customer_code                       string comment 'so customer code'
    ,cust_level1                            string comment 'cust level1'
    ,cust_level2                            string comment 'cust level2'
    ,cust_level3                            string comment 'cust level3'
    ,cust_level4                            string comment 'cust level4'
    ,import_dn_created_datetime             string comment 'DN created date' --DN创建时间 
    ,import_pgi                             string comment 'STO DN PGI' --T1 发货过账时间 PGI
    ,import_pick_up_date                    string comment 't1 pick up date' --T1实际提货时间
    ,import_invoice_receiving_date          string comment 'receving mail date' --预报时间
    ,import_actual_arrival_time             string comment 'Actul arrival time' -- 到港时间
    ,import_dock_warrant_date               string comment 'Manifests started time' -- 舱单开始时间
    ,import_declaration_start_date          string comment 'Declaration started date' --报关开始时间
    ,import_declaration_completion_date     string comment 'Declaration completion date' --报关完成日期
    ,import_into_inventory_date             string comment '实际到仓时间'
    ,import_migo                            string comment '进口Migo时间' --DN单实际收货时间
    ,wo_created_dt                          string comment '工单创建时间'
    ,wo_completed_dt                        string comment '本地化结束的时间'
    ,wo_release_dt                          string comment 'QA 工单检验完成时间'
    ,wo_internal_putway                     string comment '本仓上架时间Putaway'
    ,domestic_sto_create_dt                 string comment '国内转仓STO 创建时间'
    ,domestic_dn_create_dt                  string comment '国内转仓STO 创建时间'
    ,domestic_pgi                           string comment '国内转仓PGI'
    ,domestic_migo                          string comment '国内转仓DN MIGO'
    ,domestic_putaway                       string comment '国内转仓Putaway'
    ,so_create_dt                           string comment 'SO 创建时间'
    ,so_dn_create_dt                        string comment 'SO DN 创建时间'
    ,so_dn_pgi                              string comment 'SO PGI 时间'
    ,so_customer_receive_dt                 string comment 'SO 客户签收时间'
    ,international_trans_leadtime           bigint comment 'International Trans Leadtime' -- 到港时间 - PGI
    ,imported_record_leadtime               bigint comment '进境备案时间' -- 报关完成 - 仓单开始时间
    ,goods_imported_leadtime                bigint comment '货物进口时间' --MIGO时间-PGI时间
    ,wo_completed_leadtime                  bigint comment '本地化结束的leadtime' 
    ,wo_internal_putway_leadtime            bigint comment '本仓的上架leadtime'
    ,domestic_putaway_leadtime              bigint comment '国内转仓Putaway leadtime'
    ,so_order_operation_leadtime            bigint comment 'SO 订单处理leadtime'
    ,so_dn_operation_leadtime               bigint comment '发货处理Leadtime' 
    ,so_dn_ship_leadtime                    bigint comment '货物运输Leadtime' 
    ,goods_sales_leadtime                   bigint comment '货物销售Leadtime' 
    ,goods_putaway_leadtime                 bigint comment '货物上架Leadtime'
    ,goods_e2e_leadtime                     bigint comment '货物e2e Leadtime'
    ,goods_onhand_leadtime                  bigint comment '货物在库Leadtime'
) comment 'Product Life Cycle Daily Transation'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_product_life_cycle_daily_trans/'
tblproperties ("parquet.compression"="lzo");
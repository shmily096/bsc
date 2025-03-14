# BSC Shell Script

## 1. DWS

### 1.1 DSR

> load  dsr  relation data into DWS layer 

#### 1.1.1  Script list

脚本列表与关联表

| ID   | Name                                      | DWS Table                       | DWD Table | Comments           |
| ---- | ----------------------------------------- | ------------------------------- | --------- | ------------------ |
| #1   | dwd_to_dws_dsr_daily_trans.sh             | dws_dsr_daily_trans             |           | Depends On: #2, #3 |
| #2   | dwd_to_dws_dsr_fulfill_daily_trans.sh     | dws_dsr_fulfill_daily_trans     |           |                    |
| #3   | dwd_to_dws_dsr_ship_daily_trans.sh        | dws_dsr_ship_daily_trans        |           |                    |
| #4   | dwd_to_dws_dsr_dealer_daily_transation.sh | dws_dsr_dealer_daily_transation |           |                    |

### 1.2 Lead Time

#### 1.2.1 Script List

| ID   | Name                                                   | DWS Table                                    | Comments                  |
| ---- | ------------------------------------------------------ | -------------------------------------------- | ------------------------- |
| #1   | dwd_to_dws_plc_so_sto_wo_daily_trans.sh                | dws_so_sto_wo_daily_trans                    |                           |
| #2   | dwd_to_dws_plc_wo_daily_trans.sh                       | dws_plc_wo_daily_trans                       | Depends On:one:           |
| #3   | dwd_to_dws_plc_so_daily_trans.sh                       | dws_plc_so_daily_trans                       | Depends On:one:           |
| #4   | dwd_to_dws_plc_import_export_daily_trans.sh            | dws_plc_import_export_daily_trans            | Depends On:one:           |
| #5   | dwd_to_dws_plc_domestic_sto_daily_trans.sh             | dws_plc_domestic_sto_daily_trans             | Depends On:one:           |
| #6   | dwd_to_dws_import_export_daily_trans.sh                | dws_import_export_daily_trans                |                           |
| #7   | dwd_to_dws_product_putaway_leadtime_slc_daily_trans.sh | dws_product_putaway_leadtime_slc_daily_trans | Depends On:one:#1,2,3,4,5 |
| #8   | dwd_to_dws_product_putaway_leadtime_yh_daily_trans.sh  | dws_product_putaway_leadtime_yh_daily_trans  | Depends On:one:#1,2,3,4,5 |
| #9   | dwd_to_dws_order_proce_custlev3_daily_trans.sh         | dws_order_proce_custlev3_daily_trans         |                           |
| #10  | dwd_to_dws_order_proce_division_daily_trans.sh         | dws_order_proce_division_daily_trans         |                           |
| #11  | dwd_to_dws_order_proce_tob_daily_trans.sh              | dws_order_proce_tob_daily_trans              |                           |
| #12  | dwd_to_dws_plant_delivery_processing_daily_trans.sh    | dws_plant_delivery_processing_daily_trans    |                           |
| #13  | dwd_to_dws_t1_plant_trans.sh                           | dws_t1_plant_daily_transation                | Depends on: #3, #4        |
| #14  | dwd_to_dws_forwarder_daily_trans.sh                    | dws_forwarder_daily_trans                    | Depends on: #3, #4        |
| #15  | dwd_to_dws_lifecycle_leadtime_slc_daily_trans.sh       | dws_lifecycle_leadtime_SLC_daily_trans       | Depends on: #2,#3, #4     |
| #16  | dwd_to_dws_lifecycle_leadtime_yh_daily_trans.sh        | dws_lifecycle_leadtime_YH_daily_trans        | Depends on: #2,#3, #4,#5  |
| #17  | dwd_to_dws_sale_order_leadtime_daily_trans.sh          | dws_sale_order_leadtime_daily_trans          |                           |



## 2 DWT

### 2.1 DSR

#### 2.1.1 Script list

| ID   | Name                                   | DWT Table                    | DWS Table | DWD Table | Comments |
| ---- | -------------------------------------- | ---------------------------- | --------- | --------- | -------- |
| #1   | dws_to_dwt_dsr_dealer_quarter_trans.sh | dwt_dsr_dealer_quarter_trans |           |           |          |

### 2.2 LeadTime

#### 2.2.1 Scripts List

| ID   | Name                                            | DWT Table                                    | Comments            |
| ---- | ----------------------------------------------- | -------------------------------------------- | ------------------- |
| #1   | dws_to_dwt_forwarder_topic.sh                   | dwt_forwarder_topic                          |                     |
| #2   | dws_to_dwt_imported_topic.sh                    | dwt_imported_topic                           |                     |
| #3   | dws_to_dwt_order_proce_custlev3_topic.sh        | dwt_order_proce_custlev3_topic               |                     |
| #4   | dws_to_dwt_order_proce_division_topic.sh        | dwt_order_proce_division_topic               |                     |
| #5   | dws_to_dwt_order_proce_tob_topic.sh             | dwt_order_proce_tob_topic                    |                     |
| #6   | dws_to_dwt_plant_delivery_processing_topic.sh   | dwt_plant_delivery_processing_topic          |                     |
| #7   | dws_to_dwt_plant_topic.sh                       | dwt_plant_topic                              |                     |
| #8   | dws_dwt_product_putaway_leadtime_slc_topic.sh   | dwt_product_putaway_leadtime_slc_topic       |                     |
| #9   | dws_to_dwt_product_putaway_leadtime_yh_topic.sh | dwt_product_putaway_leadtime_yh_topic        |                     |
| #10  | dws_to_dwt_sale_order_leadtime_topic.sh         | dwt_sale_order_leadtime_topic                |                     |
| #11  | dws_to_dwt_lifecycle_slcyh_summarize_topic.sh   | dwt_lifecycle_leadtime_slcyh_summarize_topic | depends on #12, #13 |
| #12  | dws_to_dwt_lifecycle_slc_summarize_topic.sh     | dwt_lifecycle_leadtime_slc_summarize_topic   | depends on #15      |
| #13  | dws_to_dwt_lifecycle_yh_summarize_topic.sh      | dwt_lifecycle_leadtime_yh_summarize_topic    | depends on #14      |
| #14  | dws_to_dwt_lifecycle_leadtime_yh_topic.sh       | dwt_lifecycle_leadtime_YH_topic              |                     |
| #15  | dws_to_dwt_lifecycle_leadtime_slc_topic.sh      | dwt_lifecycle_leadtime_SLC_topic             |                     |
| #16  | dws_to_dwt_lifecycle_leadtime_division_slcyh.sh | dwt_lifecycle_leadtime_division_slcyh_topic  |                     |

# 3 ADS

## 3.1 LeadTime

| ID   | Name                                      | ADS Table                              | DWT Table | DWS Table |
| ---- | ----------------------------------------- | -------------------------------------- | --------- | --------- |
| #1   | ads_imported_ratio.sh                     | ads_imported_ratio                     |           |           |
| #2   | ads_product_putaway_leadtime_slc_ratio.sh | ads_product_putaway_leadtime_slc_ratio |           |           |
| #3   | ads_product_putaway_leadtime_yh_ratio.sh  | ads_product_putaway_leadtime_yh_ratio  |           |           |
| #4   | ads_sale_order_leadtime_ratio.sh          | ads_sale_order_leadtime_ratio          |           |           |
| #5   | ads_lifecycle_leadtime_slcyh_ratio.sh     | ads_lifecycle_leadtime_slcyh_ratio     |           |           |


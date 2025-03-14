#!/bin/bash
# Function:
#   将查询的结果导出至HDFS上
# History:
# 2021-11-08    Donny   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# 默认取当前时间 
if [ -n "$2" ] ;then 
    yester_day=$2
else
    #默认取昨天的日期
    yester_day=$(date -d '-1 day' +%F)
fi
yester_year=${yester_day:0:4}
yester_month=${yester_day:0:7}-01
this_month=`date -d "${yester_day}" +%Y-%m-01`
this_year=`date -d "${yester_day}" +%Y-01-01`
sync_date=$(date  +%F)
mm=10#${yester_day:5:2}
if ((mm >= 1 ))&&((mm <= 3 ));then
    q_s_date=${yester_day:0:4}-01-01
    q_s_mon=01
    q_l_date=${yester_day:0:4}-02-01
    q_l_mon=02
    q_e_date=${yester_day:0:4}-03-31
    q_e_mon=03
elif ((mm >= 4 ))&&((mm <= 6 ));then
    q_s_date=${yester_day:0:4}-04-01
    q_s_mon=04
    q_l_date=${yester_day:0:4}-05-01
    q_l_mon=05
    q_e_date=${yester_day:0:4}-06-30
    q_e_mon=06
elif ((mm >= 7 ))&&((mm <= 9 ));then
    q_s_date=${yester_day:0:4}-07-01
    q_s_mon=07
    q_l_date=${yester_day:0:4}-08-01
    q_l_mon=08
    q_e_date=${yester_day:0:4}-09-30
    q_e_mon=09
elif ((mm >= 10 ))&&((mm <= 12 ));then
    q_s_date=${yester_day:0:4}-10-01
    q_s_mon=10
    q_l_date=${yester_day:0:4}-11-01
    q_l_mon=11
    q_e_date=${yester_day:0:4}-12-31
    q_e_mon=12
fi
echo "End loading master data on $q_s_mon:$q_e_mon:$yester_month:$yester_day .................."

dwd_dim_all_kpi_maxdt=`hdfs dfs -ls '/bsc/opsdw/dwd/dwd_dim_all_kpi' | tail -n 1 | awk -F'=' '{print $NF}'`
dwd_outbound_distribution_maxdt=`hdfs dfs -ls '/bsc/opsdw/dwd/dwd_outbound_distribution' | tail -n 1 | awk -F'=' '{print $NF}'`
# 2 设置导出的 HDFS路径
export_dir='/bsc/opsdw/export/dws_dsr_billed_daily'
export_dira='/bsc/opsdw/export/dws_dsr_cr_daily'

echo "start exporting dws_dsr_billed_daily data into HDFS on $yester_day .................."

# 1 Hive SQL string
str_sql="
use ${target_db_name};
insert OVERWRITE directory '${export_dir}'
row format delimited fields terminated by '\t' 
stored as textfile
select 
so_no, net_billed, bill_date, material, billed_rebate, division, sub_division, upn_del_flag, cust_del_flag, orderreason_del_flag, billtype_del_flag, customer_code, dt_year, dt_month, bill_qty, dt
from opsdw.dws_dsr_billed_daily 
--where dt between '$q_s_date' and '$q_e_date'
where dt_year='$yester_year' and  dt_month between '$q_s_mon'and '$q_e_mon'
;
"
cr_sql="
use ${target_db_name};
insert OVERWRITE directory '${export_dira}'
row format delimited fields terminated by '\t' 
stored as textfile
SELECT so_no, bill_date, material, net_cr, division_display_name, upn_del_flag, cust_del_flag, orderreason_del_flag, billtype_del_flag, dt_year, dt_month, cr_qty, customer_code, dt
FROM opsdw.dws_dsr_cr_daily
--where dt>=date_add('$yester_day',-31)
where dt_year='$yester_year' and  dt_month between '$q_s_mon'and '$q_e_mon';
"
dwd_dim_customer_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwd_dim_customer'
row format delimited fields terminated by '\t' 
stored as textfile
select * 
from opsdw.dwd_dim_customer
where dt='$sync_date';
"
dwd_dim_material_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwd_dim_material'
row format delimited fields terminated by '\t' 
stored as textfile
select * 
from opsdw.dwd_dim_material
where dt='$sync_date';
"
dwd_dim_material_adm_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwd_dim_material_adm'
row format delimited fields terminated by '\t' 
stored as textfile
select * 
from opsdw.dwd_dim_material
where dt='2023-07-01';
"
dws_dsr_dned_daily_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_dsr_dned_daily'
row format delimited fields terminated by '\t' 
stored as textfile
select * 
from opsdw.dws_dsr_dned_daily
where dt_year='$yester_year' and  dt_month between '$q_s_mon'and '$q_e_mon'
;
"
dws_dsr_fulfill_daily_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_dsr_fulfill_daily'
row format delimited fields terminated by '\t' 
stored as textfile
select * 
from opsdw.dws_dsr_fulfill_daily
---WHERE dt_year >='$yester_year'and dt_month >='04'
where dt_year='$yester_year' and  dt_month between '$q_s_mon'and '$q_e_mon'
;
"
dws_dsr_dealer_daily_transation_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_dsr_dealer_daily_transation'
row format delimited fields terminated by '\t' 
stored as textfile
SELECT division, sub_division, customer_code, cust_level3, bill_month, bill_year, bill_date, quarter, dealer_name, parent_dealer_name, dealer_complete, dealer_mon_target, dealer_mon_total_target, dt_year, dt_month, bill_qty, dt
from opsdw.dws_dsr_dealer_daily_transation
--where dt>=date_add('$yester_day',-31)
where dt_year='$yester_year' and  dt_month between '$q_s_mon'and '$q_e_mon'
;
"
#目前是全表更新后续可改为按月更新 先推4月以后的
dwt_dsr_dealer_topic_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwt_dsr_dealer_topic'
row format delimited fields terminated by '\t' 
stored as textfile
select * 
from opsdw.dwt_dsr_dealer_topic
where dt_year='$yester_year' and  dt_month between cast('$q_s_mon' as int) and cast('$q_e_mon' as int)
;
"
#目前是全表更新后续可改为按月更新 先推4月以后的
dwt_dsr_topic_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwt_dsr_topic'
row format delimited fields terminated by '\t' 
stored as textfile
select * 
from opsdw.dwt_dsr_topic
where dt_year='$yester_year' and  dt_month between cast('$q_s_mon' as int) and cast('$q_e_mon' as int)
;
"
#推>= 季度第一天(date(-30)) trunc(date_sub('$yester_day', 30),'Q')
dws_kpi_sales_waybill_timi="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_kpi_sales_waybill_timi'
row format delimited fields terminated by '\t' 
stored as textfile
SELECT so_no, delivery_id, created_datetime, actual_gi_date, receiving_confirmation_date, ship_to_address, pick_location_id, plant, supplier, material, batch, qty, lt_cd_hr, no_work_hr, lt_dw_hr, kpi_no, dt_yearmon, dt_week, dt
from opsdw.dws_kpi_sales_waybill_timi
where dt>=trunc(date_sub('$yester_day', 30),'Q')
--date_add('$yester_day',-124)
--where substr(dt_yearmon,0,4)='$yester_year' and  substr(dt_yearmon,6,2) between '$q_s_mon'and '$q_e_mon'
--date_format(add_months('$this_month',-3),'yyyy-MM')
;
"
#推>= 季度第一天(date(-30)) trunc(date_sub('$yester_day', 30),'Q')
dws_kpi_monthly_isolate_stock="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_kpi_monthly_isolate_stock'
row format delimited fields terminated by '\t' 
stored as textfile
select name, plant, location, status, material, batch, qty, flag, supplier, kpi_code, dt
from opsdw.dws_kpi_monthly_isolate_stock
where dt>=trunc(date_sub('$yester_day', 30),'Q')
--date_add('$yester_day',-124)
--where dt between '$q_s_date' and '$q_e_date'
;
"
#推>= 季度第一天(date(-30)) trunc(date_sub('$yester_day', 30),'Q')
dws_kpi_zc_timi="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_kpi_zc_timi'
row format delimited fields terminated by '\t' 
stored as textfile
SELECT sto_no, delivery_no, reference_dn_number, create_datetime, 
actual_migo_date, pgi_datetime, delivery_mode, dn_status, ship_from_location, 
ship_from_plant, coalesce(from_supplier  ,'SLC') from_supplier, to_supplier, ship_to_plant, ship_to_location,
 material, batch, qty, lt_cd_hr, no_work_hr, lt_dw_hr, kpi_no, dt_yearmon, dt_week, dt
from opsdw.dws_kpi_zc_timi
where dt>=trunc(date_sub('$yester_day', 30),'Q')
--date_add('$yester_day',-124)
--where substr(dt_yearmon,0,4)='$yester_year' and  substr(dt_yearmon,6,2) between '$q_s_mon'and '$q_e_mon'
;
"
#推4月以后的数据 >= 季度第一天（date(-30)）
dws_kpi_stock_putaway_time="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_kpi_stock_putaway_time'
row format delimited fields terminated by '\001' 
stored as textfile
select plant, stock_location, delivery_plant,default_location,
coalesce(supplier,case when plant='D835' THEN 'SLC'
	WHEN plant='D838'THEN 'YH'
	WHEN plant='D836' THEN 'CD-NonBounded'
	WHEN plant='D837'THEN 'TJ DC'
end) supplier,
material, batch, delivery_no, 
cast(qty as int)as qty, qty_gap, start_time, end_time, 
cast(processtime_cd as int)as processtime_cd,
 cast(no_work_hr as int)as no_work_hr, 
  cast(process_wd_m as int)as process_wd_m, 
 process_category, kpicode, dt 
from opsdw.dws_kpi_stock_putaway_time
where dt>=trunc(date_sub('$yester_day', 30),'Q')
---date_add('$yester_day',-124)
--where dt>='2022-04-01'
--where dt between '$q_s_date' and '$q_e_date'
;
"
#每次全量更新取最新分区数据
dwd_dim_kpi="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwd_dim_kpi'
row format delimited fields terminated by '\001' 
stored as textfile
select * 
from opsdw.dwd_dim_kpi
where dt=(select max(dt) from opsdw.dwd_dim_kpi)
;
"

#推>= 季度第一天(date(-30)) trunc(date_sub('$yester_day', 30),'Q')
dws_ie_kpi="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_ie_kpi'
row format delimited fields terminated by '\001' 
stored as textfile
select 
kpicode, plant, housewaybillno, forwording, 
case when kpicode='IE001' then forwording 
	when kpicode='IE002' then 'SLC'
	else supplier end as supplier,
 need_calcaulated, t1pickupdate, dockwarrantdate, 
 intoinventorydate, intoinventorydate_kpi, migo_date, 
 shipment_number, shipmenttype, invoice, upn, qty, 
 lt_day_cd, lt_day_wd,
  dt 
from opsdw.dws_ie_kpi
where year_mon>=substr(trunc(date_sub('$yester_day', 30),'Q'),1,7)
--substr('$q_s_date',1,7)
--where dt between '$q_s_date' and '$q_e_date'
;
"
#推>= 季度第一天(date(-30)) trunc(date_sub('$yester_day', 30),'Q')
dws_kpi_cc_so_delivery="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_kpi_cc_so_delivery'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT material, batch, so_no, delivery_id, plant, pick_location_id, qty, line, this_mon_flag, flag, dt
from opsdw.dws_kpi_cc_so_delivery
where dt>=trunc(date_sub('$yester_day', 30),'Q')
--date_add('$yester_day',-124)
--where dt between '$q_s_date' and '$q_e_date'
;
"
#推昨天对应当季度的数据
dws_dsr_fulfill_monthly="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_dsr_fulfill_monthly'
row format delimited fields terminated by '\001' 
stored as textfile
select * 
from opsdw.dws_dsr_fulfill_monthly
 where dt_year='$yester_year'
 and  dt_month between cast('$q_s_mon' as int) and cast('$q_e_mon' as int)
;
"
#推>= 季度第一天(date(-30)) trunc(date_sub('$yester_day', 30),'Q')
dws_kpi_sto_migo2pgi="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_kpi_sto_migo2pgi'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT plant, upn, bt, inventorystatus, sapinbounddn, workorderno, batch, inbounddn, outbounddn, supplier, id, inboundtime, outboundtime, localizationfinishtime, qty, lt_cd_hr, no_work_hr, lt_dw_hr, kpi_no,is_pacemaker, distribution_properties,  dt
FROM opsdw.dws_kpi_sto_migo2pgi
where dt>=trunc(date_sub('$yester_day', 30),'Q')
--date_add('$yester_day',-124);
--where dt between '$q_s_date' and '$q_e_date'
;
"
dws_iekpi_e2e="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_iekpi_e2e'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT 
	outbound_yr,
	pickupdocument_no,
	commericalinvoice,
	inbound_invoice,
	emergencysigns,
	forwarding,
	arrivalgoods_type,
	shipfrom_country,
	preinspection_flag,
	bscinformslcdate,
	t1pickupdate,
	etd,
	eta,
	reviseetd,
	reviseeta,
	actualarrivaltime,
	forwordinginformslcpick,
	dockwarrantdate,
	intoinventorydate,
	inbounddeclaration_startdate,
	inbounddeclaration_finishdate,
	picturetaken_date,
	inspection_appointmentdate,
	inspection_finishdate,
	inbound_pics,
	inbound_mon,
	inbound_yr,
	outbound_biz_no,
	declaration_itemname, 
	outbound_commericalinvoice,
	outbound_invoice,
	document_finishpreparationdate,
	customrelease_1,
	commodityinspection_date,
	chineselabelpicturereceiveddate,
	customrelease_2,
	testscheduled_date,
	actualtest_date,
	ciq_signcompletiondate,
	taxpayment_applicationdate,
	taxpayment_completiondate,
	declaration_completiondate,
	is_excluded,
	is_malaysia,
	abnormal_reason,
	status,
	is_valid,
	category_code,
	distribution_status,
	outbound_pieces,
	destination_wh,
	airport,
	outbound_mon,
	dock_invent_cd,
	dock_invent_holiday, 
	dock_invent_wd,
	invent_cust1_cd,
	invent_cust1_holiday,
	invent_cust1_wd,
	cust1_chinesepicture_cd,
	cust1_chinesepicture_holiday,
	cust1_chinesepicture_wd,
	chinesepicture_commodity_cd,
	chinesepicture_commodity_holiday,
	chinesepicture_commodity_wd,
	commod_act_cust2_cd, 
	commod_act_cust2_holiday,
	commod_act_cust2_wd,
	act_ciq_cd, 
	act_ciq_holiday,
	act_ciq_wd,
	start_end_cd,
	start_end_holiday,
	start_end_wd,
	coodraft_receiveddate,
	coocertificate_receiveddate,
	jsons
FROM opsdw.dws_iekpi_e2e_aws
where outbound_yr>=substr('$sync_date',1,4) and outbound_commericalinvoice not like '%&'
;
"
dwd_dim_all_kpi="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwd_dim_all_kpi'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT 
	kpicode, func, stream, sub_stream, category, supplier, index, unit, formula, index_level, criteria, target, vaild_from, vaild_to, dt
	kpicode, func, stream, sub_stream, category, supplier, index, unit, formula, index_level, criteria, target, vaild_from, vaild_to, dt
FROM opsdw.dwd_dim_all_kpi
where dt ='$dwd_dim_all_kpi_maxdt'
;
"
dwd_outbound_distribution="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwd_outbound_distribution'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT 
	no,  ---序号
	signal_no, --指令单号
	biz_no, --业务编号
	css_no, --报检单号
	distributedoc_receivedate,   --收到分拨清单时间
	get_certificate, --是否出证 
	endorselist_receiveddate, --核注清单收到时间  
	customclearance_preparationfinishdate,--报关准备完成时间
	taxpayment_applicationdate,--付税申请时间  
	taxpayment_completiondate,  --付税完成时间
	customclearance_date, --清关完成时间
	remark, --备注                                 
	is_excluded, --是否排除
	get_certificatelist, -- 是否拿到出证清单                                      
	status, --状态
	distirib_custom_cd,
	distirib_custom_wd,
	dt
FROM opsdw.dwd_outbound_distribution
where dt='$dwd_outbound_distribution_maxdt'
;
"
dwd_dim_dsr_le_srr_test="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwd_dim_dsr_le_srr_test'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT 
	division, 
	year, 
	month, 
	le_cny, 
	le_usd, 
	srr_cny,
	srr_usd,
	srr_version,
    dp_prder_project,
    sales_ro_comment,
    dt
	FROM opsdw.dwd_dim_dsr_le_srr
where dt =(select max(dt) from opsdw.dwd_dim_dsr_le_srr where dt>='$yester_month')
;
"
dwd_iekpi_e2e_tj="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwd_iekpi_e2e_tj'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT 
	customdockdate, 
	dockwarrantdate,
	inbounddeclaration_finishdate,
	intoinventorydate,
	invoice,
	pickupdocument_no,
	commercialinvoice,
	commodityinspection_outboundcheck,
	dock_customdock_cd,
	dock_customdock_holiday,
	dock_customdock_wd,
	declaration_customdock_cd, 
	declaration_customdock_holiday,
	declaration_customdock_wd,
	invent_customdock_cd, 
	invent_customdock_holiday,
	invent_customdock_wd,
	outbound_invent_cd, 
	outbound_invent_holiday,
	outbound_invent_wd,
	jsons,
	outbound_yr
FROM opsdw.dwd_iekpi_e2e_tj_aws
where outbound_yr='$yester_year'
;
"
dws_kpi_dwd_fact_import_export_dn_info_pdt="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_kpi_dwd_fact_import_export_dn_info_pdt'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT 
sto_no, 
delivery_no, 
reference_dn_no, 
order_status, 
ship_from_plant,
 ship_to_plant,
  total_qty, 
  actual_good_issue_datetime, 
  actual_migo_datetime, 
  fin_dim_id, 
  item_business_group,
   lt_cd_hr, 
   lt_dw_hr,
    kpi_no, 
	dt_yearmon, 
	dt
FROM opsdw.dws_kpi_dwd_fact_import_export_dn_info_pdt
where dt>=date_add('$yester_day',-124)
;
"
dwt_networkswitch_qtycompare="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwt_networkswitch_qtycompare'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT 
	*
    FROM opsdw.dwt_networkswitch_qtycompare
where dt ='$sync_date'
;
"
dwt_networkswitch_upn_list="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwt_networkswitch_upn_list'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT 
	*
    FROM opsdw.dwt_networkswitch_upn_list
where dt =(select max(dt) from opsdw.dwt_networkswitch_upn_list)
;
"
dwt_finance_monthly_cs_freight_ocgs_htm="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwt_finance_monthly_cs_freight_ocgs_htm'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT 
	*
    FROM opsdw.dwt_finance_monthly_cs_freight_ocgs_htm
;
"
dws_finance_upn_qty="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_finance_upn_qty'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT 
	*
    FROM opsdw.dws_finance_upn_qty
;
"
dwd_dim_cfda_upn="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwd_dim_cfda_upn'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT 
registration_no, upn, valid_fromdate, valid_enddate, dt
    FROM opsdw.dwd_dim_cfda_upn
	where dt='$sync_date'
;
"
dws_dsr_billed_cr_qty_netvalue_detail="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_dsr_billed_cr_qty_netvalue_detail'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT 
	*
    FROM opsdw.dws_dsr_billed_cr_qty_netvalue_detail
where year_mon<=(
		SELECT max(versions)
		from dwt_finance_monthly_cs_freight_ocgs_htm
		where itemcode ='S_001')
;
"
dws_finance_net_sales_cogs_gap="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_finance_net_sales_cogs_gap'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT 
	*
    FROM opsdw.dws_finance_net_sales_cogs_gap
;
"
dws_finance_cogs_std_cogs_at_standard_detail="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_finance_cogs_std_cogs_at_standard_detail'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT 
	*
    FROM opsdw.dws_finance_cogs_std_cogs_at_standard_detail
;
"
dws_finance_cogs_std_cogs_at_standard_gap="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_finance_cogs_std_cogs_at_standard_gap'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT 
	*
    FROM opsdw.dws_finance_cogs_std_cogs_at_standard_gap
;
"
dws_finance_cogs_sharing_detail="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_finance_cogs_sharing_detail'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT 
	*
    FROM opsdw.dws_finance_cogs_sharing_detail
;
"
dws_eeo_detail="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_eeo_detail'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT 
	*
    FROM opsdw.dws_eeo_detail
;
"
dws_eeo="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_eeo'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT 
	material  ,
	division_excel  ,
	currency  ,
	file_name  ,
	small_version  ,
	itemcode  ,
	versions  ,
	qty  ,
	amount  ,
	pricechange  ,
	last_amount  ,
	finall_amount  ,
	eeo_flag  ,
	months 
    FROM opsdw.dws_eeo
;
"
dws_finance_ocgs_inventory_charges_detail="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_finance_ocgs_inventory_charges_detail'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT 
	*
    FROM opsdw.dws_finance_ocgs_inventory_charges_detail
;
"
ods_aws_inbound_txn_bsc_psi="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/ods_aws_inbound_txn_bsc_psi'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT 
	*
    FROM opsdw.ods_aws_inbound_txn_bsc_psi
;
"
dwt_kpi_by_bu_detail="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwt_kpi_by_bu_detail'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT 
kpi_code, dt, plant, bu, value1, value2, flag1, flag2, flag3, year_mon
    FROM opsdw.dwt_kpi_by_bu_detail
where year_mon>=add_months('$yester_day',-12) AND year_mon >='2024-01'

;
"
# 2. 执行加载数据SQL
if [ "$1"x = "dws_dsr_billed_daily"x ];then
	echo "hdfs $1 only run"	
    echo "$str_sql"
	$hive -e "$str_sql"
	echo "hdfs  finish dws_dsr_billed_daily data into hdfs layer on ${yester_day} .................."
elif [ "$1"x = "dws_iekpi_e2e"x ];then
    echo " hdfs $1 only run"
	echo "$dws_iekpi_e2e"
	$hive -e "$dws_iekpi_e2e"
	echo "hdfs  finish dws_iekpi_e2e data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dwt_kpi_by_bu_detail"x ];then
    echo " hdfs $1 only run"
	echo "$dwt_kpi_by_bu_detail"
	$hive -e "$dwt_kpi_by_bu_detail"
	echo "hdfs  finish dwt_kpi_by_bu_detail data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "ods_aws_inbound_txn_bsc_psi"x ];then
    echo " hdfs $1 only run"
	echo "$ods_aws_inbound_txn_bsc_psi"
	$hive -e "$ods_aws_inbound_txn_bsc_psi"
	echo "hdfs  finish ods_aws_inbound_txn_bsc_psi data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dws_finance_ocgs_inventory_charges_detail"x ];then
    echo " hdfs $1 only run"
	echo "$dws_finance_ocgs_inventory_charges_detail"
	$hive -e "$dws_finance_ocgs_inventory_charges_detail"
	echo "hdfs  finish dws_finance_ocgs_inventory_charges_detail data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dws_eeo"x ];then
    echo " hdfs $1 only run"
	echo "$dws_eeo"
	$hive -e "$dws_eeo"
	echo "hdfs  finish dws_eeo data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dws_eeo_detail"x ];then
    echo " hdfs $1 only run"
	echo "$dws_eeo_detail"
	$hive -e "$dws_eeo_detail"
	echo "hdfs  finish dws_eeo_detail data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dws_finance_cogs_sharing_detail"x ];then
    echo " hdfs $1 only run"
	echo "$dws_finance_cogs_sharing_detail"
	$hive -e "$dws_finance_cogs_sharing_detail"
	echo "hdfs  finish dws_finance_cogs_sharing_detail data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dws_finance_cogs_std_cogs_at_standard_gap"x ];then
    echo " hdfs $1 only run"
	echo "$dws_finance_cogs_std_cogs_at_standard_gap"
	$hive -e "$dws_finance_cogs_std_cogs_at_standard_gap"
	echo "hdfs  finish dws_finance_cogs_std_cogs_at_standard_gap data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dws_finance_cogs_std_cogs_at_standard_detail"x ];then
    echo " hdfs $1 only run"
	echo "$dws_finance_cogs_std_cogs_at_standard_detail"
	$hive -e "$dws_finance_cogs_std_cogs_at_standard_detail"
	echo "hdfs  finish dws_finance_cogs_std_cogs_at_standard_detail data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dws_finance_net_sales_cogs_gap"x ];then
    echo " hdfs $1 only run"
	echo "$dws_finance_net_sales_cogs_gap"
	$hive -e "$dws_finance_net_sales_cogs_gap"
	echo "hdfs  finish dws_finance_net_sales_cogs_gap data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dws_dsr_billed_cr_qty_netvalue_detail"x ];then
    echo " hdfs $1 only run"
	echo "$dws_dsr_billed_cr_qty_netvalue_detail"
	$hive -e "$dws_dsr_billed_cr_qty_netvalue_detail"
	echo "hdfs  finish dws_dsr_billed_cr_qty_netvalue_detail data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dwd_dim_cfda_upn"x ];then
    echo " hdfs $1 only run"
	echo "$dwd_dim_cfda_upn"
	$hive -e "$dwd_dim_cfda_upn"
	echo "hdfs  finish dwd_dim_cfda_upn data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dws_finance_upn_qty"x ];then
    echo " hdfs $1 only run"
	echo "$dws_finance_upn_qty"
	$hive -e "$dws_finance_upn_qty"
	echo "hdfs  finish dws_finance_upn_qty data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dwt_finance_monthly_cs_freight_ocgs_htm"x ];then
    echo " hdfs $1 only run"
	echo "$dwt_finance_monthly_cs_freight_ocgs_htm"
	$hive -e "$dwt_finance_monthly_cs_freight_ocgs_htm"
	echo "hdfs  finish dwt_finance_monthly_cs_freight_ocgs_htm data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dwt_networkswitch_upn_list"x ];then
    echo " hdfs $1 only run"
	echo "$dwt_networkswitch_upn_list"
	$hive -e "$dwt_networkswitch_upn_list"
	echo "hdfs  finish dwt_networkswitch_upn_list data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dwt_networkswitch_qtycompare"x ];then
    echo " hdfs $1 only run"
	echo "$dwt_networkswitch_qtycompare"
	$hive -e "$dwt_networkswitch_qtycompare"
	echo "hdfs  finish dwt_networkswitch_qtycompare data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dws_kpi_dwd_fact_import_export_dn_info_pdt"x ];then
    echo " hdfs $1 only run"
	echo "$dws_kpi_dwd_fact_import_export_dn_info_pdt"
	$hive -e "$dws_kpi_dwd_fact_import_export_dn_info_pdt"
	echo "hdfs  finish dws_kpi_dwd_fact_import_export_dn_info_pdt data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dwd_dim_dsr_le_srr_test"x ];then
    echo " hdfs $1 only run"
	echo "$dwd_dim_dsr_le_srr_test"
	$hive -e "$dwd_dim_dsr_le_srr_test"
	echo "hdfs  finish dwd_dim_dsr_le_srr_test data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dwd_iekpi_e2e_tj"x ];then
    echo " hdfs $1 only run"
	echo "$dwd_iekpi_e2e_tj"
	$hive -e "$dwd_iekpi_e2e_tj"
	echo "hdfs  finish dwd_iekpi_e2e_tj data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dws_kpi_cc_so_delivery"x ];then
    echo " hdfs $1 only run"
	echo "$dws_kpi_cc_so_delivery"
	$hive -e "$dws_kpi_cc_so_delivery"
	echo "hdfs  finish dws_kpi_cc_so_delivery data into hdfs layer on ${yester_day} .................."
elif [ "$1"x = "dws_kpi_sto_migo2pgi"x ];then
    echo " hdfs $1 only run"
	echo "$dws_kpi_sto_migo2pgi"
	$hive -e "$dws_kpi_sto_migo2pgi"
	echo "hdfs  finish dws_kpi_sto_migo2pgi data into hdfs layer on ${yester_day} .................."
elif [ "$1"x = "dws_dsr_fulfill_monthly"x ];then
    echo " hdfs $1 only run"
	echo "$dws_dsr_fulfill_monthly"
	$hive -e "$dws_dsr_fulfill_monthly"
	echo "hdfs  finish dws_dsr_fulfill_monthly data into hdfs layer on ${yester_day} .................."
elif [ "$1"x = "dws_dsr_cr_daily"x ];then
    echo " hdfs $1 only run"
	echo "$cr_sql"
	$hive -e "$cr_sql"
	echo "hdfs  finish dws_dsr_cr_daily data into hdfs layer on ${yester_day} .................."
elif [ "$1"x = "dwd_dim_customer"x ];then
    echo " hdfs $1 only run"
	echo "$dwd_dim_customer_sql"
	$hive -e "$dwd_dim_customer_sql"
	echo "hdfs  finish dwd_dim_customer data into hdfs layer on ${yester_day} .................."
elif [ "$1"x = "dwd_dim_material"x ];then
    echo " hdfs $1 only run"
	echo "$dwd_dim_material_sql"
	$hive -e "$dwd_dim_material_sql"
	echo "hdfs  finish dwd_dim_material data into hdfs layer on ${yester_day} .................."
elif [ "$1"x = "dwd_dim_material_adm"x ];then
    echo " hdfs $1 only run"
	echo "$dwd_dim_material_adm_sql"
	$hive -e "$dwd_dim_material_adm_sql"
	echo "hdfs  finish dwd_dim_material_adm data into hdfs layer on ${yester_day} .................."
elif [ "$1"x = "dws_dsr_dned_daily"x ];then
    echo " hdfs $1 only run"
	echo "$dws_dsr_dned_daily_sql"
	$hive -e "$dws_dsr_dned_daily_sql"
	echo "hdfs  finish dws_dsr_dned_daily_sql data into hdfs layer on ${yester_day} .................."
elif [ "$1"x = "dws_dsr_fulfill_daily"x ];then
    echo " hdfs $1 only run"
	echo "$dws_dsr_fulfill_daily_sql"
	$hive -e "$dws_dsr_fulfill_daily_sql"
	echo "hdfs  finish dws_dsr_fulfill_daily data into hdfs layer on ${yester_day} .................."
elif [ "$1"x = "dws_dsr_dealer_daily_transation"x ];then
    echo " hdfs $1 only run"
	echo "$dws_dsr_dealer_daily_transation_sql"
	$hive -e "$dws_dsr_dealer_daily_transation_sql"
	echo "hdfs  finish dws_dsr_dealer_daily_transation_sql data into hdfs layer on ${yester_day} .................."
elif [ "$1"x = "dwt_dsr_dealer_topic"x ];then
    echo " hdfs $1 only run"
	echo "$dwt_dsr_dealer_topic_sql"
	$hive -e "$dwt_dsr_dealer_topic_sql"
	echo "hdfs  finish dwt_dsr_dealer_topic_sql data into hdfs layer on ${yester_day} .................."
elif [ "$1"x = "dwt_dsr_topic"x ];then
    echo " hdfs $1 only run"
	echo "$dwt_dsr_topic_sql"
	$hive -e "$dwt_dsr_topic_sql"
	echo "hdfs  finish dwt_dsr_topic_sql data into hdfs layer on ${yester_day} .................."
	sh /bscflow/dwt/dwt_dwt_dsr_topic_history.sh
elif [ "$1"x = "dws_kpi_sales_waybill_timi"x ];then
    echo " hdfs $1 only run"
	echo "$dws_kpi_sales_waybill_timi"
	$hive -e "$dws_kpi_sales_waybill_timi"
	echo "hdfs  finish dws_kpi_sales_waybill_timi data into hdfs layer on ${yester_day} .................."
elif [ "$1"x = "dws_kpi_monthly_isolate_stock"x ];then
    echo " hdfs $1 only run"
	echo "$dws_kpi_monthly_isolate_stock"
	$hive -e "$dws_kpi_monthly_isolate_stock"
	echo "hdfs  finish dws_kpi_monthly_isolate_stock data into hdfs layer on ${yester_day} .................."
elif [ "$1"x = "dws_kpi_zc_timi"x ];then
    echo " hdfs $1 only run"
	echo "$dws_kpi_zc_timi"
	$hive -e "$dws_kpi_zc_timi"
	echo "hdfs  finish dws_kpi_zc_timi data into hdfs layer on ${yester_day} .................."
elif [ "$1"x = "dws_kpi_stock_putaway_time"x ];then
    echo " hdfs $1 only run"
	echo "$dws_kpi_stock_putaway_time"
	$hive -e "$dws_kpi_stock_putaway_time"
	echo "hdfs  finish dws_kpi_stock_putaway_time data into hdfs layer on ${yester_day} .................."
elif [ "$1"x = "dwd_dim_kpi"x ];then
    echo " hdfs $1 only run"
	echo "$dwd_dim_kpi"
	$hive -e "$dwd_dim_kpi"
	echo "hdfs  finish dwd_dim_kpi data into hdfs layer on ${yester_day} .................."
elif [ "$1"x = "dws_ie_kpi"x ];then
    echo " hdfs $1 only run"
	echo "$dws_ie_kpi"
	$hive -e "$dws_ie_kpi"
	echo "hdfs  finish dws_ie_kpi data into hdfs layer on ${yester_day} .................."
elif [ "$1"x = "dwd_dim_all_kpi"x ];then
    echo " hdfs $1 only run"
	echo "$dwd_dim_all_kpi"
	$hive -e "$dwd_dim_all_kpi"
	echo "hdfs  finish dwd_dim_all_kpi data into hdfs layer on ${yester_day} .................."
elif [ "$1"x = "dwd_outbound_distribution"x ];then
    echo " hdfs $1 only run"
	echo "$dwd_outbound_distribution"
	$hive -e "$dwd_outbound_distribution"
	echo "hdfs  finish dwd_outbound_distribution data into hdfs layer on ${yester_day} .................."
else
	echo "$1 not found"
fi
exitCode=$?
if [ "$exitCode" -ne 0 ];then  #ne不等于
    echo "[Error] hive execute failed!"
    exit $exitCode
fi

echo "End exporting ads_forwarder_app data into HDFS on $yester_day .................."
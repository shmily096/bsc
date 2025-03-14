#!/bin/bash
# Function:
#   sync up dws_to_dwt_kpi_by_bu_detail_PO_DN 
# History:
# 2024-01-18    Sway   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

if [ -n "$1" ] ;then 
    sync_date=$1
	end_date=$1
else
    sync_date=$(date  +%F)
	end_date=$(date  +%F)
fi

# 获取下个月的日期（加上一个月）  
next_month_date=$(date -d "$current_date +1 month" +"%F")

# 获取上个月的日期（减上一个月）  
last_month_date=$(date -d "$current_date -1 month" +"%F")

echo "start syncing dwd_trans_csgn_clear data into DWS layer on ${sync_date} : ${sync_date[year]}"
dwd_dim_material_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_material | tail -n 1 | awk -F'=' '{print $NF}'`
ods_trans_csgn_order_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_trans_csgn_order | tail -n 1 | awk -F'=' '{print $NF}'`
ods_trans_csgn_t2_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_trans_csgn_t2 | tail -n 1 | awk -F'=' '{print $NF}'`
ods_trans_consignmenttracking_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_trans_consignmenttracking | tail -n 1 | awk -F'=' '{print $NF}'`
dwd_dim_customer_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_customer | tail -n 1 | awk -F'=' '{print $NF}'`
ods_trans_consignmentlist_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_trans_consignmentlist | tail -n 1 | awk -F'=' '{print $NF}'`
ods_mdm_customermaster_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_mdm_customermaster | tail -n 1 | awk -F'=' '{print $NF}'`
ods_trans_twinventory_consignment_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_trans_twinventory_consignment | tail -n 1 | awk -F'=' '{print $NF}'`
ods_mdm_upn_mbew_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_mdm_upn_mbew | tail -n 1 | awk -F'=' '{print $NF}'`

# billed:
# dned:

sto_sql="
-- 参数
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.reducers.max=8;
set mapred.reduce.tasks=8;
add jar /user/hive/numDay-1.0-SNAPSHOT.jar;
create temporary function myudf as 'org.example.Nmu';


--------------------------------------------------------------------------dwd_lzo_trans_consignmenttracking
insert overwrite table dwd_lzo_trans_consignmenttracking partition(dt)
SELECT id, updatedt, active, divisionname, customercodesap, customernamesap, customer, customernumber, 
material, plant, materialdescription, batch, expiration, available, committed, deliverydocnum, postingdate, 
salesorder, orderdate, customerponumber, storagetype, ordertype, customerstatus, materialtype, consignmenttype, 
duedate, remainingdays, pgivsexpiration, pgivsexpirationscope, expirationvsreportdate, expirationvsreportdatescope, 
lastworkdate, alertmessenger, '$sync_date'
FROM opsdw.ods_trans_consignmenttracking;

drop table if exists tmp_dwd_calendar_right15_workday;
create table  tmp_dwd_calendar_right15_workday stored as orc as 
select year_month_date,SUBSTR(year_month_date,1,7) year_month  from 
(
select 
cal_date ,cal_month ,cal_year ,workday_flag ,year_month_date ,
ROW_NUMBER() over(PARTITION by cal_year ,cal_month  order by year_month_date desc)  nm,
month_weeknum ,day_of_week 
from dwd_dim_calendar where cal_year >=2024  and workday_flag ='Y') a 
where a.nm=15;

drop table if exists tmp_dwd_dim_material;
create table  tmp_dwd_dim_material stored as orc as 
select material_code ,division_display_name ,standard_cost ,standard_cost_usd ,
product_line1_name,product_line2_name,product_line3_name,product_line4_name,product_line5_name
from dwd_dim_material where dt='$dwd_dim_material_maxdt';

drop table if exists tmp_dwd_dim_customer;
create table  tmp_dwd_dim_customer stored as orc as 
select cust_account ,cust_name ,lpad(cust_account, 10, '0') lpad_cust_account,
case when dealertype is null then 'hospital' else dealertype end dealertype,
case when dealertype is null then 'HD'
when dealertype ='Others' then 'HD' else dealertype end dealertype2
from opsdw.dwd_dim_customer where dt='$dwd_dim_customer_maxdt';

drop table if exists tmp_bsc_lp_price;
create table  tmp_bsc_lp_price stored as orc as 
select DISTINCT  item_code,
case when customer_code like 'LP_%' then 'LP' else customer_code end customer_code,
gross_price_usd,ds,SUBSTR(date_sub(ds,1),0,7) year_mon
from ods_aws_chinapoms_aop_summery_txn_bsc_lp_price;

drop table if exists tmp_dwd_csgntrackingprice;
create table tmp_dwd_csgntrackingprice stored as orc as 
select 
a.id,a.divisionname ,a.customernumber ,a.customer ,
a.upn,a.qty,a.ordertype,a.plant,a.active,
a.deliverydocnum,a.postingdate,a.salesorder,a.orderdate,a.customerponumber,
a.expiration,
a.year,a.month,a.year_mon,a.inventorydate,b.dealertype,c.gross_price_usd
from (
select 
id, divisionname ,customernumber ,customer ,
material upn,
available qty,
ordertype,plant,active,
case when ordertype in ('KE','KA') then committed else available end deliverydocnum,
postingdate,salesorder,orderdate,customerponumber,
expiration,
cast(YEAR(date_sub(updatedt, 1)) as string) year,cast(MONTH(date_sub(updatedt, 1)) as string) month
,SUBSTR(updatedt,0,10) inventorydate,SUBSTR(date_sub(updatedt, 1),0,7) year_mon
from opsdw.ods_trans_consignmenttracking where  DAY(updatedt)=1 
) a 
join tmp_dwd_dim_customer b
on a.customernumber=b.cust_account
JOIN tmp_bsc_lp_price c 
on a.upn=c.item_code and b.dealertype2=c.customer_code 
and a.year_mon=c.year_mon;

drop table if exists tmp_salesdealerinventoryprice;
create table tmp_salesdealerinventoryprice stored as orc as
select 
divisionid ,ownerid ,ownername , locationid ,locationname ,locationparentsapid ,locationdealertype,dealertype
,locationparentdealer, 
	upn,qty,year,month,SUBSTR(inventorydate,0,10) inventorydate,a.year_mon
	,c.gross_price_usd
from 
(select 
divisionid ,ownerid ,ownername , locationid ,locationname ,locationparentsapid ,locationdealertype
,locationparentdealer, 
case when locationparentsapid='BSC' then locationid else locationparentsapid end cust_account,
	upn,qty,year,month, inventorydate,year_mon
from ods_trans_salesdealerinventory where OwnerName ='BSC(HQ)'
) a
join tmp_dwd_dim_customer b
on
a.cust_account=b.cust_account
JOIN tmp_bsc_lp_price c 
on a.upn=c.item_code and b.dealertype2=c.customer_code 
and a.year_mon=c.year_mon;

drop table if exists tmp_dwd_fact_sales_order_info;
create table tmp_dwd_fact_sales_order_info stored as orc as
select so_no ,lpad(so_no, 10, '0') lpad_sn_no,order_type ,SUBSTR(chinese_socreatedt,0,10) createdt ,material,line_number ,batch ,qty ,
customer_code ,division_id ,SUBSTR(chinese_socreatedt,0,7) year_mon
from dwd_fact_sales_order_info where dt>='2024-03-01'
and order_type in ('KB','OR','KA','KE','ZTKA','ZTKB');

drop table if exists tmp_ods_salesorder_partner;
create table tmp_ods_salesorder_partner stored as orc as
select so_no ,customer_shipto
from ods_salesorder_partner
where customer_function ='WE';

drop table if exists tmp_ods_mdm_customermaster;
create table  tmp_ods_mdm_customermaster stored as orc as 
select custome,cl  from ods_mdm_customermaster 
where dt='$ods_mdm_customermaster_maxdt' and cl is not null;

drop table if exists tmp_ods_mdm_upn_mbew;
create table  tmp_ods_mdm_upn_mbew stored as orc as 
select * from ods_mdm_upn_mbew 
where dt='$ods_mdm_upn_mbew_maxdt';

--------------------------------------------------------------------------csgn
insert overwrite table dwd_trans_csgn_clear partition(dt)
select 
a.id ,
a.updatedt,
a.ordertype,
a.sapcode,
a.dealername,
a.orderstatus,
a.createdate,
a.productlinename,
a.upn,
a.upnname,
a.requiredqty,
a.cfnprice,
a.amount,
b.division_display_name,
date(a.createdate) csgndate,
DATE_ADD(date(a.createdate),21) duedate,
a.amount/7.15/1.3 amountusd,
b.standard_cost_usd,
c.year_month_date,
a.dt
from 
(
select 
id ,updatedt,ordertype,sapcode,dealername,orderstatus,createdate,productlinename,upn,
upnname,requiredqty,cfnprice,amount,date(createdate) csgndate,DATE_ADD(date(createdate),21) duedate,
amount/7.15/1.3 amountusd,dt
 from ods_trans_csgn_order where dt='$ods_trans_csgn_order_maxdt'
) a 
join tmp_dwd_dim_material b on 
a.upn=b.material_code
join tmp_dwd_calendar_right15_workday c 
on SUBSTR(a.updatedt,1,7)=c.year_month;

--------------------------------------------------------------------------csgn t2
insert overwrite table dwd_trans_csgn_t2 partition(dt)
SELECT 
	a.id, 
	a.updatedt, 
	a.active, 
	a.dealercode, 
	a.dealername, 
	a.dealertype, 
	a.nbr, 
	a.salesdate, 
	a.divisionid, 
	b.division_display_name, 
	a.upn, 
	a.batch, 
	a.qrcode, 
	a.qty, 
	a.unitprice, 
	b.standard_cost_usd,
	b.standard_cost,
	a.consignmenttype, 
	a.ordertype, 
	a.parentdealercode, 
	a.parentdealername,
    a.dt
 from 
(
select 
id, 
	updatedt, active, dealercode, dealername, dealertype, nbr, salesdate, divisionid, upn, 
	batch, qrcode, qty, unitprice, consignmenttype, ordertype, parentdealercode, parentdealername,
    dt
 from ods_trans_csgn_t2 where dt='$ods_trans_csgn_t2_maxdt'
) a 
join tmp_dwd_dim_material b on 
a.upn=b.material_code;


--------------------------------------------------------------------------csgn sales
insert overwrite table dwd_trans_salesdealerinventory partition(year_mon)
select 
	a.divisionid 
	,b.division_display_name 
	,a.ownerid 
	,a.ownername  
	,a.locationid 
	,a.locationname 
	,a.locationparentsapid 
	,a.locationparentdealer 
	,a.locationdealertype,
	a.locationparentdealer parentname,
	a.locationname locname,
	a.upn,
	a.qty,
	a.gross_price_usd invamtbybscstdsellprice,
	b.standard_cost_usd,
	b.standard_cost,
	a.year,
	a.month,
	dealertype,
	b.product_line1_name,
	b.product_line2_name,
	b.product_line3_name,
	b.product_line4_name,
	b.product_line5_name,
	a.inventorydate,
	a.year_mon
from tmp_salesdealerinventoryprice a 
join tmp_dwd_dim_material b on 
a.upn=b.material_code;

--------------------------------------------------------------------------consignmenttracking
insert overwrite table dwd_trans_consignmenttracking partition(year_mon)
select 
	id,
	d.division_display_name,
	customer,
	upn,
	qty,
	gross_price_usd ,
	ordertype,
	plant,
	active,
	deliverydocnum,
	postingdate,
	salesorder,
	orderdate,
	customerponumber,
	case when DATEDIFF(expiration,inventorydate)<0 then 'Expired'
	when DATEDIFF(expiration,inventorydate)<15 then 'Expired in 15days'
	else 'Expired in 30 days(>15)' end expiration_type,
	expiration,
	dealertype,
	d.product_line1_name,
	d.product_line2_name,
	d.product_line3_name,
	d.product_line4_name,
	d.product_line5_name,
	d.standard_cost_usd,
	inventorydate,
	year_mon 
from 
tmp_dwd_csgntrackingprice a 
join tmp_dwd_dim_material d on 
a.upn=d.material_code;


--------------------------------------------------------------------------csgn turn over rate
insert overwrite table dwd_csgnturn_over_rate partition(year_mon)
select 
	a.so_no,
	a.division_id,
	a.customer_code,
	b.cust_name,
	a.order_type,
	a.material,
	a.line_number,
	a.batch,
	a.createdt,
	a.qty,
	b.dealertype,
	c.gross_price_usd,
	a.year_mon
from tmp_dwd_fact_sales_order_info a 
join tmp_ods_salesorder_partner p 
on a.lpad_sn_no =p.so_no 
join tmp_dwd_dim_customer b
on p.customer_shipto=b.lpad_cust_account
join tmp_dwd_dim_customer t
on a.customer_code=t.cust_account
JOIN tmp_bsc_lp_price c 
on a.material=c.item_code and t.dealertype=c.customer_code 
and a.year_mon=c.year_mon;


--------------------------------------------------------------------------consignmentlist
insert overwrite table dwd_trans_consignmentlist partition(year_mon)
select 
	a.divisionid,
	d.division_display_name,
	a.customernumber,
	a.customername,
	a.material upn,
	a.category,
	b.dealertype,
	d.product_line1_name,
	d.product_line2_name,
	d.product_line3_name,
	d.product_line4_name,
	d.product_line5_name,
	a.quantity qty,
	c.gross_price_usd,
	d.standard_cost_usd,
	a.updatedt , 
	a.year_mon
from 
(select 
SUBSTR(updatedt,0,10) updatedt ,divisionid,division ,
case when LENGTH(customernumber)>6 then substr(customernumber,5,15) else customernumber end customernumber ,
customername ,material ,quantity ,category ,
SUBSTR(updatedt,0,7) year_mon
from opsdw.ods_trans_consignmentlist where dt='$ods_trans_consignmentlist_maxdt' 
and LENGTH(customername)>4
) a 
join tmp_dwd_dim_customer b
on a.customernumber=b.cust_account
JOIN tmp_bsc_lp_price c 
on a.material=c.item_code and b.dealertype2=c.customer_code 
and a.year_mon=c.year_mon
join tmp_dwd_dim_material d on 
a.material=d.material_code;

--------------------------------------------------------------------------dwd_hk_expiration
insert overwrite table dwd_hk_expiration partition(dt)
select a.updatedt ,a.material ,a.customer ,a.customernumber ,a.expiration ,
cast(a.available as float)+cast(a.\`committed\` as float) as qty ,
b.stprs,b.peinh,c.cl,
a.type,a.posting_date ,a.dt
from ods_trans_twinventory_consignment a
left join tmp_ods_mdm_upn_mbew b 
on a.material =b.matnr 
left join tmp_ods_mdm_customermaster c 
on TRIM(a.customernumber)  =TRIM(c.custome)
where a.plant ='D815' and a.dt='$ods_trans_twinventory_consignment_maxdt';
"



delete_tmp="
drop table tmp_dwd_calendar_right15_workday;
alter TABLE ods_trans_twinventory_consignment drop partition (dt='$last_month_date');
----drop table tmp_dwd_dim_material;
----drop table tmp_dwd_dim_customer;
----drop table tmp_bsc_lp_price;
"
# 2. 执行加载数据SQL
echo "$sto_sql"
$hive -e "$sto_sql"
#第二部分收尾删除所有临时表
echo "two $delete_tmp"
$hive -e "$delete_tmp"
echo "End syncing dwd_trans_csgn_clear data into DWS layer on ${sync_date} .................."
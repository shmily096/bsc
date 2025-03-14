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

year_month=$(date  +'%Y-%m')
echo "start syncing dws_to_dwt_kpi_by_bu_detail_PO_DN data into DWS layer on ${sync_date} : ${sync_date[year]}"
# ct_otds_itemlevel_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ct_otds_itemlevel | tail -n 1 | awk -F'=' '{print $NF}'`
ods_material_master_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_material_master | tail -n 1 | awk -F'=' '{print $NF}'`
ods_otds_supply_week_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_otds_supply_week | tail -n 1 | awk -F'=' '{print $NF}'`


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


insert overwrite table ods_otds_itemlevel partition(pardate)
SELECT vbeln, posnr, etenr, matnr, family, franchise, division2_desc, supplier, auart, sold_to, country, gd_region, gd_super_region, werks, vkorg, lifsk, vsbed,
l1, l1_desc, l2, l2_desc, l3, l3_desc, l4, l4_desc, l5, l5_desc, l6_desc, director, manager, planner, 
supply_site, order_create_date, order_create_time, customer_req_date, order_qty, not_shipped_by_edatu, 
unshipped_value, not_confirmed_by_edatu, unconfirmed_value, item_level_bo_qty, item_level_bo_value, pardate
FROM opsdw.ct_otds_itemlevel where dt='$year_month';

drop table if exists tmp_ct_otds_itemlevel;
create table  tmp_ct_otds_itemlevel stored as orc as 
select vbeln ,posnr ,matnr,division2_desc division ,customer_req_date ,pardate ,
b.year_month_date ,b.cal_year,b.cal_month, 
case when SUBSTR(month_weeknum,7,1)>4 then 4 else SUBSTR(month_weeknum,7,1) end week
from opsdw.ods_otds_itemlevel a
join dwd_dim_calendar b 
on date(a.customer_req_date)=b.year_month_date
;

drop table if exists tmp_ods_otds_order_calendar;
create table  tmp_ods_otds_order_calendar stored as orc as
select 
aa.year_month_date,bb.division_id
from (
select year_month_date,'aa' as c from dwd_dim_calendar 
where year_month_date>='2024-01-01' and 
year_month_date<date(date_add(current_date(), -2))
) aa 
join (
select DISTINCT division_id ,'aa' as c 
from dwd_fact_sales_order_info 
where dt>='2024-01-01' and division_id is not null
) bb on aa.c=bb.c;

drop table if exists tmp_ods_otds_delete_material;
create table  tmp_ods_otds_delete_material stored as orc as
select material ,UPPER(english_name) english_name,
UPPER(sap_upl_level2_name) level2, UPPER(sap_upl_level3_name) level3,
UPPER(sap_upl_level4_name) level4,UPPER(sap_upl_level5_name) level5
from ods_material_master where dt='$ods_material_master_maxdt'
;

drop table if exists tmp_dwd_otds_order_material;
create table  tmp_dwd_otds_order_material stored as orc as
select so_no,line_number,material ,division_id ,date(MAX(request_delivery_date)) d1 
from dwd_fact_sales_order_info 
where dt>='2024-01-01' and division_id is not null
and order_type in ('KB','OR','FD','ZNC')
and material not in (
select material  from tmp_ods_otds_delete_material 
where level4 like '%ORISE%'
UNION 
select material  from tmp_ods_otds_delete_material 
where level4 like '%LOTUS EDGE%'
UNION 
select material  from tmp_ods_otds_delete_material 
where level2 like '%GYNSURG%'
UNION 
select material  from tmp_ods_otds_delete_material 
where level4 like '%AVVIGO%'
UNION 
select material  from tmp_ods_otds_delete_material 
where level2 like '%Y%90%'
UNION 
select material  from tmp_ods_otds_delete_material 
where level3 like '%CRYO%ABLATION%' and level4 like '%CRYO%'
UNION 
select material from tmp_ods_otds_delete_material 
where english_name like '%ICE%FX%' and level4 like '%CONSOLE%'
UNION 
select material from tmp_ods_otds_delete_material 
where level5 like '%ICE%FX%CART%'
UNION 
select material from tmp_ods_otds_delete_material 
where english_name like '%VISUAL%ICE%' and level4 like '%CONSOLE%'
)
group by so_no,line_number,material ,division_id;


drop table if exists tmp_dwd_otds_order_info;
create table  tmp_dwd_otds_order_info stored as orc as 
select 
c.year_month_date d1,c.division_id,nvl(b.total_qty,0) total_qty,
sum(nvl(b.total_qty,0)) 
over(PARTITION by DATE_FORMAT(c.year_month_date,'yyyy-MM') , c.division_id order by c.year_month_date ROWS BETWEEN  UNBOUNDED PRECEDING AND CURRENT ROW) sum_qty
,DATE_FORMAT(c.year_month_date,'yyyy-MM') year_mon
from tmp_ods_otds_order_calendar c
left join
(select 
d1 ,division_id,COUNT(1) total_qty
from tmp_dwd_otds_order_material 
group by d1 ,division_id ) b
on c.year_month_date=b.d1 and c.division_id=b.division_id;

drop table if exists tmp_dwd_otds_itemlevel;
create table  tmp_dwd_otds_itemlevel stored as orc as 
select 
vbeln ,posnr ,matnr,
case 
	when division='ICardio' then 'IC'
    when division='StrHrt' then 'SH'
	when division='EPT' then 'EP'
    else division
end division 
,customer_req_date ,a.year_month,a.pardate,
a.year_month_date,
case when a.bo_reason='Order management' 
then nvl(b.bo_reason,a.bo_reason) 
else a.bo_reason
end bo_reason
from 
(
select 
vbeln ,posnr ,matnr,division ,customer_req_date ,date_format(pardate,'yyyyMM') year_month,pardate,
year_month_date,nvl(c.bo_reason,'Order management') bo_reason
from 
tmp_ct_otds_itemlevel b
left JOIN 
(select * from ods_otds_supply_week where dt='$ods_otds_supply_week_maxdt') c
on (b.matnr =c.upn or case when b.division ='CRM' then SUBSTR(b.matnr,3,4) end = c.upn)
and b.cal_year =c.kpi_year and b.cal_month =c.kpi_month and b.week=c.kpi_week
) a 
LEFT join 
(
select 0||order_document order_document ,order_line ,year_month ,'Demand management' bo_reason
from opsdw.ods_otds_demand where dp_request like '%\u4fdd\u7559%'
) b 
on a.vbeln=b.order_document and a.posnr=b.order_line and a.year_month=b.year_month;

--------------------------------------------------------------------------dwd_otds_itemlevel
insert overwrite table dwd_otds_itemlevel 
SELECT 
	vbeln, 
	posnr, 
	matnr, 
	division, 
	customer_req_date, 
	year_month, 
	pardate, 
	year_month_date, 
	bo_reason
    FROM opsdw.tmp_dwd_otds_itemlevel;


--------------------------------------------------------------------------OTDS
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
----第一次刷全年
select 
null as plant,
a.d1 dt,
a.division_id as bu,
nvl(b.qty,0) as value1,
a.sum_qty as value2,  
a.total_qty as flag1, 
null as flag2, 
null as flag3,
case 
	when a.bo_reason='Supply constrain' THEN 'BS0101'
	when a.bo_reason='Supply Constrain' THEN 'BS0101'
	when a.bo_reason='Demand management' THEN 'BS0102'
	when a.bo_reason='Order management' THEN 'BS0103'
end kpi_code,
a.year_mon
from (
select d1 ,division_id ,sum_qty,total_qty,'Supply Constrain' bo_reason,year_mon from opsdw.tmp_dwd_otds_order_info
union 
select d1 ,division_id ,sum_qty,total_qty,'Demand management' bo_reason,year_mon from opsdw.tmp_dwd_otds_order_info
union
select d1 ,division_id ,sum_qty,total_qty,'Order management' bo_reason,year_mon from opsdw.tmp_dwd_otds_order_info
) a 
left join 
(
select 
pardate,division,bo_reason,COUNT(1) qty 
from opsdw.tmp_dwd_otds_itemlevel
group by pardate,division,bo_reason
) b 
on a.d1 =b.pardate and UPPER(a.division_id)=UPPER(b.division) and UPPER(a.bo_reason)=UPPER(b.bo_reason)
where a.year_mon >=(select SUBSTR(MIN(pardate),0,7)  from opsdw.tmp_dwd_otds_itemlevel);

"
delete_tmp="
drop table tmp_dwd_perfect_calendar;
drop table tmp_dwd_perfect_rate;
"
# 2. 执行加载数据SQL
echo "$sto_sql"
$hive -e "$sto_sql"
#第二部分收尾删除所有临时表
echo "two $delete_tmp"
# $hive -e "$delete_tmp"
echo "End syncing dwt_kpi_by_bu_detail data into DWS layer on ${sync_date} .................."
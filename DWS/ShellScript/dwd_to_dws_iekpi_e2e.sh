#!/bin/bash
# Function:
#   sync up dws_iekpi_e2e 
# History:
# 2021-07-08    Donny   v1.0    init

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

echo "start syncing dws_iekpi_e2e data into DWS layer on ${sync_date} : ${sync_date[year]}"

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
add jar /user/hive/numDay-1.0-SNAPSHOT.jar;
create temporary function myudf as 'org.example.Nmu';

insert overwrite table opsdw.dws_iekpi_e2e partition(outbound_yr) 
SELECT 
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
	outbound_mon
, dock_invent_cd
,  dock_invent_holiday    
,  dock_invent_wd 
,   invent_cust1_cd
,  invent_cust1_holiday    
, invent_cust1_wd 
, cust1_chinesepicture_cd
,  cust1_chinesepicture_holiday    
, cust1_chinesepicture_wd 
,  chinesepicture_commodity_cd
,chinesepicture_commodity_holiday    
,  chinesepicture_commodity_wd 
,  commod_act_cust2_cd
,  commod_act_cust2_holiday
, commod_act_cust2_wd
,  act_ciq_cd
,  act_ciq_holiday    
, act_ciq_wd 
,start_end_cd
, start_end_holiday
, start_end_wd
,coodraft_receiveddate
,coocertificate_receiveddate
--, dock_coodraft_cd
--,  dock_coodraft_wd 
--,  coocer_invent_cd
--,  coocer_invent_wd 
,\"{\"||'\"dock_coodraft_cd\":'||'\"'||nvl(dock_coodraft_cd,'')||'\"'||\",\"||'\"dock_coodraft_wd\":'||'\"'||nvl(dock_coodraft_wd,'')||'\"'||\",\"||'\"coocer_invent_cd\":'||'\"'||nvl(coocer_invent_cd,'') ||'\"'||\",\"||'\"coocer_invent_wd\":'||'\"'||nvl(coocer_invent_wd,'')||'\"'||\"}\" as jsons  	
,outbound_yr
from (select 
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
	outbound_mon
, CAST((unix_timestamp(intoinventorydate) - unix_timestamp(dockwarrantdate))/(60 * 60) AS float)/24  as dock_invent_cd
, myudf(dockwarrantdate,intoinventorydate)  as dock_invent_holiday    
, (case when CAST((unix_timestamp(intoinventorydate) - unix_timestamp(dockwarrantdate))/(60 * 60) AS float)
             - myudf(dockwarrantdate,intoinventorydate)*24 <0 then 0
       else CAST((unix_timestamp(intoinventorydate) - unix_timestamp(dockwarrantdate))/(60 * 60) AS float)
             - myudf(dockwarrantdate,intoinventorydate)*24  
       end)/24 as dock_invent_wd 
, CAST((unix_timestamp(customrelease_1) - unix_timestamp(intoinventorydate))/(60 * 60) AS float)/24  as invent_cust1_cd
, myudf(intoinventorydate,customrelease_1)  as invent_cust1_holiday    
, (case when CAST((unix_timestamp(customrelease_1) - unix_timestamp(intoinventorydate))/(60 * 60) AS float)
             - myudf(intoinventorydate,customrelease_1)*24 <0 then 0
       else CAST((unix_timestamp(customrelease_1) - unix_timestamp(intoinventorydate))/(60 * 60) AS float)
             - myudf(intoinventorydate,customrelease_1)*24  
       end)/24 as invent_cust1_wd 
, CAST((unix_timestamp(chineselabelpicturereceiveddate) - unix_timestamp(customrelease_1))/(60 * 60) AS float)/24  as cust1_chinesepicture_cd
, myudf(customrelease_1,chineselabelpicturereceiveddate) as cust1_chinesepicture_holiday    
, (case when CAST((unix_timestamp(chineselabelpicturereceiveddate) - unix_timestamp(customrelease_1))/(60 * 60) AS float)
             - myudf(customrelease_1,chineselabelpicturereceiveddate)*24 <0 then 0
       else CAST((unix_timestamp(chineselabelpicturereceiveddate) - unix_timestamp(customrelease_1))/(60 * 60) AS float)
             - myudf(customrelease_1,chineselabelpicturereceiveddate)*24  
       end)/24 as cust1_chinesepicture_wd 
, CAST((unix_timestamp(commodityinspection_date) - unix_timestamp(chineselabelpicturereceiveddate))/(60 * 60) AS float)/24  as chinesepicture_commodity_cd
, myudf(chineselabelpicturereceiveddate,commodityinspection_date)  as chinesepicture_commodity_holiday    
, (case when CAST((unix_timestamp(commodityinspection_date) - unix_timestamp(chineselabelpicturereceiveddate))/(60 * 60) AS float)
             - myudf(chineselabelpicturereceiveddate,commodityinspection_date)*24 <0 then 0
       else CAST((unix_timestamp(commodityinspection_date) - unix_timestamp(chineselabelpicturereceiveddate))/(60 * 60) AS float)
             - myudf(chineselabelpicturereceiveddate,commodityinspection_date)*24  
       end)/24 as chinesepicture_commodity_wd 
, (case when category_code='2' then CAST((unix_timestamp(actualtest_date) - unix_timestamp(commodityinspection_date))/(60 * 60) AS float)
    else  CAST((unix_timestamp(customrelease_2) - unix_timestamp(commodityinspection_date))/(60 * 60) AS float) end)/24 as commod_act_cust2_cd
, (case when category_code='2' then  myudf(commodityinspection_date,actualtest_date)*24 
    else myudf(commodityinspection_date,customrelease_2)*24 end)/24 as commod_act_cust2_holiday
,(case when  category_code='2' then  CAST((unix_timestamp(actualtest_date) - unix_timestamp(commodityinspection_date))/(60 * 60) AS float)-myudf(commodityinspection_date,actualtest_date)*24 
    else CAST((unix_timestamp(customrelease_2) - unix_timestamp(commodityinspection_date))/(60 * 60) AS float)-myudf(commodityinspection_date,customrelease_2)*24 end )/24 as commod_act_cust2_wd
, CAST((unix_timestamp(ciq_signcompletiondate) - unix_timestamp(actualtest_date))/(60 * 60) AS float)/24  as act_ciq_cd
, myudf(actualtest_date,ciq_signcompletiondate)  as act_ciq_holiday    
, (case when CAST((unix_timestamp(ciq_signcompletiondate) - unix_timestamp(actualtest_date))/(60 * 60) AS float)
             - myudf(actualtest_date,ciq_signcompletiondate)*24 <0 then 0
       else CAST((unix_timestamp(ciq_signcompletiondate) - unix_timestamp(actualtest_date))/(60 * 60) AS float)
             - myudf(actualtest_date,ciq_signcompletiondate)*24  
       end)/24 as act_ciq_wd 
,(case when category_code='2' then  CAST((unix_timestamp(ciq_signcompletiondate) - unix_timestamp(dockwarrantdate))/(60 * 60) AS float)
    when category_code='3' then CAST((unix_timestamp(customrelease_2) - unix_timestamp(dockwarrantdate))/(60 * 60) AS float)
	else CAST((unix_timestamp(intoinventorydate) - unix_timestamp(dockwarrantdate))/(60 * 60) AS float) end)/24 as start_end_cd
, (case when category_code='2' then  myudf(dockwarrantdate,ciq_signcompletiondate)*24
    when category_code='3' then myudf(dockwarrantdate,customrelease_2) *24
	else myudf(dockwarrantdate,intoinventorydate)*24  end)/24 as start_end_holiday
, (case when  category_code='2' then CAST((unix_timestamp(ciq_signcompletiondate) - unix_timestamp(dockwarrantdate))/(60 * 60) AS float)- myudf(dockwarrantdate,ciq_signcompletiondate)*24
    when category_code='3' then CAST((unix_timestamp(customrelease_2) - unix_timestamp(dockwarrantdate))/(60 * 60) AS float)-myudf(dockwarrantdate,customrelease_2) *24
	else CAST((unix_timestamp(intoinventorydate) - unix_timestamp(dockwarrantdate))/(60 * 60) AS float)-myudf(dockwarrantdate,intoinventorydate)*24 end)/24 as start_end_wd
,coodraft_receiveddate
,coocertificate_receiveddate
, CAST((unix_timestamp(coodraft_receiveddate) - unix_timestamp(dockwarrantdate))/(60 * 60) AS float)/24  as dock_coodraft_cd
, (CAST((unix_timestamp(coodraft_receiveddate) - unix_timestamp(dockwarrantdate))/(60 * 60) AS float)
             - myudf(dockwarrantdate,coodraft_receiveddate)*24)/24 as  dock_coodraft_wd 
, CAST((unix_timestamp(intoinventorydate)+1*24*3600 - unix_timestamp(coocertificate_receiveddate))/(60 * 60) AS float)/24  as coocer_invent_cd
, (CAST((unix_timestamp(intoinventorydate)+1*24*3600 - unix_timestamp(coocertificate_receiveddate))/(60 * 60) AS float)
             - myudf(coocertificate_receiveddate,intoinventorydate)*24 )/24 as coocer_invent_wd 
,outbound_yr
FROM opsdw.dwd_iekpi_e2e
where outbound_yr>=substr('$sync_date',1,4))x
;
"

# 2. 执行加载数据SQL
echo "$sto_sql"
$hive -e "$sto_sql"

echo "End syncing dws_iekpi_e2e data into DWS layer on ${sync_date} .................."	

#!/bin/bash
# Function:
#   iekpi
# History:
# 2023-03-31    slc   v1.0    init
export LANG="en_US.UTF-8"
# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# 默认取当前时间的前一天 
if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date  +%F)
fi

echo "start syncing dwd_iekpi_e2e_tj  data into DWD layer on ${sync_date} .................."
# 1 Hive SQL string
sto_sql="
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
add jar /user/hive/numDay-1.0-SNAPSHOT.jar;
create temporary function myudf as 'org.example.Nmu';


drop table tmp_inbound_outbound_tj;
create table tmp_inbound_outbound_tj as 
select
	inbound.customdockdate, ---海关仓单时间
	inbound.dockwarrantdate,	---仓单确认时间
	inbound.inbounddeclaration_finishdate, ---进境备案完成时间
	nvl(inbound.intoinventorydate, outbound.intoinventorydate) as intoinventorydate, ---入库时间
	inbound.intoinventorydate as inbound_intoinventorydate,
	outbound.intoinventorydate as outbound_intoinventorydate,
	inbound.invoice,
	inbound.pickupdocument_no, ---提单号
	 port,
	outbound.commercialinvoice,
	outbound.commodityinspection_inboundcheck, ---入库查验
	outbound.commodityinspection_llicheck, --本地化查验
	outbound.declaration_completiondate, ---出库清关一放
	outbound.commodityinspection_outboundcheck,	---商检查验时间3（出库查验）
		----商检查验时间<-海关仓单时间
	 CAST((unix_timestamp(outbound.commodityinspection_outboundcheck) - unix_timestamp(customdockdate))/(60 * 60) AS float)/24  as outbound_customdock_cd  
	, (case when CAST((unix_timestamp(outbound.commodityinspection_outboundcheck) - unix_timestamp(customdockdate))/(60 * 60) AS float)
             - myudf(customdockdate,outbound.commodityinspection_outboundcheck)*24 <0 then 0
       else CAST((unix_timestamp(outbound.commodityinspection_outboundcheck) - unix_timestamp(customdockdate))/(60 * 60) AS float)
             - myudf(customdockdate,outbound.commodityinspection_outboundcheck)*24  
       end)/24 as outbound_customdock_wd ,
	---入库时间<-仓单确认时间
	 CAST((unix_timestamp(nvl(inbound.intoinventorydate, outbound.intoinventorydate)) - unix_timestamp(inbound.dockwarrantdate))/(60 * 60) AS float)/24  as dock_invent_cd  ,
	 (case when CAST((unix_timestamp(nvl(inbound.intoinventorydate, outbound.intoinventorydate)) - unix_timestamp(inbound.dockwarrantdate))/(60 * 60) AS float)
             - myudf(inbound.dockwarrantdate,nvl(inbound.intoinventorydate, outbound.intoinventorydate))*24 <0 then 0
       else CAST((unix_timestamp(nvl(inbound.intoinventorydate, outbound.intoinventorydate)) - unix_timestamp(inbound.dockwarrantdate))/(60 * 60) AS float)
             - myudf(inbound.dockwarrantdate,nvl(inbound.intoinventorydate, outbound.intoinventorydate))*24  
       end)/24 as dock_invent_wd ,
	----入库时间<-进境备案完成时间
	CAST((unix_timestamp(nvl(inbound.intoinventorydate, outbound.intoinventorydate)) - unix_timestamp(inbound.inbounddeclaration_finishdate))/(60 * 60) AS float)/24  as declar_invent_cd  ,
	(case when CAST((unix_timestamp(nvl(inbound.intoinventorydate, outbound.intoinventorydate)) - unix_timestamp(inbound.inbounddeclaration_finishdate))/(60 * 60) AS float)
             - myudf(inbound.inbounddeclaration_finishdate,nvl(inbound.intoinventorydate, outbound.intoinventorydate))*24 <0 then 0
       else CAST((unix_timestamp(nvl(inbound.intoinventorydate, outbound.intoinventorydate)) - unix_timestamp(inbound.inbounddeclaration_finishdate))/(60 * 60) AS float)
             - myudf(inbound.inbounddeclaration_finishdate,nvl(inbound.intoinventorydate, outbound.intoinventorydate))*24  
       end)/24 as declar_invent_wd ,
	-----入库查验<-入库 
	CAST((unix_timestamp(outbound.commodityinspection_inboundcheck) - unix_timestamp(nvl(inbound.intoinventorydate, outbound.intoinventorydate)))/(60 * 60) AS float)/24  as invent_inboundcheck_cd  ,
	if(CAST((unix_timestamp(outbound.commodityinspection_inboundcheck) - unix_timestamp(nvl(inbound.intoinventorydate, outbound.intoinventorydate)))/(60 * 60) AS float)
		-myudf(nvl(inbound.intoinventorydate, outbound.intoinventorydate),outbound.commodityinspection_inboundcheck)*24<0,0,
		CAST((unix_timestamp(outbound.commodityinspection_inboundcheck) - unix_timestamp(nvl(inbound.intoinventorydate, outbound.intoinventorydate)))/(60 * 60) AS float)
		-myudf(nvl(inbound.intoinventorydate, outbound.intoinventorydate),outbound.commodityinspection_inboundcheck)*24
		)/24 as invent_inboundcheck_wd,
	---本地化查验<-入库查验
	CAST((unix_timestamp(outbound.commodityinspection_llicheck) - unix_timestamp(outbound.commodityinspection_inboundcheck))/(60 * 60) AS float)/24  as inboundcheck_llicheck_cd  ,
		if(CAST((unix_timestamp(outbound.commodityinspection_llicheck) - unix_timestamp(outbound.commodityinspection_inboundcheck))/(60 * 60) AS float)
		-myudf(outbound.commodityinspection_inboundcheck,outbound.commodityinspection_llicheck)*24<0,0,
		CAST((unix_timestamp(outbound.commodityinspection_llicheck) - unix_timestamp(outbound.commodityinspection_inboundcheck))/(60 * 60) AS float)
		-myudf(outbound.commodityinspection_inboundcheck,outbound.commodityinspection_llicheck)*24
		)/24 as inboundcheck_llicheck_wd,
	---出库清关一放<-本地化查验
		CAST((unix_timestamp(outbound.declaration_completiondate) - unix_timestamp(outbound.commodityinspection_llicheck))/(60 * 60) AS float)/24  as llicheck_cust1_cd  ,
		if(CAST((unix_timestamp(outbound.declaration_completiondate) - unix_timestamp(outbound.commodityinspection_llicheck))/(60 * 60) AS float)
		-myudf(outbound.commodityinspection_llicheck,outbound.declaration_completiondate)*24<0,0,
		CAST((unix_timestamp(outbound.declaration_completiondate) - unix_timestamp(outbound.commodityinspection_llicheck))/(60 * 60) AS float)
		-myudf(outbound.commodityinspection_llicheck,outbound.declaration_completiondate)*24
		)/24 as llicheck_cust1_wd,
	---商检查验出库<-出库清关一放
	CAST((unix_timestamp(outbound.commodityinspection_outboundcheck) - unix_timestamp(outbound.declaration_completiondate))/(60 * 60) AS float)/24  as cust1_outcheck_cd  ,
		if(CAST((unix_timestamp(outbound.commodityinspection_outboundcheck) - unix_timestamp(outbound.declaration_completiondate))/(60 * 60) AS float)
		-myudf(outbound.declaration_completiondate,outbound.commodityinspection_outboundcheck)*24<0,0,
		CAST((unix_timestamp(outbound.commodityinspection_outboundcheck) - unix_timestamp(outbound.declaration_completiondate))/(60 * 60) AS float)
		-myudf(outbound.declaration_completiondate,outbound.commodityinspection_outboundcheck)*24
		)/24 as cust1_outcheck_wd,
	nvl(inbound.year,outbound.year) as outbound_yr
FROM opsdw.ods_inbound_declaration_tj inbound 
 full join opsdw.ods_outbound_customclearance_tj outbound on lpad(inbound.invoice,20,'0') = lpad(outbound.commercialinvoice,20,'0')
where inbound.year='${sync_date:0:4}'
	and outbound.year='${sync_date:0:4}'
	and inbound.pickup_status='\u5168\u90e8\u4e0d\u53ef\u5206\u62e8';---全部不可分拨
	
insert overwrite table opsdw.dwd_iekpi_e2e_tj partition(outbound_yr) 
select
	customdockdate
	,dockwarrantdate
	,inbounddeclaration_finishdate
	,intoinventorydate
	,invoice
	,pickupdocument_no
	,\"{\"
	||'\"port\":'||'\"'||nvl(port,'') ||'\"'||\",\"
	||'\"inbound_intoinventorydate\":'||'\"'||nvl(inbound_intoinventorydate,'') ||'\"'||\",\"
	||'\"outbound_intoinventorydate\":'||'\"'||nvl(outbound_intoinventorydate,'') ||'\"'||\",\"
	||'\"outbound_customdock_cd\":'||'\"'||nvl(outbound_customdock_cd,0) ||'\"'||\",\"
	||'\"outbound_customdock_wd\":'||'\"'||nvl(outbound_customdock_wd,0) ||'\"'||\",\"
	||'\"commodityinspection_inboundcheck\":'||'\"'||nvl(commodityinspection_inboundcheck,'') ||'\"'||\",\"
	||'\"commodityinspection_llicheck\":'||'\"'||nvl(commodityinspection_llicheck,'') ||'\"'||\",\"
	||'\"declaration_completiondate\":'||'\"'||nvl(declaration_completiondate,'') ||'\"'||\",\"
	||'\"dock_invent_cd\":'||'\"'||nvl(dock_invent_cd,0) ||'\"'||\",\"
	||'\"dock_invent_wd\":'||'\"'||nvl(dock_invent_wd,0) ||'\"'||\",\"
	||'\"declar_invent_cd\":'||'\"'||nvl(declar_invent_cd,0) ||'\"'||\",\"
	||'\"declar_invent_wd\":'||'\"'||nvl(declar_invent_wd,0) ||'\"'||\",\"
	||'\"invent_inboundcheck_cd\":'||'\"'||nvl(invent_inboundcheck_cd,0) ||'\"'||\",\"
	||'\"invent_inboundcheck_wd\":'||'\"'||nvl(invent_inboundcheck_wd,0) ||'\"'||\",\"
	||'\"inboundcheck_llicheck_cd\":'||'\"'||nvl(inboundcheck_llicheck_cd,0) ||'\"'||\",\"
	||'\"inboundcheck_llicheck_wd\":'||'\"'||nvl(inboundcheck_llicheck_wd,0) ||'\"'||\",\"
	||'\"llicheck_cust1_cd\":'||'\"'||nvl(llicheck_cust1_cd,0) ||'\"'||\",\"
	||'\"llicheck_cust1_wd\":'||'\"'||nvl(llicheck_cust1_wd,0) ||'\"'||\",\"
	||'\"cust1_outcheck_cd\":'||'\"'||nvl(cust1_outcheck_cd,0) ||'\"'||\",\"
	||'\"cust1_outcheck_wd\":'||'\"'||nvl(cust1_outcheck_wd,0) ||'\"'
	||\"}\" as jsons
	,commercialinvoice
	,commodityinspection_outboundcheck
	----仓单确认时间<-海关仓单时间
	, CAST((unix_timestamp(dockwarrantdate) - unix_timestamp(customdockdate))/(60 * 60) AS float)/24  as dock_customdock_cd
	, myudf(customdockdate,dockwarrantdate)  as dock_customdock_holiday    
	, (case when CAST((unix_timestamp(dockwarrantdate) - unix_timestamp(customdockdate))/(60 * 60) AS float)
             - myudf(customdockdate,dockwarrantdate)*24 <0 then 0
       else CAST((unix_timestamp(dockwarrantdate) - unix_timestamp(customdockdate))/(60 * 60) AS float)
             - myudf(customdockdate,dockwarrantdate)*24  
       end)/24 as dock_customdock_wd 
	----进境备案完成时间<-海关仓单时间
	, CAST((unix_timestamp(inbounddeclaration_finishdate) - unix_timestamp(customdockdate))/(60 * 60) AS float)/24  as declaration_customdock_cd
	, myudf(customdockdate,inbounddeclaration_finishdate)  as declaration_customdock_holiday    
	, (case when CAST((unix_timestamp(inbounddeclaration_finishdate) - unix_timestamp(customdockdate))/(60 * 60) AS float)
             - myudf(customdockdate,inbounddeclaration_finishdate)*24 <0 then 0
       else CAST((unix_timestamp(inbounddeclaration_finishdate) - unix_timestamp(customdockdate))/(60 * 60) AS float)
             - myudf(customdockdate,inbounddeclaration_finishdate)*24  
       end)/24 as declaration_customdock_wd    
	----入库时间<-海关仓单时间
	, CAST((unix_timestamp(intoinventorydate) - unix_timestamp(customdockdate))/(60 * 60) AS float)/24  as invent_customdock_cd
	, myudf(customdockdate,intoinventorydate)  as invent_customdock_holiday    
	, (case when CAST((unix_timestamp(intoinventorydate) - unix_timestamp(customdockdate))/(60 * 60) AS float)
             - myudf(customdockdate,intoinventorydate)*24 <0 then 0
       else CAST((unix_timestamp(intoinventorydate) - unix_timestamp(customdockdate))/(60 * 60) AS float)
             - myudf(customdockdate,intoinventorydate)*24  
       end)/24 as invent_customdock_wd 
	----商检查验时间3(出库查验)<-入库时间
	, CAST((unix_timestamp(commodityinspection_outboundcheck) - unix_timestamp(intoinventorydate))/(60 * 60) AS float)/24  as outbound_invent_cd
	, myudf(intoinventorydate,commodityinspection_outboundcheck)  as outbound_invent_holiday    
	, (case when CAST((unix_timestamp(commodityinspection_outboundcheck) - unix_timestamp(intoinventorydate))/(60 * 60) AS float)
             - myudf(intoinventorydate,commodityinspection_outboundcheck)*24 <0 then 0
       else CAST((unix_timestamp(commodityinspection_outboundcheck) - unix_timestamp(intoinventorydate))/(60 * 60) AS float)
             - myudf(intoinventorydate,commodityinspection_outboundcheck)*24  
       end)/24 as outbound_invent_wd
	,outbound_yr
from tmp_inbound_outbound_tj;
"
delete_sql="
drop table tmp_inbound_outbound_tj;
"	
# 2. 执行加载数据SQL
echo  "$sto_sql"
$hive -e "$sto_sql"
$hive -e "$delete_sql"
echo "End syncing dwd_iekpi_e2e_tj data into DWD layer on ${sync_date} .................."
	

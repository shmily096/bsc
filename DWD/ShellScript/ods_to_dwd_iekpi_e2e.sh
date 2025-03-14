#!/bin/bash
# Function:
#   iekpi
# History:
# 2022-10-27    Donny   v1.0    init
export LANG=zh_CN.gbk
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

echo "start syncing inventory onhand data into DWD layer on ${sync_date} .................."
# 1 Hive SQL string
sto_sql="
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
drop table tmp_inbounddeclaration;
create table tmp_inbounddeclaration as 
select idc.biz_no 
		,idc.coodraft_receiveddate
		,idc.coocertificate_receiveddate
     , idc.pickupdocument_no
     , idc.commericalinvoice
     , idc.invoice
	 , ids.emergencysigns
     , idc.forwarding 
     , idc.arrivalgoods_type 
     , idc.shipfrom_country 
     , idc.preinspection_flag 
     , idc.bscinformslcdate
	 , ids.t1pickupdate 
     , ids.etd
     , ids.eta
     , ids.reviseetd 
     , ids.reviseeta 
     , ids.actualarrivaltime 
     , ids.forwordinginformslcpick
     , idc.dockwarrantdate 
     , idc.intoinventorydate 
     , idc.inbounddeclaration_startdate
     , idc.inbounddeclaration_finishdate 
     , idc.picturetaken_date 
     , idc.inspection_appointmentdate 
     , idc.inspection_finishdate
     , idc.pieces 
     , idc.qty 
     , idc.mon 
     , idc.yr
 from (
select biz_no 
		,coodraft_receiveddate
		,coocertificate_receiveddate
     , pickupdocument_no
     , commericalinvoice
     , trim(invoice) as invoice
     , forwarding 
     , arrivalgoods_type 
     , mon
     , week 
     , shipfrom_country 
     , pieces 
     , qty 
     , preinspection_flag 
     , bscinformslcdate
     , dockwarrantdate 
     , intoinventorydate 
     , inbounddeclaration_startdate
     , inbounddeclaration_finishdate 
     , picturetaken_date 
     , inspection_appointmentdate 
     , inspection_finishdate 
     , year as yr
  from (select * from ods_inbound_declaration  where year='${sync_date:0:4}')xx
  lateral view explode(split(replace(replace(replace(trim(commericalinvoice),' ','/'),',','/'),'，','/'),'/')) t as invoice  ) idc 
 left join (
     select commercialinvoice 
      , trim(invoice) as invoice  
      , emergencysigns 
      , t1pickupdate 
      , etd
      , eta
      , reviseetd 
      , reviseeta 
      , actualarrivaltime 
      , forwordinginformslcpick
      , updatedate 
   from dwd_trans_ctmshipmentstatus
lateral view explode(split(replace(replace(replace(trim(commercialinvoice),' ','/'),',','/'),'，','/'),'/')) t as invoice
  where infotype = 'Inbound'
    and dt = '$sync_date'
    and updatedate >= '2022-01-01' )ids on lpad(upper(idc.invoice),20,'0') = lpad(upper(ids.invoice),20,'0')
group by idc.biz_no 
,idc.coodraft_receiveddate
		,idc.coocertificate_receiveddate
     , idc.pickupdocument_no
     , idc.commericalinvoice
     , idc.invoice
	 , ids.emergencysigns
     , idc.forwarding 
     , idc.arrivalgoods_type 
     , idc.shipfrom_country 
     , idc.preinspection_flag 
     , idc.bscinformslcdate
	 , ids.t1pickupdate 
     , ids.etd
     , ids.eta
     , ids.reviseetd 
     , ids.reviseeta 
     , ids.actualarrivaltime 
     , ids.forwordinginformslcpick
     , idc.dockwarrantdate 
     , idc.intoinventorydate 
     , idc.inbounddeclaration_startdate
     , idc.inbounddeclaration_finishdate 
     , idc.picturetaken_date 
     , idc.inspection_appointmentdate 
     , idc.inspection_finishdate
     , idc.pieces 
     , idc.qty 
     , idc.week 
     , idc.mon 
     , idc.yr;
drop table tmp_customclearance;
create table tmp_customclearance as 
select biz_no 
     , declaration_itemname 
     , commericalinvoice
     , invoice
     , document_finishpreparationdate
     , customclearance_date 
     , chineselabelpicturereceiveddate 
	 , declaration_completiondate
     , batchinfo_receiveddate
     , taxpayment_applicationdate
     , taxpayment_completiondate
     , commodityinspection_date
     , is_excluded
     , is_malaysia
     , abnormal_reason 
     , status 
     , case when length(invoice)<=3 then 'N' 
            when upper(invoice) rlike '^[0-9A-Z.+]+$' = true and upper(invoice) not In ('DEMO', 'SAMPLE') then 'Y'
          else 'N' end as is_valid
	 , pieces
     , mon
     , yr
 from (
		select biz_no 
			 , declaration_itemname 
			 , commericalinvoice
			 , REGEXP_EXTRACT(trim(upper(invoice)),'^[0-9A-Z.+]+',0) as invoice
			 , document_finishpreparationdate
			 , customclearance_date 
			 , chineselabelpicturereceiveddate 
			 , declaration_completiondate
			 , batchinfo_receiveddate
			 , taxpayment_applicationdate
			 , taxpayment_completiondate
			 , commodityinspection_date
			 , is_excluded
			 , is_malaysia
			 , abnormal_reason 
			 , status1 as status
			 , pieces
             , mon			 
			 , yr
		 from (
		select biz_no 
			 , declaration_itemname 
			 , commericalinvoice
			 , document_finishpreparationdate
			 , customclearance_date 
			 , chineselabelpicturereceiveddate 
			 , declaration_completiondate
			 , batchinfo_receiveddate
			 , taxpayment_applicationdate 
			 , taxpayment_completiondate
			 , commodityinspection_date
			 , coalesce(is_excluded,'\u5426') as is_excluded
			 , 'N' as is_malaysia
			 , abnormal_reason 
			 , status1
			 , pieces
             , mon			 
			 , year as yr 
		  from  (select * from ods_outbound_customclearance  where year='${sync_date:0:4}')xx  
		  union all 
		  select biz_no 
			 , declaration_itemname 
			 , commericalinvoice
			 , document_finishpreparationdate
			 , customclearance_date 
			 , cast(chineselabelpicturereceiveddate as string) as chineselabelpicturereceiveddate
			 , declaration_completiondate
			 , cast(batchinfo_receiveddate as string) as batchinfo_receiveddate
			 , cast(taxpayment_applicationdate as string) as taxpayment_applicationdate
			 , cast(taxpayment_completiondate as string) as taxpayment_completiondate
			 , cast(commodityinspection_date as string) as commodityinspection_date
			 , '\u5426' as is_excluded
			 , 'Y' as is_malaysia
			 , abnormal_reason 
			 , status_1
			 , cast(pieces as int) as pieces
			 , mon
			 , year as yr 
		  from (select * from ods_outbound_customclarance_mal  where year='${sync_date:0:4}')xxx   ) cust_cl
		 lateral view explode(split(replace(trim(commericalinvoice),' ','/'),'/')) t as invoice 
  ) all_com ;
  drop table tmp_customclearance_pacemaker;
create table tmp_customclearance_pacemaker as 
select commericalinvoice
     , trim(invoice) as invoice
	 , forwarder_referenceid as biz_no
     , item_name as declaration_itemname
     , document_finishpreparationdate
     , taxpayment_applicationdate	 
     , taxpayment_completiondate
     , declaration_completiondate	 
     , localization_completiondate as chineselabelpicturereceiveddate
     , commodityinspection_date 
     , testscheduled_date 
     , actualtest_date 
     , ciq_signcompletiondate 
	 , COALESCE(abnormal_reason,remark) as abnormal_reason
	 , mon
     , year as yr
 from (select * from opsdw.ods_outbound_pacemaker  where year='${sync_date:0:4}')xxx  
 lateral view explode(split(replace(replace(trim(commericalinvoice),',','/'),'，','/'),'/')) t as invoice
where trim(upper(noo)) like 'CRM%' ;
drop table tmp_ie_invoicecategory ;
create table tmp_ie_invoicecategory as
	select invoice
		  , max(category) as category_code
	 from (
		select invoice
			 , 3 as category
		 from tmp_customclearance  -- 出库清关
		union all 
		select invoice
			 , 2 as category
		 from tmp_customclearance_pacemaker  -- 起搏器
		union all 
		 select invoice
			 , -1 as category
		 from tmp_inbounddeclaration -- 进境备案 
		  ) all_invoice 
		group by invoice ;
		
insert overwrite table opsdw.dwd_iekpi_e2e partition(outbound_yr)
select idc.pickupdocument_no
     , idc.commericalinvoice
     , idc.invoice as inbound_invoice
	 , idc.emergencysigns  ---没啥用可以剔除
     , idc.forwarding 
     , idc.arrivalgoods_type 
     , idc.shipfrom_country 
     , idc.preinspection_flag ---没啥用可以剔除
     , idc.bscinformslcdate	---没啥用可以剔除
	 , idc.t1pickupdate 	--没啥用可以剔除
     , idc.etd	--没啥用可以剔除
     , idc.eta	--没啥用可以剔除
     , idc.reviseetd 	--没啥用可以剔除
     , idc.reviseeta 	--没啥用可以剔除
     , idc.actualarrivaltime --没啥用可以剔除
     , idc.forwordinginformslcpick	--没啥用可以剔除
     , cast(idc.dockwarrantdate as timestamp) as dockwarrantdate  --1
     , cast(idc.intoinventorydate as timestamp) as intoinventorydate --2
     , idc.inbounddeclaration_startdate	--没啥用可以剔除
     , idc.inbounddeclaration_finishdate 	--没啥用可以剔除
     , idc.picturetaken_date 	--没啥用可以剔除
     , idc.inspection_appointmentdate --没啥用可以剔除
     , idc.inspection_finishdate	--没啥用可以剔除
     , idc.pieces as inbound_pics	--没啥用可以剔除
     , idc.mon as inbound_mon	--没啥用可以剔除
     , idc.yr as inbound_yr	--这个其实就是更新时间对应的年
	 , cast(idc.coodraft_receiveddate as timestamp) as coodraft_receiveddate ---jia
	 , cast(idc.coocertificate_receiveddate as timestamp) as coocertificate_receiveddate --jia
	 , inb_all.biz_no as outbound_biz_no
	 , inb_all.declaration_itemname --没啥用可以剔除
	 , coalesce(inb_all.commericalinvoice, idc.commericalinvoice) as outbound_commericalinvoice
	 , inb_all.invoice as outbound_invoice   ---主键
	 , inb_all.document_finishpreparationdate	--没啥用可以剔除
	 , cast(inb_all.customrelease_1 as timestamp) as customrelease_1 --3海关一放时间也叫清关完成时间
	 , cast(inb_all.commodityinspection_date as timestamp) as commodityinspection_date --5商检查验时间
     , cast(inb_all.chineselabelpicturereceiveddate as timestamp) as chineselabelpicturereceiveddate	--4收到仓库中文标签照片时间
	 , cast(inb_all.customrelease_2 as timestamp) as customrelease_2 --8海关二放时间
	 , cast(inb_all.testscheduled_date as timestamp) as testscheduled_date	--没啥用可以剔除
	 , cast(inb_all.actualtest_date as timestamp) as actualtest_date --6实际检测时间
	 , cast(inb_all.ciq_signcompletiondate as timestamp) as ciq_signcompletiondate --7两证完成
	 , inb_all.taxpayment_applicationdate --付税申请时间	--没啥用可以剔除
	 , inb_all.taxpayment_completiondate	--付税完成时间	--没啥用可以剔除
	 , inb_all.declaration_completiondate	--没啥用可以剔除
	 , inb_all.is_excluded  --自定义的好像没用上
	 , inb_all.is_malaysia	--自定义的是否马来西亚
	 , inb_all.abnormal_reason --没啥用可以剔除
	 , inb_all.status --没啥用可以剔除
	 , inb_all.is_valid	--没啥用可以剔除
	 , inb_all.category_code
	 , case when inb_all.category_code = -1 and idc.intoinventorydate is null then '\u672a\u5230\u5e93'	--未到库
            when inb_all.category_code = -1 and idc.intoinventorydate is not null then '\u5206\u62e8' --分拨
         else '\u975e\u5206\u62e8'  --非分拨
		 end as distribution_status
	 , inb_all.pieces as outbound_pieces
	 , 'D835' as destination_wh
	 , 'PVG'  as airport
	 , inb_all.mon as outbound_mon
	, COALESCE (inb_all.yr,idc.yr) as outbound_yr
 from (
	select biz_no 
		 , declaration_itemname 
		 , commericalinvoice
		 , invoice
		 , document_finishpreparationdate
		 ---, declaration_completiondate as customrelease_1
		 , taxpayment_completiondate as customrelease_1
		 , commodityinspection_date
		 , chineselabelpicturereceiveddate 
		 , customclearance_date as customrelease_2
		 , null as testscheduled_date 
		 , null as actualtest_date 
		 , null as ciq_signcompletiondate
		 , taxpayment_applicationdate
		 , taxpayment_completiondate
		 , declaration_completiondate
		 , is_excluded
		 , is_malaysia
		 , abnormal_reason 
		 , status 
		 , is_valid
		 , pieces
		 , 3 as category_code
		 , mon
		 , yr
	  from tmp_customclearance 
	union all 
	select biz_no
		 , declaration_itemname
		 , commericalinvoice
		 , invoice
		 , document_finishpreparationdate
		 , taxpayment_completiondate as customrelease_1
		 , chineselabelpicturereceiveddate
		 , commodityinspection_date 
		 , null as customrelease_2
		 , testscheduled_date 
		 , actualtest_date 
		 , ciq_signcompletiondate 
		 , taxpayment_applicationdate	 
		 , taxpayment_completiondate
		 , declaration_completiondate	 
		 , '\u5426' as is_excluded
		 , 'N' as is_malaysia
		 , abnormal_reason
		 , null as status 
		 , 'Y' as is_valid
		 , 0 as pieces
		 , 2 as category_code
		 , mon
		 , yr
	 from tmp_customclearance_pacemaker
	union all 
	select null as biz_no
		 , null as declaration_itemname
		 , null as commericalinvoice
		 , invoice
		 , null as document_finishpreparationdate
		 , null as customrelease_1
		 , null as chineselabelpicturereceiveddate
		 , null as commodityinspection_date 
		 , null as customrelease_2
		 , null as testscheduled_date 
		 , null as actualtest_date 
		 , null as ciq_signcompletiondate 
		 , null as taxpayment_applicationdate	 
		 , null as taxpayment_completiondate
		 , null as declaration_completiondate	 
		 , null as is_excluded
		 , null as is_malaysia
		 , null as abnormal_reason
		 , null as status 
		 , null as is_valid
		 , 0    as pieces
		 , -1 as category_code
		 , null as mon
		 , null as yr
	  from tmp_ie_invoicecategory
	 where category_code = -1
 ) inb_all
 left join tmp_inbounddeclaration idc on lpad(inb_all.invoice,20,'0') = lpad(idc.invoice,20,'0')
 where COALESCE (inb_all.yr,idc.yr)>='2000'
 group by idc.pickupdocument_no
     , idc.commericalinvoice
     , idc.invoice
	 , idc.emergencysigns
     , idc.forwarding 
     , idc.arrivalgoods_type 
     , idc.shipfrom_country 
     , idc.preinspection_flag 
     , idc.bscinformslcdate
	 , idc.t1pickupdate 
     , idc.etd
     , idc.eta
     , idc.reviseetd 
     , idc.reviseeta 
     , idc.actualarrivaltime 
     , idc.forwordinginformslcpick 
     , cast(idc.dockwarrantdate as timestamp)
     , cast(idc.intoinventorydate as timestamp) 
     , idc.inbounddeclaration_startdate
     , idc.inbounddeclaration_finishdate 
     , idc.picturetaken_date 
     , idc.inspection_appointmentdate 
     , idc.inspection_finishdate
     , idc.pieces
     , idc.mon
     , idc.yr
	 , cast(idc.coodraft_receiveddate as timestamp) 
	 , cast(idc.coocertificate_receiveddate as timestamp) 
	 , inb_all.biz_no
	 , inb_all.declaration_itemname 
	 , coalesce(inb_all.commericalinvoice, idc.commericalinvoice)
	 , inb_all.invoice
	 , inb_all.document_finishpreparationdate
	 , cast(inb_all.customrelease_1 as timestamp) 
	 , cast(inb_all.commodityinspection_date as timestamp) 
     , cast(inb_all.chineselabelpicturereceiveddate  as timestamp) 
	 , cast(inb_all.customrelease_2 as timestamp) 
	 , cast(inb_all.testscheduled_date  as timestamp) 
	 , cast(inb_all.actualtest_date  as timestamp) 
	 , cast(inb_all.ciq_signcompletiondate as timestamp) 
	 , inb_all.taxpayment_applicationdate
	 , inb_all.taxpayment_completiondate
	 , inb_all.declaration_completiondate
	 , inb_all.is_excluded
	 , inb_all.is_malaysia
	 , inb_all.abnormal_reason 
	 , inb_all.status 
	 , inb_all.is_valid
	 , inb_all.category_code
	 , case when inb_all.category_code = -1 and idc.intoinventorydate is null then '\u672a\u5230\u5e93' --未到库
            when inb_all.category_code = -1 and idc.intoinventorydate is not null then '\u5206\u62e8' --分拨
         else '\u975e\u5206\u62e8' end  --非分拨
	 , inb_all.pieces
	 , inb_all.mon
	 , inb_all.yr;
"

delete_sql="
drop table tmp_inbounddeclaration;
drop table tmp_customclearance;
drop table tmp_customclearance_pacemaker;
drop table tmp_ie_invoicecategory;
"	
# 2. 执行加载数据SQL
echo  "$sto_sql"
$hive -e "$sto_sql"
#$hive -e "$delete_sql"
echo "End syncing dwd_iekpi_e2e data into DWD layer on ${sync_date} .................."
 
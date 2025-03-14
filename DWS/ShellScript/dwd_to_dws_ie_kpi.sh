#!/bin/bash
# function:
#   sync up ie_kpi 
# history:
# 2022-07-08    donny   v1.0    init
#按天计算
# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # hadoop的配置路径
export LANG="en_US.UTF-8"
if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date  +%F)
	sync_year=$(date  +'%Y')
	last_mon=`date -d '1 month ago' +%Y-%m`
fi

echo "start syncing ie_kpi data into dws layer on $sync_date : $sync_year"
sto_sql="
-- 参数
use ${target_db_name};
set mapreduce.job.queuename = default;
set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions.pernode = 100000;
set hive.exec.max.dynamic.partitions = 100000;
add jar /user/hive/numDay-1.0-SNAPSHOT.jar;
create temporary function myudf as 'org.example.Nmu';
---set hive.exec.parallel=false;

drop table if exists tmp_ctm_shipstatus;
create table tmp_ctm_shipstatus stored as orc as
--- 最近30天的货物跟踪状态
--- workorder 为主键  -- appops：订单级别更新
select 
	housewaybillno
	,x.forwording
	, coalesce(y.supplier,x.forwording )as  supplier
	, case when y.need_calcaulated=1 then 1 else 0 end as need_calcaulated  --需考核flag
	,t1pickupdate --T1提货时间
	,dockwarrantdate --仓单确认
	,intoinventorydate	 --入仓时间
	,case when  substr(intoinventorydate,12,2)>=12 
			then cast(from_unixtime(unix_timestamp(date_add(substr(intoinventorydate,1,10),1))+9*60*60,'yyyy-MM-dd HH:mm:ss') as TIMESTAMP)
			else intoinventorydate
		end as intoinventorydate_kpi
	,updatedate 
	,worknumber as shipment_number
	,shipmenttype
	,substr(dockwarrantdate,1,7) as dockwa_mon
	,datediff(substr(dockwarrantdate,1,10),substr(t1pickupdate,1,10)) as ti_dock_Leadtime_day_cd
	,datediff(substr(dockwarrantdate,1,10),substr(t1pickupdate,1,10))-myudf(t1pickupdate,dockwarrantdate)  as ti_dock_Leadtime_day_wd  
	,datediff(substr(IntoInventoryDate,1,10),substr(dockwarrantdate,1,10)) as Inventory_dock_Leadtime_day_cd
	,datediff(substr(IntoInventoryDate,1,10),substr(dockwarrantdate,1,10))-myudf(dockwarrantdate,IntoInventoryDate)  as Inventory_dock_Leadtime_day_wd  
 from dwd_trans_ctmshipmentstatus x --国外发货到国内清关各时间节点TRANS_CTMShipmentStatus
 left join (select forwording,supplier,need_calcaulated from 
 			opsdw.dwd_dim_ie_supplier where dt=(select max(dt) from opsdw.dwd_dim_ie_supplier))y on x.forwording=y.forwording
where dt =(select max(dt) from dwd_trans_ctmshipmentstatus)
	and shipmenttype='Inbound_进境' 
	and substr(dockwarrantdate,1,10)>= date_add('$sync_date',-93)
	--and substr(dockwarrantdate,1,10)>= '2022-04-01';

drop table if exists tmp_ctm_iq;
create table tmp_ctm_iq stored as orc as
--- 通过workorder关联出upn和site
--- 单workorder，upn对应多行记录
    select shipment_number
		 , commercialinvoiceno as invoice
		 , upn
		 , broker
		 , sum(declqty) as delc_qty
		 , case when site='BSC_SH' then 'D835'
		  WHEN site='BSC_CD' then 'D836' 
		  WHEN site='BSC_TJ' then 'D837' end as site
	  from dwd_ctm_intergrationquery  ---TRANS_CTMIntegrationQuery
	 where dt >= date_add('$sync_date',-360)
	   and dt <= '$sync_date'
	   and ShipmentType='Inbound_进境'
  group by shipment_number
		 , commercialinvoiceno
		 , upn
		 , broker
		 , case when site='BSC_SH' then 'D835'
		  WHEN site='BSC_CD' then 'D836'
		  WHEN site='BSC_TJ' then 'D837'  end;
		 
drop table if exists tmp_ctm_ti;
create table tmp_ctm_ti stored as orc as
 select  
        x.invoice, 
		--x.delivery,
        max(coalesce(y.actual_migo_datetime,sap_migo_date)) as migo_date
    from (select invoice , delivery from ${target_db_name}.ods_commercial_invoice_dn_mapping 
			where x.dt = ( select max(dt) from ${target_db_name}.ods_commercial_invoice_dn_mapping max_dt)
			and x.delivery is not null )x  --源表TRANS_T1Invoice全量手工上传的
	left join (select distinct delivery_no,actual_migo_datetime
				from ${target_db_name}.dwd_fact_import_export_dn_info  --源表 TRANS_ImportExportDelivery sap下载的
				where dt >= date_add('$sync_date',-360)and dt <= '$sync_date' 
					and actual_migo_datetime is not null ) y on x.delivery=y.delivery_no
	group by x.invoice
	;	

drop table if exists tmp_ctm_shipinfo;
create table tmp_ctm_shipinfo stored as orc as
   select css.housewaybillno
		,css.forwording
		,css.supplier
		,css.need_calcaulated		
		,css.t1pickupdate
		,css.dockwarrantdate
		,css.intoinventorydate
		,css.intoinventorydate_kpi
		,css.updatedate
		,css.shipment_number
		,css.shipmenttype
		,css.dockwa_mon
		,css.ti_dock_Leadtime_day_cd
		,css.ti_dock_Leadtime_day_wd
		,css.Inventory_dock_Leadtime_day_cd
		,css.Inventory_dock_Leadtime_day_wd
        , ciq.invoice
        , ciq.upn
		, ciq.broker
		, ciq.delc_qty
		, ciq.site
		, ti.migo_date
		--, ti.delivery
		, ((unix_timestamp(ti.migo_date) - unix_timestamp(css.intoinventorydate))/(60 * 60) )  as migo_Inventory_Leadtime_day_cd
		, myudf(css.intoinventorydate,ti.migo_date)*24  as no_work_hr    
		, case when ((unix_timestamp(ti.migo_date) - unix_timestamp(css.intoinventorydate))/(60 * 60) )
            	 - myudf(css.intoinventorydate,ti.migo_date)*24 <0 then 0
      		 else ((unix_timestamp(ti.migo_date) - unix_timestamp(css.intoinventorydate))/(60 * 60))
            	 - myudf(css.intoinventorydate,ti.migo_date)*24 
       		end as migo_Inventory_Leadtime_day_wd 
		--, datediff(substr(ti.migo_date,1,10),substr(css.IntoInventoryDate,1,10)) as migo_Inventory_Leadtime_day_cd
		--,datediff(substr(ti.migo_date,1,10),substr(css.IntoInventoryDate,1,10))-myudf(css.IntoInventoryDate,ti.migo_date)  as migo_Inventory_Leadtime_day_wd  
     from tmp_ctm_shipstatus  css
  left join tmp_ctm_iq ciq on css.shipment_number = ciq.shipment_number
  left join tmp_ctm_ti ti  on int(ciq.invoice)=int(ti.invoice);  

insert overwrite table opsdw.dws_ie_kpi partition(dt) 
 select 'IE001' as kpicode 
		,site AS plant
        , housewaybillno
	    ,forwording
		,supplier
		,need_calcaulated		
		,t1pickupdate
		,dockwarrantdate
		,intoinventorydate
		,intoinventorydate_kpi
		,migo_date
		,shipment_number
		,shipmenttype
		,invoice
		,upn
		,sum(delc_qty) as qty
		,ti_dock_Leadtime_day_cd
		,case when ti_dock_Leadtime_day_wd<0 then 0 else ti_dock_Leadtime_day_wd end as ti_dock_Leadtime_day_wd
		,min(to_date(dockwarrantdate)) as dt
   from tmp_ctm_shipinfo
   where dockwarrantdate is not null
   and t1pickupdate is not null
 group by 
		housewaybillno
	    ,forwording
		,supplier
		,need_calcaulated		
		,t1pickupdate
		,dockwarrantdate
		,intoinventorydate
		,intoinventorydate_kpi
		,migo_date
		,shipment_number
		,shipmenttype
		,ti_dock_Leadtime_day_cd
		,case when ti_dock_Leadtime_day_wd<0 then 0 else ti_dock_Leadtime_day_wd end
		,invoice
		,upn
		,site
union all 
 select 'IE002' as kpicode 
		,site AS plant
        , housewaybillno
	    ,forwording
		,case when broker='上海畅联国际货运有限公司' then 'SLC' else supplier end as supplier
		,case when broker='上海畅联国际货运有限公司' then 1 ELSE 0 END AS need_calcaulated		
		,t1pickupdate
		,dockwarrantdate
		,intoinventorydate
		,intoinventorydate_kpi
		,migo_date
		,shipment_number
		,shipmenttype
		,invoice
		,upn
		,sum(delc_qty) as qty
		,Inventory_dock_Leadtime_day_cd
		,case when Inventory_dock_Leadtime_day_wd<0 then 0 else Inventory_dock_Leadtime_day_wd end as Inventory_dock_Leadtime_day_wd
		,min(to_date(intoinventorydate)) as dt
   from tmp_ctm_shipinfo
   where intoinventorydate is not null
   and dockwarrantdate is not null
   and intoinventorydate<>'1899-12-30 00:00:00.000'
 group by 
		housewaybillno
	    ,forwording
		,case when broker='上海畅联国际货运有限公司' then 'SLC' else supplier end	
		,case when broker='上海畅联国际货运有限公司' then 1  ELSE 0 END		
		,t1pickupdate
		,dockwarrantdate
		,intoinventorydate
		,intoinventorydate_kpi
		,migo_date
		,shipment_number
		,shipmenttype
		,Inventory_dock_Leadtime_day_cd
		,case when Inventory_dock_Leadtime_day_wd<0 then 0 else Inventory_dock_Leadtime_day_wd end 
		,invoice
		,upn
		,site
union all 
 select 'WH014' as kpicode 
		,site AS plant
       -- , delivery as housewaybillno
	   ,housewaybillno
	    ,forwording
		,case when broker='上海畅联国际货运有限公司' then 'SLC' else supplier end as supplier
		,case when broker='上海畅联国际货运有限公司' then 1  ELSE 0 END AS need_calcaulated		
		,t1pickupdate
		,dockwarrantdate
		,intoinventorydate
		,intoinventorydate_kpi
		,migo_date
		,shipment_number
		,shipmenttype
		,invoice
		,upn
		,sum(delc_qty) as qty
		,CASE WHEN migo_Inventory_Leadtime_day_cd<0 then 0 else  migo_Inventory_Leadtime_day_cd end as migo_Inventory_Leadtime_day_cd
		,case when migo_Inventory_Leadtime_day_wd<0 then 0 else migo_Inventory_Leadtime_day_wd end as migo_Inventory_Leadtime_day_wd
		,min(to_date(migo_date)) as dt
   from tmp_ctm_shipinfo
   where migo_date is not null
   and intoinventorydate is not null
    and intoinventorydate<>'1899-12-30 00:00:00.000'
 group by 
		--delivery
		housewaybillno
	    ,forwording
		,case when broker='上海畅联国际货运有限公司' then 'SLC' else supplier end
		,case when broker='上海畅联国际货运有限公司' then 1  ELSE 0 END		
		,t1pickupdate
		,dockwarrantdate
		,intoinventorydate
		,intoinventorydate_kpi
		,migo_date
		,shipment_number
		,shipmenttype
		,CASE WHEN migo_Inventory_Leadtime_day_cd<0 then 0 else  migo_Inventory_Leadtime_day_cd end
		,case when migo_Inventory_Leadtime_day_wd<0 then 0 else migo_Inventory_Leadtime_day_wd end
		,invoice
		,upn
		,site
"
delete_tmp="
drop table tmp_ctm_shipstatus;
drop table tmp_ctm_iq;
drop table tmp_ctm_ti;
drop table tmp_ctm_shipinfo;
"
# 2. 执行加载数据SQL
echo "$sto_sql"
$hive -e "$sto_sql"
echo "four $delete_tmp"
#$hive -e "$delete_tmp"

echo "End syncing dws_ie_kpi data into DWS layer on ${sync_date} .................."	
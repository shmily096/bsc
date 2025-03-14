#!/bin/bash
# Function:
#   sync up SO,STO,WO by QR code 
# History:
# 2021-7-06    Donny   v1.0    init

# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

#脚本描述：以运单明细表做主表取qrcode对应的工单号和dn 以及订单表对应的订单创建时间和发货仓库


if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date  +%F)
fi

if [ -n "$2" ] ;then 
    sync_year=$2
else
    sync_year=$(date  +'%Y')
fi


echo "start syncing data into dws layer on $sync_date :$sync_year .................."


sql_str="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
--set hive.exec.mode.local.auto=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;



drop table if exists tmp_dws_so_sto_wo_daily_trans_so_dn_ng_d835;
create table tmp_dws_so_sto_wo_daily_trans_so_dn_ng_d835 stored as orc as
       select  so_no 
              ,delivery_id as so_dn_no 
              ,batch 
              ,material 
              ,qr_code
              ,chinese_dncreatedt as dn_detail_dt
       from dwd_fact_sales_order_dn_detail
	   ---主表
       where qr_code is not null  
       and qr_code <> ''
        and dt >=date_add('$sync_date',-8)  
    and chinese_dncreatedt>=date_add('$sync_date',-7)
       group by 
              so_no 
              ,delivery_id 
              ,batch 
              ,material 
              ,qr_code
              ,chinese_dncreatedt;
              
drop table if exists tmp_dws_so_sto_wo_daily_trans_wo_qr_ng_d835;
create table tmp_dws_so_sto_wo_daily_trans_wo_qr_ng_d835 stored as orc as             
    select  work_order_no 
              ,dn_no as import_dn 
              ,qr_code
       from dwd_fact_work_order_qr_code_mapping_new 
	   ---第一次关联表
       where dt >=date_add('$sync_date',-400)
	   AND qr_code IN (SELECT qr_code FROM tmp_dws_so_sto_wo_daily_trans_so_dn_ng_d835)
       group by
              work_order_no 
              ,dn_no
              ,qr_code;
drop table if exists tmp_dws_so_sto_wo_daily_trans_so_ng_d835;
create table tmp_dws_so_sto_wo_daily_trans_so_ng_d835 stored as orc as  
           select  so_no 
              ,chinese_socreatedt 
              ,pick_up_plant
       from dwd_fact_sales_order_info
	   --第二次关联表
       where lower(pick_up_plant)= lower('D835')
        and dt>=date_add('$sync_date',-300)
        and lower(order_type) in ('or','znc','fd','kb')
		AND so_no IN (SELECT DISTINCT so_no FROM tmp_dws_so_sto_wo_daily_trans_so_dn_ng_d835)
       group by so_no 
              ,chinese_socreatedt
              ,pick_up_plant;   
			  
drop table if exists tmp_dws_so_sto_wo_daily_trans_so_qr_d835;
create table tmp_dws_so_sto_wo_daily_trans_so_qr_d835 stored as orc as 
--主表和第一关联表关联
select   
        so_dn_ng_d835.material 
       ,so_dn_ng_d835.batch 
       ,so_dn_ng_d835.so_dn_no 
       ,so_dn_ng_d835.so_no 
       ,so_dn_ng_d835.qr_code 
       ,wo_qr_ng_d835.work_order_no 
       ,wo_qr_ng_d835.import_dn
       ,so_dn_ng_d835.dn_detail_dt     
       ,so_dn_ng_d835.dn_detail_dt as dt       
from tmp_dws_so_sto_wo_daily_trans_so_dn_ng_d835 so_dn_ng_d835
inner join tmp_dws_so_sto_wo_daily_trans_wo_qr_ng_d835 wo_qr_ng_d835 
on wo_qr_ng_d835.qr_code = so_dn_ng_d835.qr_code;
--关联完成删除临时表
drop table tmp_dws_so_sto_wo_daily_trans_so_dn_ng_d835;
drop table tmp_dws_so_sto_wo_daily_trans_wo_qr_ng_d835;

drop table if exists tmp_dws_so_sto_wo_daily_trans;
create table tmp_dws_so_sto_wo_daily_trans stored as orc as  
--第一次关联后和第二表关联
select 
        so_dn_ng_d835.material 
       ,so_dn_ng_d835.batch 
       ,so_dn_ng_d835.so_dn_no 
       ,so_dn_ng_d835.so_no 
       ,so_dn_ng_d835.qr_code 
       ,so_dn_ng_d835.work_order_no 
       ,so_dn_ng_d835.import_dn
       ,so_dn_ng_d835.dn_detail_dt
	   ,so_ng_d835.chinese_socreatedt
       ,so_dn_ng_d835.dn_detail_dt as dt
       ,so_ng_d835.pick_up_plant as plant	   
from tmp_dws_so_sto_wo_daily_trans_so_qr_d835 so_dn_ng_d835
inner join tmp_dws_so_sto_wo_daily_trans_so_ng_d835 so_ng_d835 on so_ng_d835.so_no = so_dn_ng_d835.so_no;

--关联完成删除临时表
drop table tmp_dws_so_sto_wo_daily_trans_so_qr_d835;
drop table tmp_dws_so_sto_wo_daily_trans_so_ng_d835;

insert overwrite table dws_so_sto_wo_daily_trans partition(dt, plant)
select   
        x.material 
       ,x.batch 
       ,x.so_dn_no 
       ,x.so_no 
       ,x.qr_code 
       ,null as domestic_sto_dn
       ,null as domestic_sto
       ,x.work_order_no 
       ,x.import_dn
       ,x.dn_detail_dt
       ,x.chinese_socreatedt
       ,substr(x.dt,1,10) as dt
       ,x.plant    
from tmp_dws_so_sto_wo_daily_trans x
group by x.material 
       ,x.batch 
       ,x.so_dn_no 
       ,x.so_no 
       ,x.qr_code 
       ,x.work_order_no 
       ,x.import_dn
       ,x.dn_detail_dt
       ,x.chinese_socreatedt
       ,substr(x.dt,1,10)
       ,x.plant
	   ;
"
delete_tmp="
drop table tmp_dws_so_sto_wo_daily_trans;
"
# 2. 执行加载数据SQL
echo "$sql_str"
$hive -e "$sql_str"
echo "$delete_tmp"
$hive -e "$delete_tmp"
echo "End syncing data into DWS layer on $sync_year : $sync_date .................."
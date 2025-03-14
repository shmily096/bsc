#!/bin/bash
# Function:
#   sync up sales order leadtime of product life cycle 
# History:
# 2021-07-05    Donny   v1.0    init

# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

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

echo "start syncing data into dws layer on $sync_year :$sync_date .................."


sql_str="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
--set hive.exec.max.created.files=100000;
--set parquet.memory.min.chunk.size=100000;
--set hive.input.format=org.apache.hadoop.hive.ql.io.hiveinputformat;
--set hive.exec.reducers.max=8;
--set mapred.reduce.tasks=8;
--set hive.exec.parallel=false;
drop table if exists tmp_dws_plc_so_daily_trans_so_sto_wo;
create table tmp_dws_plc_so_daily_trans_so_sto_wo stored as orc as
 select
    material
    ,batch
    ,qr_code
    ,so_dn_no
    ,so_no
    ,import_dn
    ,work_order_no
    ,MAX(domestic_sto_dn) AS domestic_sto_dn
    from dws_so_sto_wo_daily_trans
    where dt >=date_add('$sync_date',-7) ---最近7天运单创建的中国时间
	and dt<='$sync_date'
    group by
       material
      ,batch
      ,qr_code
      ,so_dn_no
      ,so_no
      ,import_dn
      ,work_order_no
      ;
drop table if exists tmp_dws_plc_so_daily_trans_so_dn;
create table tmp_dws_plc_so_daily_trans_so_dn stored as orc as
 select 
        so_no
       ,delivery_id
       ,created_datetime  as so_dn_create_dt
       ,actual_gi_date as so_dn_pgi
       ,coalesce(receiving_confirmation_date,actual_gi_date) as so_customer_receive_dt
    from dwd_fact_sales_order_dn_info
    where dt >=date_add('$sync_date',-7)  ---最近7天创建运单的
	and dt<='$sync_date'
    and month(actual_gi_date)>1
    group by  
        so_no
       ,delivery_id
       ,created_datetime
       ,actual_gi_date
       ,coalesce(receiving_confirmation_date,actual_gi_date);
drop table if exists tmp_dws_plc_so_daily_trans_so;
create table tmp_dws_plc_so_daily_trans_so stored as orc as
select
        soi.material,
        soi.batch,
        soi.so_no,
        soi.so_create_dt,
        soi.customer_code,
        soi.is_transfer_whs,
        cust_so.level1_code,
        cust_so.level2_code,
        cust_so.level3_code,
        cust_so.level4_code
    from
    (
        select
            material, 
            batch,
            so_no ,
            created_datetime as so_create_dt ,
            customer_code,
            if(lower(pick_up_plant)= lower('D838'), 'true', 'false') as is_transfer_whs
        from
            dwd_fact_sales_order_info
        where dt >=date_add('$sync_date',-100)  --订单创建100天内
		and dt<='$sync_date'
        group by
            material,
            batch,
            so_no ,
            created_datetime,
            customer_code,
            pick_up_plant

    ) soi
    left join 
    (
        select distinct
            cust_account ,
            level1_code ,
            level2_code ,
            level3_code ,
            level4_code
        from
            dwd_dim_customer
        where dt >=date_add('$sync_date',-3)
		and dt<='$sync_date'
    ) cust_so on cust_so.cust_account = soi.customer_code 
    group by 
        soi.material,
        soi.batch,
        soi.so_no,
        soi.so_create_dt,
        soi.customer_code,
        soi.is_transfer_whs,
        cust_so.level1_code,
        cust_so.level2_code,
        cust_so.level3_code,
        cust_so.level4_code;
drop table if exists tmp_dws_plc_so_daily_trans_sku;
create table tmp_dws_plc_so_daily_trans_sku stored as orc as
  select 
        material_code as material
        ,division_display_name as dvision
        ,sub_division
        ,product_line1_name 
        ,product_line2_name
        ,product_line3_name 
        ,product_line4_name 
        ,product_line5_name 
        ,business_group as item_type
    from
        dwd_dim_material
    where
        sub_division is not null 
        and sub_division <> ''
		--and dt='2022-08-31'  --重刷用
        and dt in ( select max(dt) from dwd_dim_material where dt>=date_sub('$sync_date',10))
    group by 
        material_code
        ,division_display_name
        ,sub_division
        ,product_line1_name 
        ,product_line2_name
        ,product_line3_name 
        ,product_line4_name 
        ,product_line5_name 
        ,business_group;
--with parts

drop table if exists tmp_dws_plc_so_daily_trans_one;
create table tmp_dws_plc_so_daily_trans_one stored as orc as
select 
    distinct
    so_sto_wo.material
    ,so_sto_wo.batch
    ,so_sto_wo.qr_code
    ,so_sto_wo.import_dn
    ,so_sto_wo.work_order_no
    ,so_sto_wo.domestic_sto_dn
    ,so_dn.so_dn_create_dt
    ,so_dn.so_dn_pgi
    ,so_dn.so_customer_receive_dt
	,so_sto_wo.so_no
from tmp_dws_plc_so_daily_trans_so_sto_wo so_sto_wo
inner join tmp_dws_plc_so_daily_trans_so_dn so_dn 
		on so_dn.delivery_id=so_sto_wo.so_dn_no and so_dn.so_no=so_sto_wo.so_no;
---删除临时表			
drop table tmp_dws_plc_so_daily_trans_so_sto_wo;
drop table tmp_dws_plc_so_daily_trans_so_dn;	

drop table if exists tmp_dws_plc_so_daily_trans_two;
create table tmp_dws_plc_so_daily_trans_two stored as orc as	
select 
    distinct
    so_sto_wo.material
    ,so_sto_wo.batch
    ,so_sto_wo.qr_code
    ,so_sto_wo.import_dn
    ,so_sto_wo.work_order_no
    ,so_sto_wo.domestic_sto_dn
    ,so.is_transfer_whs
    ,so.customer_code
    ,so.level1_code
    ,so.level2_code
    ,so.level3_code
    ,so.level4_code
    ,so.so_create_dt
    ,so_sto_wo.so_dn_create_dt
    ,so_sto_wo.so_dn_pgi
    ,so_sto_wo.so_customer_receive_dt
from tmp_dws_plc_so_daily_trans_one so_sto_wo
inner join tmp_dws_plc_so_daily_trans_so so 
		on so_sto_wo.so_no = so.so_no and so_sto_wo.material=so.material ;
		
---删除临时表			
drop table tmp_dws_plc_so_daily_trans_one;
drop table tmp_dws_plc_so_daily_trans_so;	


		
insert overwrite table ${target_db_name}.dws_plc_so_daily_trans partition(dt)
select 
    distinct
    so_sto_wo.material
    ,so_sto_wo.batch
    ,so_sto_wo.qr_code
    ,so_sto_wo.import_dn
    ,so_sto_wo.work_order_no
    ,so_sto_wo.domestic_sto_dn
    ,so_sto_wo.is_transfer_whs
    ,sku.dvision
    ,sku.sub_division
    ,sku.product_line1_name
    ,sku.product_line2_name
    ,sku.product_line3_name 
    ,sku.product_line4_name 
    ,sku.product_line5_name 
    ,sku.item_type
    ,so_sto_wo.customer_code
    ,so_sto_wo.level1_code
    ,so_sto_wo.level2_code
    ,so_sto_wo.level3_code
    ,so_sto_wo.level4_code
    ,so_sto_wo.so_create_dt
    ,so_sto_wo.so_dn_create_dt
    ,so_sto_wo.so_dn_pgi
    ,so_sto_wo.so_customer_receive_dt
    ,date_format(so_sto_wo.so_dn_pgi,'yyyy-MM-dd') as dt_date
from tmp_dws_plc_so_daily_trans_two so_sto_wo
left join tmp_dws_plc_so_daily_trans_sku sku on sku.material=so_sto_wo.material
; 
---删除临时表			
drop table tmp_dws_plc_so_daily_trans_two;
drop table tmp_dws_plc_so_daily_trans_sku;
"
# 2. 执行加载数据SQL
$hive -e "$sql_str"

echo "End syncing data into DWS layer on $sync_year : $sync_date .................."
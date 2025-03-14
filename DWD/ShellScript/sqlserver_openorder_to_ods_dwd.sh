#!/bin/bash
#   sync up BSC APP data to HDFS
# History:
#   2021-05-07    Donny   v1.0    draft
#   2021-05-10    Donny   v1.1    update connection string & other table sync

# 设置sqoop工具路径
sqoop="/opt/module/sqoop/bin/sqoop"

# 设置同步的数据库
sync_db='bsc_app_ops'

# 设置数据库连接字符串
connect_str_mysql="jdbc:mysql://172.25.48.1:3306/$sync_db"
connect_str_sqlserver="jdbc:sqlserver://10.226.99.103:16000;username=opsWin;password=opsWinZaq1@wsx;database=APP_OPS;"

# 同步日期设置，默认同步前一天数据
if [ -n "$2" ]; then
    sync_date=$2
else
    sync_date=$(date  +%F)
fi
yesterday=$(date -d '-1 day' +%F)
this_year=`date -d "${sync_date}" +%Y-01`
#同步SQl Server数据通过sqoop
sync_data_sqlserver() {
    echo "${sync_date} stat syncing........"
    hdfs dfs -mkdir -p /bsc/origin_data/$sync_db/$1/$sync_date
    $sqoop import \
        --connect "$connect_str_sqlserver" \
        --target-dir /bsc/origin_data/$sync_db/$1/$sync_date \
        --delete-target-dir \
        --query "$2 and \$CONDITIONS" \
        --num-mappers 1 \
        --fields-terminated-by '\001' \
        --compress \
        --compression-codec lzop \
        --null-string '\\N' \
        --null-non-string '\\N'

    hadoop jar /opt/module/hadoop3/share/hadoop/common/hadoop-lzo-0.4.20.jar \
        com.hadoop.compression.lzo.DistributedLzoIndexer \
        /bsc/origin_data/$sync_db/$1/$sync_date

    echo "${sync_date} end syncing........"
}
# 同步销售发货单
sync_trans_openordercn_info() {
    sync_data_sqlserver "trans_openordercn" "SELECT 
                                            UpdateDT,
                                            Active,
                                            SalesOrg, 
                                            Customer,
                                            DistCh,
                                            Div,
                                            AcctGrp, 
                                            Rep,
                                            Curr,
                                            Document, 
                                            Line, 
                                            CustPoNumber, 
                                            DocDate, 
                                            [Type],
                                            DcTp,
                                            Material,
                                            Descript, 
                                            PrfCtr,
                                            Batch,
                                            Expire, 
                                            Plant, 
                                            SLoc,
                                            Qty,
                                            UOM,
                                            BO,
                                            HB, 
                                            IB, 
                                            UserName,
                                            Rsn,
                                            NetValue, 
                                            CustomerName,
                                            SalesRepName,
                                            POOrg,
                                            CustPoTy,
                                            InvoiceHeaderText,
                                            PurchReq,
                                            StkPlant,
                                            BostonScientificInternalText,
                                            BostonScientificInternalItemText, 
                                            StockLocationPartnerNumber, 
                                            StockLocationPartnerName, 
                                            InsertDate
                                FROM APP_OPS.dbo.TRANS_OpenOrderCN
                        where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'
						 "
											}
sync_trans_delivery_intrans_info() {
    sync_data_sqlserver "trans_delivery_intrans" "SELECT 
                                                    ID, 
                                                    UpdateDT,
                                                    Active, 
                                                    ECCOutboundDN, 
                                                    ECCInboundDN, 
                                                    S4OutboundDN,
                                                    Creation, 
                                                    SendingPlant,
                                                    SendingSLOC, 
                                                    PhysicalSendingPlant,
                                                    RecevingPlant, 
                                                    RecevingSLOC,
                                                    PhysicalRecevingPlant,
                                                    PDT, 
                                                    PGIDATE,
                                                    STONo, 
                                                    STOCreatiionDate,
                                                    ProfitCenter,
                                                    DNLineNo,
                                                    Material, 
                                                    BATCH,
                                                    Quantity,
                                                    ExpirationDate,
                                                    MDSLOC,
                                                    STOFlag,
                                                    PaceMakerFlag,
                                                    isOversea
                                            FROM APP_OPS.dbo.TRANS_Delivery_InTrans
                                            ---where 1=1
                                    where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'
						 "
											}
# 按业务分类同步数据
if [ "$1"x = "trans_openordercn"x ];then
	echo "$1 only run"
	sync_trans_openordercn_info 
elif [ "$1"x = "trans_delivery_intrans"x ];then
    echo " $1 only run"
	echo "$sync_date  ok"
	sync_trans_delivery_intrans_info
else
    echo "failed run"

fi    

# 设置必要的参
target_db_name='opsdw'
origin_db_name='bsc_app_ops' #原始数据库
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

#  1.业务数据SQL
#Load data into table ods_trans_openordercn from hdfs trans_openordercn by partition
ods_trans_openordercn_sql="
load data inpath '/bsc/origin_data/$origin_db_name/trans_openordercn/$sync_date' overwrite
into table ${target_db_name}.ods_trans_openordercn
partition(dt='$sync_date');
"
ods_trans_delivery_intrans_sql="
load data inpath '/bsc/origin_data/$origin_db_name/trans_delivery_intrans/$sync_date' overwrite
into table ${target_db_name}.ods_trans_delivery_intrans
partition(dt='$sync_date');
"
# 2. 执行加载数据SQL
if [ "$1"x = "trans_openordercn"x ];then
	echo "$1 only run"
	$hive -e"$ods_trans_openordercn_sql"
elif [ "$1"x = "trans_delivery_intrans"x ];then
    echo " $1 only run"
	echo "$sync_date  ok"
	$hive -e"$ods_trans_delivery_intrans_sql"
else
    echo "failed run"

fi   


echo "End loading data on {$sync_date} ..hdfs to ods_trans_openordercn........................................................"


echo "start syncing so dn data into DWD layer on ${sync_date} .................."
###############################################ods_dwd
export LANG="en_US.UTF-8"
echo "start syncing ODS TO DWD  layer on ${sync_date} ................."
# 设置必要的参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径
dwd_dim_customer_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_customer | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`
dwd_dim_material_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_material | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`
dwd_dim_division_rebate_rate_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_division_rebate_rate | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`
# 1 Hive SQL string
so_sql="
use ${target_db_name};
-- 参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;

drop table if exists tmp_dwd_fact_trans_openordercn_socri;
create table tmp_dwd_fact_trans_openordercn_socri stored as orc as
     select CAST(CAST(so_no as int) as string) as so_no
	      , min(to_date(request_delivery_date)) as request_delivery_date
	   from dwd_salesorder_createdinfo  --源表:TRANS_SalesOrder_CreatedInfo
	 where dt_month>='$this_year'
    group by CAST(CAST(so_no as int) as string);

drop table if exists tmp_dwd_fact_trans_openordercn_cust;
create table tmp_dwd_fact_trans_openordercn_cust stored as orc as
    select distinct 
        cust_account
        ,level3_code
        ,level4_code as customer_type
    from dwd_dim_customer  --源表:MDM_CustomerMaster,次表MDM_CustomerMaster_KNB1,MDM_CustomerMaster_KNVI,ods_customer_level
    where dt  in (select max(dt) from dwd_dim_customer where dt>=date_sub('$dwd_dim_customer_maxdt',7))
      ---='$dwd_dim_customer_maxdt'
;

drop table if exists tmp_dwd_fact_trans_openordercn_sku;
create table tmp_dwd_fact_trans_openordercn_sku stored as orc as
    select distinct
         bu_sku.material_code 
        ,bu_sku.division_id
        ,bu_sku.division_display_name
        ,bu_sku.default_location
        ,bu_sku.sub_division
        ,bu_sku.sub_division_bak
        ,bu_sku.business_group
    from dwd_dim_material bu_sku --源表主表MDM_MaterialMaster 全量更新,次表MDM_DivisionMaster 全量更新
    where bu_sku.dt ='$dwd_dim_material_maxdt'
    ---in (select max(dt) from dwd_dim_material max_dt where dt>=date_sub('$sync_date',10))
;

drop table if exists tmp_dwd_fact_trans_openordercn_rebate;
create table tmp_dwd_fact_trans_openordercn_rebate stored as orc as
    select * from dwd_dim_division_rebate_rate --不定时手工维护
    where dt='$dwd_dim_division_rebate_rate_maxdt' 
    ---in (select max(dt) from dwd_dim_division_rebate_rate)
;
drop table if exists tmp_dwd_fact_trans_openordercn_default_rebate;
create table tmp_dwd_fact_trans_openordercn_default_rebate stored as orc as
    select division, default_rate,cust_business_type
    from tmp_dwd_fact_trans_openordercn_rebate
    group by division, default_rate,cust_business_type
;
drop table if exists tmp_dwd_fact_trans_openordercn_default_cust_rebate;
create table tmp_dwd_fact_trans_openordercn_default_cust_rebate stored as orc as
    select dealer_code
         , bu
         , dealer_type
         , rebate_rate
         , upn
         , calculation_type ---- by customer
      from dwd_dim_customer_rebaterate
     where upn is null
       and start_date <= '${sync_date}'
       and end_date >= '${sync_date}'
       and calculation_type = '1'
    union all 
    select dealer_code
         , bu
         , dealer_type
         , rebate_rate
         , upn
         , calculation_type ---- by customer & UPN
      from dwd_dim_customer_rebaterate
     where upn is null
       and start_date <= '${sync_date}'
       and end_date >= '${sync_date}'
       and calculation_type = '2';


drop table if exists tmp_dwd_fact_trans_openordercn;
create table tmp_dwd_fact_trans_openordercn stored as orc as
select 
    updatedt
    ,active
    ,salesorg
    ,int(cn.customer) as customer
    ,distch
    ,divv
    ,acctgrp
    ,rep
    ,curr
    ,int(cn.document) as document
    ,line
    ,custponumber as po_number
    ,docdate
    ,typer
    ,dctp
    ,material
    ,descript
    ,prfctr
    ,batch
    ,expire
    ,plant
    ,sloc
    ,qty
    ,uom
    ,bo
    ,hb
    ,ib
    ,username
    ,rsn
    ,netvalue
    ,customername
    ,salesrepname
    ,poorg
    ,custpoty
    ,invoiceheadertext
    ,purchreq
    ,stkplant
    ,bostonscientificinternaltext
    ,bostonscientificinternalitemtext
    ,stocklocationpartnernumber
    ,stocklocationpartnername
    ,insertdate
    ,socri.request_delivery_date
    ,cust.level3_code
    ,cust.customer_type
    ,sku.division_display_name as division
    ,sku.default_location
    ,sku.sub_division
    ,sku.sub_division_bak
    ,sku.business_group
from ods_trans_openordercn cn
left join tmp_dwd_fact_trans_openordercn_socri socri on int(cn.document)=int(socri.so_no )
left join tmp_dwd_fact_trans_openordercn_cust cust on int(cn.customer)=int(cust.cust_account)
left join tmp_dwd_fact_trans_openordercn_sku sku on cn.material=sku.material_code
where dt='$sync_date' and insertdate=date_add('$sync_date',-1);


drop table if exists tmp_dwd_fact_trans_openordercn_combination_rebate;
create table tmp_dwd_fact_trans_openordercn_combination_rebate stored as orc as
select 
	aa.po_number,
	aa.sub_division_bak,
	rr.product_level,
	rr.combo_group,
	rr.rate
from (select 
			distinct
			division,
			po_number, 
			sub_division_bak 
		from tmp_dwd_fact_trans_openordercn  --取出bu和匹配的字段拿来匹配
		where division is not null 
			  and sub_division_bak is not null 
			  and po_number is not null) aa
inner join dwd_combination_rebate rr  
    on aa.division=rr.bu
	and aa.sub_division_bak=rr.product_level
    and rr.start_date<='${sync_date}'
	and rr.end_date>='${sync_date}'
;

drop table if exists tmp_dwd_fact_trans_openordercn_combination_rebate_list;
create table tmp_dwd_fact_trans_openordercn_combination_rebate_list stored as orc as
select  distinct po_number,combx,rate --最后炸开去重
	from (
	SELECT po_number,combo_group,concat_ws(',',combo) as combo,rate   --数组转换成字符串
	FROM (
	SELECT po_number,combo_group,rate,combo,
	case when concat_ws(',',combo_group) =concat_ws(',',combo)  then 1 else 0 end as rn  --把数组转换成字符串比较出相等的标记为1
	FROM (	
		SELECT 
			po_number,
			sort_array(split(replace(replace(combo_group,'\[\"',''),'\"\]',''),'\"\,\"'))as combo_group, --把字符串转换成数组再排序
			rate,
			sort_array(collect_set(sub_division_bak)) AS combo  --把字段转换成数组再排序
		FROM tmp_dwd_fact_trans_openordercn_combination_rebate
		GROUP BY po_number,combo_group,rate)X)Y
		WHERE Y.rn=1)z
	lateral view explode(split(replace(replace(replace(combo,' ',''),',','/'),'，','/'),'/')) t as combx
;

-- sync up SQL string
insert overwrite table dwd_fact_openordercn 
 select 
     cn.updatedt
    ,cn.active
    ,cn.salesorg
    ,cn.customer
    ,cn.distch
    ,cn.divv
    ,cn.acctgrp
    ,cn.rep
    ,cn.curr
    ,cn.document
    ,cn.line
    ,cn.po_number
    ,cn.docdate
    ,cn.typer
    ,cn.dctp
    ,cn.material
    ,cn.descript
    ,cn.prfctr
    ,cn.batch
    ,cn.expire
    ,cn.plant
    ,cn.sloc
    ,cn.qty
    ,cn.uom
    ,cn.bo
    ,cn.hb
    ,cn.ib
    ,cn.username
    ,cn.rsn
    ,cn.netvalue
    ,cn.customername
    ,cn.salesrepname
    ,cn.poorg
    ,cn.custpoty
    ,cn.invoiceheadertext
    ,cn.purchreq
    ,cn.stkplant
    ,cn.bostonscientificinternaltext
    ,cn.bostonscientificinternalitemtext
    ,cn.stocklocationpartnernumber
    ,cn.stocklocationpartnername
    ,cn.insertdate
    ,cn.request_delivery_date
    ,cn.level3_code
    ,cn.customer_type
    ,cn.division
    ,cn.default_location
    ,cn.sub_division
    ,cn.sub_division_bak
    ,cn.business_group
    ,coalesce( combination_rebate_list.rate,
			case when upper(cn.po_number) like '%SP' and cn.sub_division_bak in ('LeVeen针电极','Soloist针电极') then 0
              when upper(cn.po_number) like '%SP' and cn.material in ('M00562301','M00562321','M00562391'
                                                                                                    ,'M00561311'
                                                                                                    ,'M00562451'
                                                                                                    ,'M00562341'
                                                                                                    ,'M00562371') then 0
          else coalesce( case when rebate.division='EP' AND cn.insertdate>='2023-10-01' THEN 0.031 ELSE rebate.rate END, default_rebate.default_rate, 0) + coalesce(cust_rebate.rebate_rate,0) end ) as rebate_rate 
from tmp_dwd_fact_trans_openordercn cn
left outer join tmp_dwd_fact_trans_openordercn_rebate rebate on cn.division = rebate.division 
    and array_contains(rebate.cust_business_type,cn.level3_code)
    and rebate.sub_divison = cn.sub_division_bak
left outer join tmp_dwd_fact_trans_openordercn_default_rebate default_rebate on default_rebate.division = cn.division and array_contains(default_rebate.cust_business_type,cn.level3_code)
---left outer join tmp_dwd_fact_trans_openordercn_default_cust_rebate cust_rebate on cn.division = cust_rebate.bu and cn.customer = cust_rebate.dealer_code
left outer join (
           select  dealer_code
                 , bu
                 , rebate_rate
		    from tmp_dwd_fact_trans_openordercn_default_cust_rebate
		   where calculation_type = '1') cust_rebate on cn.division = cust_rebate.bu and cn.customer = cust_rebate.dealer_code
left outer join (
           select  dealer_code
                 , upn
                 , rebate_rate
		    from tmp_dwd_fact_sales_order_invoice_cust_rebate
		   where calculation_type = '2') cust_rebate2 on cn.material = cust_rebate2.upn and cn.customer = cust_rebate2.dealer_code
left outer join (select po_number,combx,rate from tmp_dwd_fact_trans_openordercn_combination_rebate_list 
                    where upper(po_number) like '%BM' )combination_rebate_list 
	on cn.po_number=combination_rebate_list.po_number 
	and cn.sub_division_bak=combination_rebate_list.combx
    ;
"

dwd_trans_delivery_intrans_sql="
use ${target_db_name};
-- 参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
insert overwrite table dwd_trans_delivery_intrans partition(dt)
SELECT 
    ECCOutboundDN,
    ECCInboundDN,
    S4OutboundDN, 
    Creation,
    SendingPlant,
    SendingSLOC,
    PhysicalSendingPlant, 
    RecevingPlant, 
    RecevingSLOC, 
    PhysicalRecevingPlant, 
    PDT, 
    PGIDATE, 
    STONo, 
    STOCreatiionDate, 
    ProfitCenter,
    DNLineNo, 
    Material, 
    BATCH, 
    Quantity,
    ExpirationDate,
    MDSLOC, 
    STOFlag, 
    PaceMakerFlag,
    isOversea,
    date(Creation) as dt
FROM ods_trans_delivery_intrans
where dt='${sync_date}' and isOversea>=0;
"
# 2. 执行SQL
if [ "$1"x = "trans_openordercn"x ];then
	echo "$1 only run"
    count=`$hive -e "select count(*) from opsdw.ods_trans_openordercn where dt='$sync_date' and insertdate>=date_add('$sync_date',-1)" | tail -n1`

    if [ $count -eq 0 ]; then
    echo "Error: Failed to import data, count is zero."
    exit 1
    fi
	$hive -e "$so_sql"
elif [ "$1"x = "trans_delivery_intrans"x ];then
    echo " $1 only run"
	echo "$sync_date  ok"
	$hive -e"$dwd_trans_delivery_intrans_sql"
else
    echo "dwd_putaway_info failed"
fi 
echo "End syncing so dn data into DWD layer on ${sync_date} .................." 

#!/bin/bash
# Function:
#   sync up PG data to HDFS template
# History:
#   2022-03-31    slc   v1.0    init
#变量1 源表名这里是wo_qrcode/le_srr 不填就是两个都跑 只控制(pg到hdfs和ods到dwd) hdfs到ods会两个表都跑
#变量2 源库名这里是tableaudb  目标库也是这个
#变量3分区时间  这个不填就是默认当天  可以指定时间wo_qrcode是源表时间和目标分区时间，le_srr是目标分区时间
#文件存储路径：cd /bscflow/dwd
#只跑wo_qrcode： sh pg_to_hdfst_to_ods_to_dwd.sh wo_qrcode 
#只跑le_srr：sh pg_to_hdfst_to_ods_to_dwd.sh le_srr tableaudb 时间
#指定源表时间和目标分区：sh pg_to_hdfst_to_ods_to_dwd.sh wo_qrcode tableaudb 时间
#两个表都跑：sh pg_to_hdfst_to_ods_to_dwd.sh 
hiveservices.sh start
start-dfs.sh


########第一阶段 从pg到hdfs
# 1 设置sqoop工具路径
sqoop="/opt/module/sqoop/bin/sqoop"

# 2 设置同步的数据库
if [ -n "$2" ] ;then 
    sync_db=$2
else
    sync_db='tableaudb'
   # echo 'please input 第二个变量 the PostgreSQL db to be synced!'
   # exit 1
fi

# 3 设置数据库连接字符串
connect_str_pg="jdbc:postgresql://10.226.98.59:55433/$sync_db"
#59的也读58的pg保证数据源一致

# 4 同步日期设置，默认同步当天数据
if [ -n "$3" ]; then
    sync_date=$3
else
    sync_date=$(date  +%F)
fi
old_date1="$(date -d "3 day ago" +'%F')"
#5 DB User&Password -- TO be udpated
user='postgres'
pwd='1qazxsw2'
CC="'"         #这个是为了给日期加上单引号如果不加到hdfs就没有lzo.index
#同步PostgreSQL数据通过sqoop
sync_data_pg() {
    echo "${sync_date} stat syncing........"
    $sqoop import \
        --connect $connect_str_pg\
        --username $user \
        --password $pwd \
        --target-dir /bsc/origin_data/$sync_db/$1/$sync_date \
        --delete-target-dir \
        --query "$2 and \$CONDITIONS" \
	--hive-drop-import-delims \
        --num-mappers 1 \
        --fields-terminated-by '\t' \
        --compress \
        --compression-codec lzop \
        --null-string '\\N' \
        --null-non-string '\\N'

    hadoop jar /opt/module/hadoop3/share/hadoop/common/hadoop-lzo-0.4.20.jar \
        com.hadoop.compression.lzo.DistributedLzoIndexer \
        /bsc/origin_data/$sync_db/$1/$sync_date

    echo "${sync_date} end syncing........"
}

sync_wq_pg() {
    echo "${sync_date} stat syncing........"
    $sqoop import \
        --connect $connect_str_pg\
        --username $user \
        --password $pwd \
        --target-dir /bsc/origin_data/$sync_db/$1/$sync_date \
        --delete-target-dir \
        --query "$2 and \$CONDITIONS" \
		--hive-drop-import-delims \
        --num-mappers 1 \
        --fields-terminated-by ',' \
        --compress \
        --compression-codec lzop \
        --null-string '\\N' \
        --null-non-string '\\N'

    hadoop jar /opt/module/hadoop3/share/hadoop/common/hadoop-lzo-0.4.20.jar \
        com.hadoop.compression.lzo.DistributedLzoIndexer \
        /bsc/origin_data/$sync_db/$1/$sync_date

    echo "${sync_date} end syncing........"
}
sync_sto_pg() {
    echo "${sync_date} stat syncing........"
    $sqoop import \
        --connect $connect_str_pg\
        --username $user \
        --password $pwd \
        --target-dir /bsc/origin_data/$sync_db/$1/$sync_date \
        --delete-target-dir \
        --query "$2 and \$CONDITIONS" \
		--hive-drop-import-delims \
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

# 同步wo_qrcode数据

sync_wo_qrcode_data() {
    echo "Start syncing wo_qrcode data information"
   sync_wq_pg "woqrcode" "SELECT distinct
                            trim(plant_id)as plant_id
                            ,trim(work_order_no) as work_order_no
                            ,trim(dn_no) as dn_no
                            ,trim(material) as material
                            ,trim(batch) as batch
                            ,trim(qrcode) as qr_code
			                ,trim(dt) as dt                                        
                            FROM wo_qrcode where 1=1	
			" 				                          
							
    echo "End syncing plant master data information"
}
sync_le_srr_data() {
    echo "Start syncing le_srr data information"
    sync_data_pg "le_srr" "SELECT
				division as division, 
				"year" as year, 
				"month" as month, 
				le_cny as  le_cny, 
				le_usd as le_usd , 
				srr_cny as srr_cny, 
				srr_usd as srr_usd, 
				srr_version as srr_version,
                '' as dp_prder_project,
                '' as sales_ro_comment,
                '$sync_date' as dt							
				FROM public.le_srr 
                --where dt>=date('$sync_date')-1
                where 1=1"
	echo "End syncing plant master data information"
}
sync_dsr_le_srr_data() {
    echo "Start syncing dsr_le_srr data information"
    sync_data_pg "dsr_le_srr" "SELECT
				division as division, 
				"year" as year, 
				"month" as month, 
				le_cny as  le_cny, 
				le_usd as le_usd , 
				srr_cny as srr_cny, 
				srr_usd as srr_usd, 
				srr_version as srr_version,
                dp_prder_project,
                sales_ro_comment,
                 dt							
				FROM public.dsr_le_srr 
                where dt>=date('$sync_date')-1
                "
	echo "End syncing plant master data information"
}
sync_sto_migo2pgi_data() {
    echo "Start syncing  data information"
    sync_sto_pg "sto_migo2pgi" "SELECT 
                            x."inboundtime",
                            x."plant" as plant, 
                            x."upn" as upn ,
                            x."bt" as bt,
                            x."inventorystatus" as inventorystatus, 
                            x."sapinbounddn" as sapinbounddn, 
                            x."workorderno" as workorderno, 
                            x."batch" as batch, 
                            x."inbounddn" as inbounddn ,
                            cast(x."outboundtime" as timestamp) as outboundtime,
                            x."qty" as qty, 
                            x."outbounddn" as outbounddn, 
                            x."supplier" as supplier,
                            x."id" as id,
                            x.is_pacemaker,
                            x.distribution_properties,
                            cast(x."localizationfinishtime" as timestamp) as localizationfinishtime
                    FROM public.sto_migo2pgi x 
					where date(UpdateDT) =date('$sync_date')-1"
	echo "End syncing plant master data information"
}
sync_upn_change_bu_data() {
    echo "Start syncing  data information"
    sync_sto_pg "upn_change_bu" "SELECT 
                                        x.startdate, 
                                        x.dt,
                                        x.upn,
                                        x.correctedbu,
                                        x.enddate,
                                        x.applicant
                                        FROM public.dim_upn_recategorized x
					                where date(dt) =date('$sync_date')-1"
	echo "End syncing upn_change_bu data information"
}
sync_qibo_fenbo_data() {
    echo "Start syncing qibo_fenbo data information"
    sync_sto_pg "qibo_fenbo" "SELECT 
                                        x.is_pacemaker,  
                                        x.chinesename,
                                        x.material,
                                        x.distribution_properties
                                        FROM public.pacemaker_list x
                                        where 1=1
					               "
	echo "End syncing qibo_fenbo data information"
}
sync_inbound_declaration_data() {
    echo "Start syncing inbound_declaration data information"
    sync_sto_pg "inbound_declaration" "SELECT 
                                        preinspection_flag, 
                                        week, 
                                        dockwarrantdate, 
                                        remark, 
                                        forwarding, 
                                        shipfrom_country,
                                        mon, 
                                        '' as pickup_status,
                                        inspection_finishdate,
                                        pieces, 
                                        coocertificate_receiveddate,
                                        customsinspection_notifydate,
                                        coodraft_receiveddate, 
                                        biz_no,
                                        is_woodenpallet, 
                                        inbounddeclaration_finishdate, 
                                        picturetaken_date, 
                                        arrivalgoods_type,
                                        commericalinvoice,
                                        intoinventorydate,
                                         no,
                                        inspection_appointmentdate,
                                        pickupdocument_no, 
                                        inbounddeclaration_startdate,
                                        preinspection_no, 
                                        ccs_no, 
                                        grossweight, 
                                        qty, 
                                        bscinformslcdate,
                                        is_woodenpackage, 
                                        preinspection_abnormalreason, 
                                        status
                                        FROM public.inbound_declaration
                                        where 1=1 "
	echo "End syncing inbound_declaration data information"
}
sync_outbound_customclearance_data() {
    echo "Start syncing outbound_customclearance data information"
    sync_sto_pg "outbound_customclearance" "SELECT 
                                    "no", 
                                    abnormal_reason,
                                    mon, 
                                    declaration_completiondate,
                                    pieces, 
                                    chineselabelpicturereceiveddate, 
                                    status1,
                                    biz_no,
                                    get_certificate,
                                    declaration_itemname,
                                    commericalinvoice,
                                    intoinventorydate,
                                    demo_or_sample, 
                                    taxpayment_completiondate,
                                    is_excluded,
                                    batchinfo_receiveddate,
                                    grossweight, 
                                    ccs_no,
                                    document_finishpreparationdate,
                                    customclearance_date,
                                    certigicates_receiveddate,
                                    is_documentlistreceived,
                                    commodityinspection_date,
                                    taxpayment_applicationdate,
                                    status
                                    FROM public.outbound_customclearance
                                        where 1=1 "
	echo "End syncing outbound_customclearance data information"
}
sync_outbound_customclarance_mal_data() {
    echo "Start syncing outbound_customclarance_mal data information"
    sync_sto_pg "outbound_customclarance_mal" "SELECT
                                                commericalinvoice,
                                                intoinventorydate,
                                                no,
                                                amount, 
                                                taxpayment_completiondate, 
                                                customsinspection_date,
                                                abnormal_reason, 
                                                css_no,
                                                mon, 
                                                pieces,
                                                declaration_completiondate,
                                                chineselabelpicturereceiveddate, 
                                                batchinfo_receiveddate,
                                                biz_no, 
                                                document_finishpreparationdate,
                                                qty, 
                                                customclearance_date,
                                                status1, 
                                                is_documentlistreceived,
                                                commodityinspection_date,
                                                get_certificate, 
                                                taxpayment_applicationdate, 
                                                declaration_itemname, 
                                                status
                                                FROM public.outbound_customclarance_mal
                                                where 1=1 "
	echo "End syncing outbound_customclarance_mal data information"
}
sync_outbound_pacemaker_data() {
    echo "Start syncing outbound_pacemaker data information"
    sync_sto_pg "outbound_pacemaker" "SELECT 
                                                no,
                                                abnormal_reason, 
                                                remark, 
                                                ganrantee_paymentdate,
                                                testscheduled_date,
                                                mon, 
                                                declaration_completiondate,
                                                actualtest_date,
                                                ciq_signcompletiondate, 
                                                dailystatus, 
                                                draft_completiondate, 
                                                commericalinvoice,
                                                intoinventorydate,
                                                amount, 
                                                taxpayment_completiondate, 
                                                notificationslc_date,
                                                faircopycontactdocsent_date,
                                                item_name,
                                                localization_completiondate,
                                                scan_fee,
                                                pickup_no, 
                                                ccs_no,
                                                document_finishpreparationdate,
                                                qty, 
                                                part_number,
                                                forwarder_referenceid,
                                                commodityinspection_date, 
                                                taxpayment_applicationdate,
                                                shipment_number
                                                FROM public.outbound_pacemaker
                                                where 1=1 "
	echo "End syncing outbound_pacemaker data information"
}
sync_dsr_customer_rebate_data() {
    echo "Start syncing sync_dsr_customer_rebate_data data information"
    sync_sto_pg "dsr_customer_rebate" "
                                    SELECT 
                                    dt, 
                                    dealer_code, 
                                    calculation_type,
                                     end_date, 
                                     upn, 
                                     bu,
                                      dealer_type,
                                       rebate_rate, 
                                       start_date
                                    FROM public.dsr_customer_rebate
                                        where 1=1
					               "
	echo "End syncing sync_dsr_customer_rebate_data data information"
}
sync_dim_all_kpi_data() {
    echo "Start syncing  data information"
    sync_sto_pg "dim_all_kpi" "SELECT 
                                kpicode,
                                function, 
                                stream,
                                sub_stream, 
                                category,
                                supplier,
                                index,
                                unit,
                                formula,
                                index_level,     
                                criteria,
                                target,
                                vaild_from,        
                                vaild_to,        
                                update_dt as dt          
                                FROM public.dim_all_kpi
                                where date(update_dt) =date('$sync_date')-1"
	echo "End syncing dim_all_kpi master data information"
}
sync_dwd_dim_ie_supplier_data() {
    echo "Start syncing  data information"
    sync_sto_pg "dwd_dim_ie_supplier" "SELECT 
                                forwording, 
                                supplier, 
                                need_calcaulated, 
                                update_dt
                                FROM public.ie_supplier
                                where date(update_dt) =date('$sync_date')-1"
	echo "End syncing dwd_dim_ie_supplier master data information"
}
#判断变量1是哪个表，没有输入就两个表都跑
if [ "$1"x = "wo_qrcode"x ];then
	echo "$1 only run"
	echo "$old_date1  ok"
	sync_wo_qrcode_data 
elif [ "$1"x = "le_srr"x ];then
    echo " $1 only run"
	echo "$sync_date  ok"
	sync_le_srr_data
elif [ "$1"x = "dsr_le_srr"x ];then
    echo " $1 only run"
	echo "$sync_date  ok"
	sync_dsr_le_srr_data
elif [ "$1"x = "sto_migo2pgi"x ];then
    echo " $1 only run"
	echo "$sync_date  ok"
	sync_sto_migo2pgi_data
elif [ "$1"x = "upn_change_bu"x ];then
    echo " $1 only run"
	echo "$sync_date  ok"
	sync_upn_change_bu_data
elif [ "$1"x = "qibo_fenbo"x ];then
    echo " $1 only run"
	echo "$sync_date  ok"
	sync_qibo_fenbo_data
elif [ "$1"x = "inbound_declaration"x ];then
    echo " $1 only run"
	echo "$sync_date inbound_declaration ok"
	sync_inbound_declaration_data
elif [ "$1"x = "outbound_customclearance"x ];then
    echo " $1 only run"
	echo "$sync_date outbound_customclearance ok"
	sync_outbound_customclearance_data
elif [ "$1"x = "outbound_customclarance_mal"x ];then
    echo " $1 only run"
	echo "$sync_date outbound_customclarance_mal ok"
	sync_outbound_customclarance_mal_data
elif [ "$1"x = "outbound_pacemaker"x ];then
    echo " $1 only run"
	echo "$sync_date outbound_pacemaker ok"
	sync_outbound_pacemaker_data
elif [ "$1"x = "dsr_customer_rebate"x ];then
    echo " $1 only run"
	echo "$sync_date dsr_customer_rebate ok"
	sync_dsr_customer_rebate_data
elif [ "$1"x = "dim_all_kpi"x ];then
    echo " $1 only run"
	echo "$sync_date dim_all_kpi ok"
	sync_dim_all_kpi_data
elif [ "$1"x = "dwd_dim_ie_supplier"x ];then
    echo " $1 only run"
	echo "$sync_date dwd_dim_ie_supplier ok"
	sync_dwd_dim_ie_supplier_data
else
    echo "wo_qrcode!  le_srr all run"
    sync_wo_qrcode_data
	sync_le_srr_data
    sync_upn_change_bu_data
    sync_sto_migo2pgi_data
    sync_qibo_fenbo_data
    sync_inbound_declaration_data
    sync_outbound_customclearance_data
    sync_outbound_customclarance_mal_data
    sync_outbound_pacemaker_data
    sync_dsr_customer_rebate_data
    sync_dim_all_kpi_data
    sync_dwd_dim_ie_supplier_data
fi

##############################################第二阶段hdfs to ods
# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
origin_db_name='tableaudb' #原始数据库
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径



echo "Start loading data on {$sync_date} .hdfs to ods................."

master_sql=""
woqrcode="
 load data inpath '/bsc/origin_data/tableaudb/woqrcode/$sync_date' overwrite
into table opsdw.dwd_fact_work_order_qr_code_mapping
;"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/woqrcode/$sync_date"
if [ $? -eq 0 ];
then
	echo " diaoyong ok"
    master_sql="$master_sql""$woqrcode"
fi
sto_migo2pgi="
load data inpath '/bsc/origin_data/$origin_db_name/sto_migo2pgi/$sync_date' overwrite
into table ${target_db_name}.ods_sto_migo2pgi
partition(dt='$sync_date');
"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/sto_migo2pgi/$sync_date"
if [ $? -eq 0 ];
then
	echo " diaoyong ok"
    master_sql="$master_sql""$sto_migo2pgi"
fi

le_srr="
load data inpath '/bsc/origin_data/$origin_db_name/le_srr/$sync_date' overwrite
into table ${target_db_name}.ods_le_srr;
"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/le_srr/$sync_date"
if [ $? -eq 0 ];
then
	echo " diaoyong ok"
    master_sql="$master_sql""$le_srr"
fi
dsr_le_srr="
load data inpath '/bsc/origin_data/$origin_db_name/dsr_le_srr/$sync_date' overwrite
into table ${target_db_name}.ods_dsr_le_srr;
"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/dsr_le_srr/$sync_date"
if [ $? -eq 0 ];
then
	echo " diaoyong ok"
    master_sql="$master_sql""$dsr_le_srr"
fi
upn_change_bu="
load data inpath '/bsc/origin_data/$origin_db_name/upn_change_bu/$sync_date' overwrite
into table ${target_db_name}.ods_upn_change_bu;
"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/upn_change_bu/$sync_date"
if [ $? -eq 0 ];
then
	echo " diaoyong ok"
    master_sql="$master_sql""$upn_change_bu"
fi
qibo_fenbo="
load data inpath '/bsc/origin_data/$origin_db_name/qibo_fenbo/$sync_date' overwrite
into table ${target_db_name}.ods_pacemaker_list;
"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/qibo_fenbo/$sync_date"
if [ $? -eq 0 ];
then
	echo " diaoyong ok"
    master_sql="$master_sql""$qibo_fenbo"
fi
inbound_declaration_sql="
load data inpath '/bsc/origin_data/$origin_db_name/inbound_declaration/$sync_date' overwrite
into table ${target_db_name}.ods_inbound_declaration partition(year='${sync_date:0:4}');
"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/inbound_declaration/$sync_date"
if [ $? -eq 0 ];
then
	echo " diaoyong ok"
    master_sql="$master_sql""$inbound_declaration_sql"
fi
outbound_customclearance="
load data inpath '/bsc/origin_data/$origin_db_name/outbound_customclearance/$sync_date' overwrite
into table ${target_db_name}.ods_outbound_customclearance partition(year='${sync_date:0:4}');
"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/outbound_customclearance/$sync_date"
if [ $? -eq 0 ];
then
	echo " diaoyong ok"
    master_sql="$master_sql""$outbound_customclearance"
fi
outbound_customclarance_mal="
load data inpath '/bsc/origin_data/$origin_db_name/outbound_customclarance_mal/$sync_date' overwrite
into table ${target_db_name}.ods_outbound_customclarance_mal partition(year='${sync_date:0:4}');
"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/outbound_customclarance_mal/$sync_date"
if [ $? -eq 0 ];
then
	echo " diaoyong ok"
    master_sql="$master_sql""$outbound_customclarance_mal"
fi
outbound_pacemaker="
load data inpath '/bsc/origin_data/$origin_db_name/outbound_pacemaker/$sync_date' overwrite
into table ${target_db_name}.ods_outbound_pacemaker partition(year='${sync_date:0:4}');
"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/outbound_pacemaker/$sync_date"
if [ $? -eq 0 ];
then
	echo " diaoyong ok"
    master_sql="$master_sql""$outbound_pacemaker"
fi
dsr_customer_rebate="
load data inpath '/bsc/origin_data/$origin_db_name/dsr_customer_rebate/$sync_date' overwrite
into table ${target_db_name}.ods_dsr_customer_rebate ;
"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/dsr_customer_rebate/$sync_date"
if [ $? -eq 0 ];
then
	echo " diaoyong ok"
    master_sql="$master_sql""$dsr_customer_rebate"
fi
dim_all_kpi="
load data inpath '/bsc/origin_data/$origin_db_name/dim_all_kpi/$sync_date' overwrite
into table ${target_db_name}.ods_dim_all_kpi ;
"
hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/dim_all_kpi/$sync_date"
if [ $? -eq 0 ];
then
	echo " diaoyong ok"
    master_sql="$master_sql""$dim_all_kpi"
fi
dwd_dim_ie_supplier="
load data inpath '/bsc/origin_data/$origin_db_name/dwd_dim_ie_supplier/$sync_date' overwrite
into table ${target_db_name}.ods_dim_ie_supplier ;
"
hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/dwd_dim_ie_supplier/$sync_date"
if [ $? -eq 0 ];
then
	echo " diaoyong ok"
    master_sql="$master_sql""$dwd_dim_ie_supplier"
fi
# 2. 执行加载数据SQL
if [ "$1"x = "wo_qrcode"x ];then
	echo "ODS $1 only run"	
	$hive -e "$woqrcode"
	echo "ODS  finish wo_qrcode data into ODS layer on ${sync_date} .................."
elif [ "$1"x = "le_srr"x ];then
    echo " ODS $1 only run"
	echo "$sync_date  ok"
	$hive -e "$le_srr"
	echo "ODS  finish ods_dim_le_srr data into ODS layer on ${sync_date} .................."
elif [ "$1"x = "dsr_le_srr"x ];then
    echo " ODS $1 only run"
	echo "$sync_date  ok"
	$hive -e "$dsr_le_srr"
	echo "ODS  finish dsr_le_srr data into ODS layer on ${sync_date} .................."
elif [ "$1"x = "sto_migo2pgi"x ];then
    echo " $1 only run"
	echo "$sync_date  ok"
	$hive -e "$sto_migo2pgi"
elif [ "$1"x = "upn_change_bu"x ];then
    echo " $1 only run"
	echo "$sync_date  ok"
	$hive -e "$upn_change_bu"
elif [ "$1"x = "qibo_fenbo"x ];then
    echo " $1 only run"
	echo "$sync_date  ok"
	$hive -e "$qibo_fenbo"
elif [ "$1"x = "inbound_declaration"x ];then
    echo " $1 only run"
	echo "$sync_date  ok"
	$hive -e "$inbound_declaration_sql"
elif [ "$1"x = "outbound_customclearance"x ];then
    echo " $1 only run"
	echo "$sync_date  ok"
	$hive -e "$outbound_customclearance"
elif [ "$1"x = "outbound_customclarance_mal"x ];then
    echo " $1 only run"
	echo "$sync_date  ok"
	$hive -e "$outbound_customclarance_mal"
elif [ "$1"x = "outbound_pacemaker"x ];then
    echo " $1 only run"
	echo "$sync_date  ok"
	$hive -e "$outbound_pacemaker"
elif [ "$1"x = "dsr_customer_rebate"x ];then
    echo " $1 only run"
	echo "$sync_date  ok"
	$hive -e "$dsr_customer_rebate"
elif [ "$1"x = "dim_all_kpi"x ];then
    echo " $1 only run"
	echo "$sync_date  ok"
	$hive -e "$dim_all_kpi"
elif [ "$1"x = "dwd_dim_ie_supplier"x ];then
    echo " $1 only run"
	echo "$sync_date  ok"
	$hive -e "$dwd_dim_ie_supplier"
else
    echo "ODS wo_qrcode!  le_srr all run"
    $hive -e"$master_sql"
fi


echo "End loading data on {$sync_date} ..hdfs to ods................"
echo " finish ods $1"
########################################第三阶段 ods to dwd
export LANG="en_US.UTF-8"
echo "start syncing ODS TO DWD  layer on ${sync_date} ................."

# 1 Hive SQL string
le_srr_sql="
use ${target_db_name};
-- 参数
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.reducers.max=8; 
#set mapred.reduce.tasks=8;
set hive.exec.parallel=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;	

-- cfda master data
insert overwrite table ${target_db_name}.dwd_dim_le_srr partition(dt)
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
from ${target_db_name}.ods_le_srr   
where dt>=date_add('$sync_date',-1)
and srr_version is not null ; 
"
dsr_le_srr_sql="
use ${target_db_name};
-- 参数
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.reducers.max=8; 
#set mapred.reduce.tasks=8;
set hive.exec.parallel=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;	

-- cfda master data
insert overwrite table ${target_db_name}.dwd_dim_dsr_le_srr partition(dt)
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
from ${target_db_name}.ods_dsr_le_srr   
where dt>=date_add('$sync_date',-1)
and srr_version is not null ; 
"

qr_code_sql="
use ${target_db_name};
-- 参数
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.reducers.max=8; 
#set mapred.reduce.tasks=8;
set hive.exec.parallel=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;	
insert overwrite table opsdw.dwd_fact_work_order_qr_code_mapping_new partition(dt)
  select  
        x.plant_id, 
        x.work_order_no,
        x.dn_no,
        x.material,
        x.batch, 
        x.qr_code, 
	    x.dt
    FROM opsdw.dwd_fact_work_order_qr_code_mapping x
    where  x.dt>='$old_date1';"
sto_migo2pgi_sql="
use ${target_db_name};
-- 参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
insert overwrite table opsdw.dwd_sto_migo2pgi partition(dt)
  select  
        x.inboundtime,
        x.plant , 
        x.upn ,
        x.bt ,
        x.inventorystatus , 
        x.sapinbounddn , 
        x.workorderno , 
        x.batch , 
        x.inbounddn ,
        x.outboundtime , 
        x.qty , 
        x.outbounddn , 
        x.supplier ,
        x.id ,
        x.localizationfinishtime,
        x.is_pacemaker,
        x.distribution_properties,
        date_format(x.outboundtime,'yyyy-MM-dd') as dt
    FROM opsdw.ods_sto_migo2pgi x
    where  x.dt>=date_add('$sync_date',-1) and x.outboundtime is not null
    "
upn_change_bu_sql="
use ${target_db_name};
-- 参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
insert overwrite table opsdw.dwd_upn_change_bu partition(dt)
  select  
        x.startdate ,
        x.upn , 
        x.correctedbu ,
        x.enddate ,
        x.applicant ,
        x.update_dt
    FROM opsdw.ods_upn_change_bu x
    where  x.update_dt>=date_add('$sync_date',-1);"
qibo_fenbo_sql="
use ${target_db_name};
-- 参数
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.reducers.max=8; 
#set mapred.reduce.tasks=8;
set hive.exec.parallel=false;
insert overwrite table opsdw.dwd_pacemaker_list
  select  
        x.is_pacemaker ,
        x.chinesename , 
        x.material ,
        x.distribution_properties ,
        from_unixtime(unix_timestamp()+8*60*60,'yyyy-MM-dd HH:mm:ss') as updatetime 
    FROM opsdw.ods_pacemaker_list x
    where  x.is_pacemaker>=0;"
dsr_customer_rebate_sql="
use ${target_db_name};
-- 参数
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.reducers.max=8; 
#set mapred.reduce.tasks=8;
set hive.exec.parallel=false;
insert overwrite table opsdw.dwd_dim_customer_rebaterate
  select  
        dt, dealer_code, calculation_type, end_date, upn, bu, dealer_type, rebate_rate, start_date
    FROM opsdw.ods_dsr_customer_rebate x
    where  x.dt>='2022-11-01';"
dim_all_kpi_sql="
use ${target_db_name};
-- 参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
insert overwrite table opsdw.dwd_dim_all_kpi partition(dt)
  select  
        kpicode,
        func, 
        stream,
        sub_stream, 
        category,
        supplier,
        index,
        unit,
        formula,
        index_level,
        criteria,
        target,
        vaild_from,
        vaild_to,
        substr(dt,1,10) as dt
    FROM opsdw.ods_dim_all_kpi 
    where  substr(dt,1,10)>=date_add('$sync_date',-1);"
dwd_dim_ie_supplier_sql="
use ${target_db_name};
-- 参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
insert overwrite table opsdw.dwd_dim_ie_supplier partition(dt)
  select  
        forwording,
        supplier,
        need_calcaulated,
        substr(update_dt,1,10)AS dt
    FROM opsdw.ods_dim_ie_supplier 
    where  substr(update_dt,1,10)>=date_add('$sync_date',-1);"
# 2. 执行加载数据SQL

if [ "$1"x = "wo_qrcode"x ];then
	echo "DWD $1 only run"	
	$hive -e "$qr_code_sql"
	echo "DWD  finish wo_qrcode data into DWD layer on ${sync_date} .................."
elif [ "$1"x = "le_srr"x ];then
    echo " DWD $1 only run"
	echo "$sync_date  ok"
	$hive -e "$le_srr_sql"
	echo "DWD  finish dwd_dim_le_srr data into DWD layer on ${sync_date} .................."
elif [ "$1"x = "dsr_le_srr"x ];then
    echo " DWD $1 only run"
	echo "$sync_date  ok"
	$hive -e "$dsr_le_srr_sql"
	echo "DWD  finish dwd_dim_dsr_le_srr data into DWD layer on ${sync_date} .................."
    sh /bscflow/PG/Shell/all_dsr_to_hdfs.sh dwd_dim_dsr_le_srr_test
    sh /bscflow/PG/Shell/all_dsr_to_pg_db.sh dwd_dim_dsr_le_srr_test
elif [ "$1"x = "sto_migo2pgi"x ];then
    echo " DWD $1 only run"
	echo "$sync_date  ok"
	$hive -e "$sto_migo2pgi_sql"
	echo "DWD  finish dwd_sto_migo2pgi data into DWD layer on ${sync_date} .................."
    sh /bscflow/dws/dwd_to_dws_kpi_sto_migo2pgi.sh
    sh /bscflow/PG/Shell/all_dsr_to_hdfs.sh dws_kpi_sto_migo2pgi
    sh /bscflow/PG/Shell/all_dsr_to_pg_db.sh dws_kpi_sto_migo2pgi
elif [ "$1"x = "upn_change_bu"x ];then
    echo " DWD $1 only run"
	echo "$sync_date  ok"
	$hive -e "$upn_change_bu_sql"
	echo "DWD  finish dwd_upn_change_bu data into DWD layer on ${sync_date} .................."
elif [ "$1"x = "qibo_fenbo"x ];then
    echo " DWD $1 only run"
	echo "$sync_date  ok"
	$hive -e "$qibo_fenbo_sql"
	echo "DWD  finish qibo_fenbo data into DWD layer on ${sync_date} .................."
elif [ "$1"x = "dsr_customer_rebate"x ];then
    echo " $1 only run"
	$hive -e "$dsr_customer_rebate_sql"
	echo "DWD  finish dsr_customer_rebate data into DWD layer on ${sync_date} .................."
elif [ "$1"x = "inbound_declaration"x ];then
    echo " $1 only run"
	echo "dwd is not have"	
elif [ "$1"x = "outbound_customclearance"x ];then
    echo " $1 only run"
	echo "dwd is not have"	
elif [ "$1"x = "outbound_customclarance_mal"x ];then
    echo " $1 only run"
	echo "dwd is not have"	
elif [ "$1"x = "outbound_pacemaker"x ];then
    echo " $1 only run"
	echo "dwd is not have"	
elif [ "$1"x = "dim_all_kpi"x ];then
    echo " $1 only run"
	$hive -e "$dim_all_kpi_sql"
	echo "DWD  finish dim_all_kpi data into DWD layer on ${sync_date} .................."
    sh /bscflow/PG/Shell/all_dsr_to_hdfs.sh dwd_dim_all_kpi
    sh /bscflow/PG/Shell/all_dsr_to_pg_db.sh dwd_dim_all_kpi
elif [ "$1"x = "dwd_dim_ie_supplier"x ];then
    echo " $1 only run"
	$hive -e "$dwd_dim_ie_supplier_sql"
	echo "DWD  finish dwd_dim_ie_supplier data into DWD layer on ${sync_date} .................."
else
    echo "DWD wo_qrcode!  le_srr all run"
    $hive -e "$le_srr_sql"
	echo "dwd le_srr finish"
	$hive -e "$qr_code_sql"
	echo "dwd qr_code_sql finish"
    $hive -e "$upn_change_bu_sql"
	echo "dwd upn_change_bu_sql finish"
    echo "$sync_date  ok"
	$hive -e "$sto_migo2pgi_sql"
    echo "$sync_date  ok"
	$hive -e "$qibo_fenbo_sql"
    $hive -e "$dim_all_kpi_sql"
    $hive -e "$dwd_dim_ie_supplier_sql"
    sh /bscflow/dws/dwd_to_dws_kpi_sto_migo2pgi.sh
    sh /bscflow/PG/Shell/all_dsr_to_hdfs.sh dws_kpi_sto_migo2pgi
    sh /bscflow/PG/Shell/all_dsr_to_pg_db.sh dws_kpi_sto_migo2pgi
    sh /bscflow/PG/Shell/all_dsr_to_hdfs.sh dwd_dim_all_kpi
    sh /bscflow/PG/Shell/all_dsr_to_pg_db.sh dwd_dim_all_kpi
fi

#月初 
m_s_date=${sync_date:0:7}-01
#每月1号清空tmp
if [ "$sync_date"x = "$m_s_date"x ];then
	echo "this day is 01 rm -rf /tmp/*"	
	rm -rf /tmp/*
else
	echo "not delete"
fi
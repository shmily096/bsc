#!/bin/bash
# Function:
#   sync up PG data to HDFS template
# History:
#   2021-11-08    Donny   v1.0    init

# 1 设置sqoop工具路径
sqoop="/opt/module/sqoop/bin/sqoop"

# 2 设置同步的数据库
# if [ -n "$2" ] ;then 
#     sync_db=$2
# else
#    echo 'please input 第二个变量 the PostgreSQL db to be synced!'
#    exit 1
# fi

sync_db='tableaudb'

# 3 设置数据库连接字符串
connect_str_pg="jdbc:postgresql://10.226.98.58:55433/$sync_db"

# 4 同步日期设置，默认同步当天数据
if [ -n "$3" ]; then
    sync_date=$3
else
    sync_date=$(date  +%F)
    #sync_date='2024-10-21'
    year_month=$(date  +'%Y-%m')
    last_month=$(date -d "$(date  +'%Y%m')01 last month" +'%Y%m')
fi

#5 DB User&Password -- TO be udpated
user='bsc1'
pwd='bsc1qazxsw2'

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

#同步PostgreSQL数据通过sqoop \001
sync_data001_pg() {
    echo "${sync_date} stat syncing........"
    $sqoop import \
        --connect $connect_str_pg\
        --username $user \
        --password $pwd \
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

# 同步DEMO数据
# 同步策略 - 全量
sync_demo_data() {
    echo "Start syncing demo data information"
    sync_data_pg "demo" "select 
                            id, 
                            name, 
                            age 
                            from demo 
                            where 1=1"

    echo "End syncing plant master data information"
}


# 同步ods_dq_marc数据
# 同步策略 - 全量
sync_ods_dq_marc_data() {
    echo "Start syncing ods_dq_marc data information"
    sync_data001_pg "ods_dq_marc" "SELECT 
                                    material, 
                                    plnt, 
                                    profit_ctr, 
                                    i, 
                                    source_list, 
                                    esloc, 
                                    ms, 
                                    proctype, 
                                    spt, 
                                    pdt, 
                                    grt, 
                                    profl, 
                                    lgrp, 
                                    av, 
                                    sptc, 
                                    control_code, 
                                    lhg, 
                                    insert_time
                                    FROM public.dq_marc
                                    where 1=1"

    echo "End syncing ods_dq_marc data information"
}



# 同步ods_dq_maex_bsc数据
# 同步策略 - 全量
sync_ods_dq_maex_bsc_data() {
    echo "Start syncing ods_dq_maex_bsc data information"
    sync_data001_pg "ods_dq_maex_bsc" "SELECT 
                                        ctry, 
                                        lr, 
                                        material, 
                                        "grouping",
                                        insert_time
                                        FROM public.dq_maex_bsc
                                        where 1=1"

    echo "End syncing ods_dq_maex_bsc data information"
}

# 同步ods_dq_maex_crm数据
# 同步策略 - 全量
sync_ods_dq_maex_crm_data() {
    echo "Start syncing ods_dq_maex_crm data information"
    sync_data001_pg "ods_dq_maex_crm" "SELECT 
                                        material, 
                                        country, 
                                        customer, 
                                        valid_from, 
                                        valid_to, 
                                        prod_rel_number, 
                                        seq_number, 
                                        pr_rel_status, 
                                        ext_release_number, 
                                        insert_time
                                        FROM public.dq_maex_crm
                                        where 1=1"

    echo "End syncing ods_dq_maex_crm data information"
}


# 同步ods_dq_mara数据
# 同步策略 - 全量
sync_ods_dq_mara_data() {
    echo "Start syncing ods_dq_mara data information"
    sync_data001_pg "ods_dq_mara" "SELECT 
                                        clt, 
                                        last_chg, 
                                        changed_by, 
                                        ms, 
                                        st, 
                                        material, 
                                        old_material_no, 
                                        s, 
                                        "no", 
                                        product_hierarchy, 
                                        slife, 
                                        ean_upc, 
                                        matl_group,
                                         mtyp, 
                                         bun, 
                                         l_o, 
                                         ct, 
                                         basic_material, 
                                         sc, 
                                         "temp", 
                                         ct_1, 
                                         taxcl, 
                                        uom, 
                                        orig, 
                                        bmr, 
                                        serial_no_profile, 
                                        drug_indicator, 
                                        srai_eligibility, 
                                        capital_equipment_relevant,
                                        repairable, 
                                        service_part_type, 
                                        fseai_eligibility, 
                                        material_description, 
                                        insert_time
                                        FROM public.dq_mara
                                        where 1=1"

    echo "End syncing ods_dq_mara data information"
}

# 同步ods_dq_mlan数据
# 同步策略 - 全量
sync_ods_dq_mlan_data() {
    echo "Start syncing ods_dq_mlan data information"
    sync_data001_pg "ods_dq_mlan" "SELECT 
                                        cl, 
                                        material, 
                                        ctry, 
                                        taxcl, 
                                        taxcl_1, 
                                        taxcl_2, 
                                        taxcl_3, 
                                        taxcl_4, 
                                        taxcl_5, 
                                        taxcl_6, 
                                        taxcl_7, 
                                        taxcl_8, 
                                        material_1, 
                                        insert_time
                                        FROM public.dq_mlan
                                        where 1=1"

    echo "End syncing ods_dq_mlan data information"
}

# 同步ods_dq_mvke数据
# 同步策略 - 全量
sync_ods_dq_mvke_data() {
    echo "Start syncing ods_dq_mvke data information"
    sync_data001_pg "ods_dq_mvke" "SELECT 
                                        material, 
                                        sorg, 
                                        st, 
                                        itcgr, 
                                        plnt, 
                                        mg_1, 
                                        mg_2, 
                                        mg_3, 
                                        mg_4, 
                                        mg_5, 
                                        product_hierarchy, 
                                        insert_time
                                        FROM public.dq_mvke
                                        where 1=1"

    echo "End syncing ods_dq_mvke data information"
}

# 同步ods_dq_zv001数据
# 同步策略 - 全量
sync_ods_dq_zv001_data() {
    echo "Start syncing ods_dq_zv001 data information"
    sync_data001_pg "ods_dq_zv001" "SELECT 
                                        cl, 
                                        sorg, 
                                        dchl, 
                                        customer, 
                                        material, 
                                        mg_1, 
                                        released_quantity, 
                                        from_date, 
                                        valid_to_date, 
                                        qty_shiped, 
                                        insert_time
                                        FROM public.dq_zv001
                                        where 1=1"

    echo "End syncing ods_dq_zv001 data information"
}

# 同步ods_dq_zv002数据
# 同步策略 - 全量
sync_ods_dq_zv002_data() {
    echo "Start syncing ods_dq_zv002 data information"
    sync_data001_pg "ods_dq_zv002" "SELECT 
                                        profit_ctr, 
                                        product_hierarchy, 
                                        sales_rep, 
                                        intrnl_rep, 
                                        diag_rep, 
                                        vsr_rep, 
                                        japan_dealer, 
                                        exp_rep_2, 
                                        exp_rep_3, 
                                        exp_rep_4, 
                                        exp_rep_5, 
                                        eegrp, 
                                        exp_rep_6, 
                                        exp_rep_7, 
                                        exp_rep_8, 
                                        exp_rep_9, 
                                        exp_rep_10, 
                                        exp_rep_11, 
                                        exp_rep_12, 
                                        exp_rep_13, 
                                        exp_rep_14, 
                                        exp_rep_15, 
                                        exp_rep_16, 
                                        exp_rep_17, 
                                        exp_rep_18, 
                                        exp_rep_19, 
                                        exp_rep_20, 
                                        insert_time
                                        FROM public.dq_zv002
                                        where 1=1"

    echo "End syncing ods_dq_zv001 data information"
}

# 同步adh_mara数据
# 同步策略 - 全量
sync_adh_mara_data() {
    echo "Start syncing adh_mara information"
    sync_data001_pg "ods_adh_mara" "SELECT 
                            \"Material\", \"BATCHID\", \"DI_SEQUENCE_NUMBER\", \"Created\", \"Created by\", \"Last Chg\", \"Changed by\", \"VPSTA\", 
                            \"PSTAT\", \"Clt\", \"MTyp\", \"MBRSH\", \"Matl Group\", \"Old material no.\", \"BUn\", \"BSTME\", \"ZEINR\", \"ZEIAR\", 
                            \"ZEIVR\", \"ZEIFO\", \"AESZN\", \"BLATT\", \"No.\", \"FERTH\", \"FORMT\", \"GROES\", \"Basic material\", \"NORMT\", \"L/O\", 
                            \"EKWSL\", \"BRGEW\", \"NTGEW\", \"GEWEI\", \"VOLUM\", \"VOLEH\", \"EF\", \"SC\", \"Temp\", \"DISST\", \"TRAGR\", \"STOFF\", \"SPART\", 
                            \"KUNNR\", \"EANNR\", \"WESCH\", \"BWVOR\", \"S\", \"SAISO\", \"ETIAR\", \"ETIFO\", \"ENTAR\", \"EAN/UPC\", \"EAN CATEGORY\", \"LAENG\", 
                            \"BREIT\", \"HOEHE\", \"MEABM\", \"Product hierarchy\", \"AEKLK\", \"CADKZ\", \"QMPUR\", \"ERGEW\", \"ERGEI\", \"ERVOL\", \"ERVOE\", \"GEWTO\", 
                            \"VOLTO\", \"VABME\", \"KZREV\", \"KZKFG\", \"BMR\", \"VHART\", \"FUELG\", \"STFAK\", \"MAGRV\", \"BEGRU\", \"DATAB\", \"LIQDT\", \"SAISJ\", 
                            \"PLGTP\", \"MLGUT\", \"EXTWG\", \"SATNR\", \"Material Category\", \"KZKUP\", \"KZNFM\", \"PMATA\", \"MS\", \"St\", \"MSTDE\", \"MSTDV\", 
                            \"TaxCl\", \"RBNRM\", \"MHDRZ\", \"SLife\", \"MHDLP\", \"INHME\", \"INHAL\", \"VPREH\", \"ETIAG\", \"INHBR\", \"CMETH\", \"CUOBF\", \"KZUMW\", 
\"KOSCH\", \"SPROF\", \"NRFHG\", \"MFRPN\", \"MFRNR\", \"BMATN\", \"MPROF\", \"KZWSM\", \"SAITY\", \"PROFL\", \"IHIVI\", \"ILOOS\", \"SERLV\", 
\"KZGVH\", \"XGCHP\", \"KZEFF\", \"COMPL\", \"IPRKZ\", \"RDMHD\", \"PRZUS\", \"MTPOS_MARA\", \"BFLME\", \"MATFI\", \"CMREL\", \"BBTYP\", 
\"SLED_BBD\", \"GTIN_VARIANT\", \"GENNR\", \"RMATP\", \"GDS_RELEVANT\", \"WEORA\", \"HUTYP_DFLT\", \"PILFERABLE\", \"WHSTC\", \"WHMATGR\", 
\"HNDLCODE\", \"HAZMAT\", \"HUTYP\", \"TARE_VAR\", \"MAXC\", \"MAXC_TOL\", \"MAXL\", \"MAXB\", \"MAXH\", \"UoM\", \"Orig\", \"MFRGR\", \"QQTIME\", 
\"QQTIMEUOM\", \"QGRP\", \"Serial No. Profile\", \"PS_SMARTFORM\", \"LOGUNIT\", \"CWQREL\", \"CWQPROC\", \"CWQTOLGR\", \"ADPROF\", 
\"IPMIPPRODUCT\", \"ALLOW_PMAT_IGNO\", \"MEDIUM\", \"ANIMAL_ORIGIN\", \"TEXTILE_COMP_IND\", \"ANP\", \"BEV1_LULEINH\", 
\"BEV1_LULDEGRP\", \"BEV1_NESTRUCCAT\", \"DSD_SL_TOLTYP\", \"DSD_SV_CNT_GRP\", \"DSD_VC_GROUP\", \"VSO_R_TILT_IND\", 
\"VSO_R_STACK_IND\", \"VSO_R_BOT_IND\", \"VSO_R_TOP_IND\", \"VSO_R_STACK_NO\", \"VSO_R_PAL_IND\", \"VSO_R_PAL_OVR_D\", 
\"VSO_R_PAL_OVR_W\", \"VSO_R_PAL_B_HT\", \"VSO_R_PAL_MIN_H\", \"VSO_R_TOL_B_HT\", \"VSO_R_NO_P_GVH\", \"VSO_R_QUAN_UNIT\", 
\"VSO_R_KZGVH_IND\", \"PACKCODE\", \"DG_PACK_STATUS\", \"MCOND\", \"RETDELC\", \"LOGLEV_RETO\", \"NSNID\", \"IMATN\", \"PICNUM\", 
\"BSTAT\", \"COLOR_ATINN\", \"SIZE1_ATINN\", \"SIZE2_ATINN\", \"COLOR\", \"SIZE1\", \"SIZE2\", \"FREE_CHAR\", \"CARE_CODE\", 
\"BRAND_ID\", \"FIBER_CODE1\", \"FIBER_PART1\", \"FIBER_CODE2\", \"FIBER_PART2\", \"FIBER_CODE3\", \"FIBER_PART3\", \"FIBER_CODE4\",
\"FIBER_PART4\", \"FIBER_CODE5\", \"FIBER_PART5\", \"FASHGRD\", \"EDH_ACTIVE_INDICATOR\", \"PROCESSED_DATE\", \"ZZWRKST\", 
\"ZZPRODUCT\", \"ZZLANCON\", \"PRDHA2\", \"Drug Indicator\", \"ZZCMI\", \"SRAI Eligibility\", \"Capital Equipment Relevant\", 
\"Repairable\", \"Service Part Type\", \"ADH_LOAD_ID\", \"ADH_LOAD_DT\", \"ADH_REFRESH_DT\"
FROM public.adh_mara
where 1=1"

    echo "End syncing plant adh_mara information"
}


按业务分类同步数据
case $1 in
"dq")
    sync_ods_dq_marc_data
    sync_ods_dq_maex_bsc_data
    sync_ods_dq_maex_crm_data
    sync_ods_dq_mara_data
    sync_ods_dq_mlan_data
    sync_ods_dq_mvke_data
    sync_ods_dq_zv001_data
    sync_ods_dq_zv002_data
    sync_adh_mara_data
    ;;
"mara")
    sync_ods_dq_mara_data
    ;;
"adh_mara")
    sync_adh_mara_data
    ;;
"crm")
    sync_ods_dq_maex_crm_data
    ;;
*)
    echo "plesase use demo!"
    ;;
esac


# if [ "$1"x = "demo"x ];then
# 	echo "name is ok $1"
# 	sync_demo_data 
    
# else
#     echo "plesase use wo_qrcode!"
    
# fi
#!/bin/bash
# Function:
#   同步FTP的文件至Spark服务器
# demo:
# Shell script Path: /var/ftp/Crontab/ftpJob.sh
# History:
# 2022-01-23    Donny   v1.1    update the logic

input_root_path="/var/ftproot"
output_path="/var/ftp/output"
spark_target_path="/root/bscflow/data/ftp"
spark_trigger_path="/root/bscflow/data/trigger"
archive_path="/var/ftp/archive"
backup_data=$(date  +%F)


process_data() {
    data_path="$input_root_path/$1"
    singal_file="$data_path/done.txt"
    echo $data_path
    echo $singal_file

    if [ -f $singal_file ]; then
        #1 parse all excel as csv file
        /usr/bin/python3 /var/ftp/PYScript/ParseExcel.py -i $data_path -o $output_path
        #2 copy to spark env
        mv -f "$singal_file" "$output_path"

        docker cp /var/ftp/output/.  8416b7758c80:/root/bscflow/data/ftp
        #3 copy singnal file to triager foler
        # cp "$singal_file" 8416b7758c80:/root/bscflow/data/trigger
        
        #3 remove all files after syncing to spark
        if [ $? -eq 0 ]
        then
            backup_path="$archive_path/$backup_data" 
            if [ ! -x $backup_path ]
            then
                mkdir -p $backup_path
                chmod 777  $backup_path
            fi
            # backup all files
            from_path="$output_path/*"
            target_path="$backup_path"

            mv -f $from_path $target_path
        fi
        
        
    else
        echo "Please the singal file:done.txt doesn't exist in $data_path"
    fi

}

excel_data_path=(
    'CapitalEquipment'
    'ChengduOperation'
    'CustomerServcie'
    'CustomsInspection'
    'DemandPlanning'
    'EngineeringCenter'
    'OPSQADigital'
    'ProcessImprovement'
    'ProjectsInitiatives'
    'duty_forecast_monthly'
    'duty_forecast_aop'
    'duty_forecast_fcycle'
    'actual_duty_lastweekofmonth'
    'actual_duty_lastdayofmonth'
    'actual_duty_quarterly'
    )

for excel_el in ${excel_data_path[*]};
do
    desc_path="$input_root_path/$excel_el"

    if [ "$(ls -A $desc_path)" ]; then
        echo "----------------------------------------------------------"
        echo "Start parse file under {$excel_el}"
        if [ "$(ls -A $desc_path)" ]; then
            process_data $excel_el
        fi
        echo "Finish parse file under {$excel_el} "
        echo "***********************************************************"
    fi
done
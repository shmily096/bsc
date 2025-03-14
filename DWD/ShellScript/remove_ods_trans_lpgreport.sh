#!/bin/bash

# 设置变量，指定要处理的HDFS路径和文件名模式
hdfs_path="/bsc/opsdw/dwd/dwd_ods_trans_lpgreport"
file_pattern="dt=*"

# 获取所有分区列表，并按日期排序
partitions=( $(hdfs dfs -ls $hdfs_path | grep $file_pattern | awk '{print $NF}' | sort) )
dates=( $(echo ${partitions[@]} | tr ' ' '\n' | cut -d '=' -f 2 | uniq) )

# 计算当前日期和一年前的日期
today=$(date +%Y-%m-%d)
last_year=$(date -d "1 year ago" +%Y-%m-%d)

# 获取超过一年的日期

datex=()
for date in "${dates[@]}"; do
  if [[ "$date" < "$last_year" ]]; then
    datex+=("$date")
  fi
done
 echo "超过一年的分区：${datex[@]}"

# 获取要删除的分区
datey=()
for date in "${datex[@]}"; do
  partitions_in_month=$(echo "${partitions[@]}" | tr ' ' '\n' | grep "$date")
  if [[ ! -z "$partitions_in_month" ]]; then
    last_partition=$(echo "$partitions_in_month" | sort -r | head -n 1)
    year_month=$(echo $last_partition | awk -F 'dt=' '{print $(NF)}' | cut -c 1-7)
    new_date=$(echo $last_partition | awk -F 'dt=' '{print $(NF)}')
    datey+=("$year_month:$new_date")
  fi
done

# 对datey按年月进行group by取最新的分区
declare -A date_map
for d in "${datey[@]}"; do
  year_month=$(echo $d | cut -d ':' -f 1)
  date=$(echo $d | cut -d ':' -f 2)
  if [[ -z "${date_map[$year_month]}" || "${date_map[$year_month]}" < "$date" ]]; then
    date_map[$year_month]=$date
  fi
done
datey=()
for year_month in "${!date_map[@]}"; do
  datey+=("${date_map[$year_month]}")
done
 echo "需要保留的分区：${datey[@]}"

# 存储同时存在于datex和不在datey中的日期
date_z=()
for date_x in "${datex[@]}"; do # 遍历所有在datex中的日期
  skip=
  for date_y in "${datey[@]}"; do # 检查该日期是否在datey中
    if [[ "$date_y" == "$date_x" ]]; then
      skip=1 # 如果存在，则跳过本次循环
      break
    fi
  done
  if [[ ! -z "$skip" ]]; then # 如果存在，则跳过本次循环
    continue
  fi
  date_z+=("$date_x") # 否则将该日期添加到date_z数组中
done
for date_a in "${date_z[@]}"; do
  hdfs_path_new="${hdfs_path}/dt=${date_a}"
  echo "Deleting partition: ${hdfs_path_new}"
  if hdfs dfs -test -d "${hdfs_path_new}"; then
    hdfs dfs -rm -r "${hdfs_path_new}"
  else
    echo "Directory not found: ${hdfs_path_new}"
    continue
  fi
done

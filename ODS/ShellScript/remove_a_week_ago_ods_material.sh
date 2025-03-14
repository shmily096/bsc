echo "Reomve the data a week ago data"
begin_day=-14
end_day=-7
for((i="$begin_day";i<="$end_day";i++))
do
    remove_day=`date -d "$i days" +%F`
	echo "Tobe Removed: ${remove_day}"
	sku_path="/bsc/opsdw/ods/ods_material_master/dt=${remove_day}"
	hdfs dfs -test -d $sku_path
	if [ $? -eq 0 ]
	then
		echo "Start remove $sku_path"
		hdfs dfs -rm -f -r $sku_path
		echo "end remove $sku_path"
	fi
done




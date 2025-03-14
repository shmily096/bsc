# Spark Issues

## 1.  REFRESH TABLE tableName

> It is possible the underlying files have been updated. You can explicitly invalidate the cache in Spark by running 'REFRESHTABLE tableName' command in SQL or by recreating the Dataset/DataFrame involved

**Solution**:

```
You can run spark.catalog.refreshTable(tableName) or spark.sql(s"REFRESH TABLE $tableName") just before the write operation

spark.catalog.refreshTable(tableName)
df.write.mode(SaveMode.Overwrite).insertInto(tableName)
```

## 2. Overwrite partition Issue

```
spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
```

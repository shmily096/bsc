# Spark & MS SQL Server & Hive



## 1 Spark load data from MS SQL Server

```python
from os.path import abspath
import time
from pyspark.sql import SparkSession
from pyspark.sql import Row


if __name__ == "__main__":
    default_db = "opsdw"
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .appName("Spark Export") \
        .config("spark.sql.warehouse.dir", "/bsc/opsdw") \
        .enableHiveSupport() \
        .getOrCreate()

    #connect_str_sqlserver="jdbc:sqlserver://10.226.99.103:16000;username=opsWin;password=;database=APP_OPS;"
    url = "jdbc:sqlserver://10.226.99.103:16000;databaseName=APP_OPS;"
    table_name = "MDM_Plant"
    username = "opsWin"
    password = "opsWinZaq1@wsx"
    jdbcDF = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", username) \
        .option("password", password) \
        .load()
    
    jdbcDF.show()

    spark.stop()
```



## 2 Load data into Hive table

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

appName = "PySpark Hive Example"
master = "local"

# Create Spark session with Hive supported.
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .enableHiveSupport() \
    .getOrCreate()

# Read data from Hive database test_db, table name: test_table.
df = spark.sql("select * from test_db.test_table")
df.show()

# Let's add a new column
df = df.withColumn("NewColumn",lit('Test'))
df.show()

# Save df to a new table in Hive
df.write.mode("overwrite").saveAsTable("test_db.test_table2")
# Show the results using SELECT
spark.sql("select * from test_db.test_table2").show()

# Append data via SQL
spark.sql("insert into test_db.test_table2 values (3, 'GHI', 'SQL INSERT')")
spark.sql("select * from test_db.test_table2").show()

# Append data via code
df = spark.sql("select 4 as id, 'JKL' as value, 'Spark Write Append Mode' as NewColumn")
df.show()
df.write.mode("append").saveAsTable("test_db.test_table2")
spark.sql("select * from test_db.test_table2").show()
```



# 3 DataFrame SaveAsTable

```python
plant_location = "/bsc/opsdw/ods/ods_plant_master/"
    jdbcDF.withColumn('dt', lit(today)).write \
        .partitionBy('dt') \
        .mode("overwrite") \
        .option("path", plant_location) \
        .saveAsTable(f"{default_db}.ods_plant_master")

    spark.sql(f"select * from {default_db}.ods_plant_master").show()
```



```sql
-- opsdw.ods_plant_master definition

CREATE EXTERNAL TABLE `opsdw.ods_plant_master`(
  `plantcode` string, 
  `searchterm2` string, 
  `searchterm1` string, 
  `postlcode` string, 
  `city` string, 
  `name2` string, 
  `name1` string)
PARTITIONED BY ( 
  `dt` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
WITH SERDEPROPERTIES ( 
  'path'='hdfs://spark-master:8020/bsc/opsdw/ods/ods_plant_master') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://spark-master:8020/bsc/opsdw/ods/ods_plant_master'
TBLPROPERTIES (
  'spark.sql.create.version'='3.2.0', 
  'spark.sql.partitionProvider'='catalog', 
  'spark.sql.sources.provider'='parquet', 
  'spark.sql.sources.schema'='{"type":"struct","fields":[{"name":"PlantCode","type":"string","nullable":true,"metadata":{"scale":0}},{"name":"SearchTerm2","type":"string","nullable":true,"metadata":{"scale":0}},{"name":"SearchTerm1","type":"string","nullable":true,"metadata":{"scale":0}},{"name":"PostlCode","type":"string","nullable":true,"metadata":{"scale":0}},{"name":"City","type":"string","nullable":true,"metadata":{"scale":0}},{"name":"Name2","type":"string","nullable":true,"metadata":{"scale":0}},{"name":"Name1","type":"string","nullable":true,"metadata":{"scale":0}},{"name":"dt","type":"string","nullable":true,"metadata":{}}]}', 
  'spark.sql.sources.schema.numPartCols'='1', 
  'spark.sql.sources.schema.partCol.0'='dt', 
  'transient_lastDdlTime'='1638436436');
```

## 4 DF insertInto

```python
jdbcDF.withColumn('dt', lit(today_Ymd)).write \
        .insertInto(f"{default_db}.ods_plant_master3", True)
```


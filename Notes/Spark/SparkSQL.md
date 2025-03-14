# Spark SQL 与Hive 集成

## 1  初始化SparkSession

```python
from os.path import abspath
import time
from pyspark.sql import SparkSession
from pyspark.sql import Row

if __name__ == "__main__":
    default_db = "opsdw"
    spark = SparkSession.builder \
        .appName("OPS DW Spark") \
        .config("spark.sql.warehouse.dir", "/bsc/opsdw") \
        .enableHiveSupport() \
        .getOrCreate()
```



## 2 初始化数据库

```python
default_db = "opsdw"
db_init = f"create database if not exists {default_db} location '/bsc/{default_db}'"
spark.sql(db_init)
```



## 3 创建表

### 3.1 内部分区表

```python
# 选择数据库
spark.sql(f"use {default_db}")

# 创建内部表
demo_table = '''
CREATE TABLE IF NOT EXISTS ods_KV (key int, value  string)
    COMMENT 'Spark DEMO' 
    partitioned by(dt string)
    ROW FORMAT  delimited fields terminated by '\001'
    STORED AS TEXTFILE
    location '/bsc/opsdw/ods/kv/'
    '''
spark.sql(demo_table)
```



## 4 加载数据

### 4.1 从本地路径

```Python
kv_data_path = "/opt/module/spark32/examples/src/main/resources/kv1.txt"
init_demo_data = f"load data local inpath '{kv_data_path}' overwrite into table {default_db}.ods_kv partition(dt='{today}')"
spark.sql(init_demo_data)
```


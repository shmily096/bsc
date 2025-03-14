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
        
    # 初始化数据库
    #db_init = f"create database if not exists {default_db} location '/bsc/{default_db}'"
    #spark.sql(db_init)
    today = time.strftime("%Y-%m-%d", time.localtime())

    # 选择数据库
    spark.sql(f"use {default_db}")

    # 创建表
    demo_table = '''
    CREATE TABLE IF NOT EXISTS ods_kvv (key int, value  string)
    COMMENT 'Spark DEMO' 
    partitioned by(dt string)
    ROW FORMAT  delimited fields terminated by '\001'
    STORED AS TEXTFILE
    location '/bsc/opsdw/ods/kv/'
    '''
    spark.sql(demo_table)
    
    # 加载数据从Local Path
    kv_data_path = "/opt/module/spark32/examples/src/main/resources/kv1.txt"
    init_demo_data = f"load data local inpath '{kv_data_path}' overwrite into table {default_db}.ods_kvv partition(dt='{today}')"
    spark.sql(init_demo_data)


    spark.stop()

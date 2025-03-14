from os.path import abspath
import time
from pyspark.sql import SparkSession
from pyspark.sql import Row


if __name__ == "__main__":
    default_db = "opsdw"
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .appName("Spark Hive PG") \
        .config("spark.sql.warehouse.dir", "/bsc/opsdw") \
        .enableHiveSupport() \
        .getOrCreate()
    
    # configuration information
    pg_db_name = "bscdb"
    url = f"jdbc:postgresql://10.226.98.58:55433/{pg_db_name}"
    table_name = "public.demo"
    username = "postgres"
    password = "1qazxsw2"

    # read the table
    # 选择数据库
    spark.sql(f"use {default_db}")
    # Read the data

    read_kv_str = f"select * from {default_db}.ods_kvv limit 30"
    kv_DF = spark.sql(read_kv_str)
    kv_DF.show()

    '''
    jdbcDF = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", username) \
        .option("password", password) \
        .load()
    
    jdbcDF.show()
    '''
    #write
    kv_DF.write\
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "public.demo4") \
        .option("user", username) \
        .option("password", password) \
        .mode("overwrite") \
        .save()
    
    spark.stop()
from os.path import abspath
import time
from pyspark.sql import SparkSession
from pyspark.sql import Row


if __name__ == "__main__":
    default_db = "opsdw"
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .appName("Spark PG") \
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
    jdbcDF = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", username) \
        .option("password", password) \
        .load()
    
    jdbcDF.show()

    #write
    jdbcDF.write\
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "public.demo3") \
        .option("user", username) \
        .option("password", password) \
        .mode("overwrite") \
        .save()
    
    jdbcDF.show()
    spark.stop()
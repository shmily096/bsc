from os.path import abspath
import time
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import current_date, lit
from datetime import date, timedelta


if __name__ == "__main__":
    default_db = "opsdw"
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .appName("Load data into HIVE from MS SQL via PySpark") \
        .config("spark.sql.warehouse.dir", "/bsc/opsdw") \
        .enableHiveSupport() \
        .getOrCreate()

    #connect_str_sqlserver="jdbc:sqlserver://10.226.99.103:16000;username=opsWin;password=;database=APP_OPS;"
    url = "jdbc:sqlserver://10.226.99.103:16000;databaseName=APP_OPS;"
    table_name = "MDM_Plant"
    username = "opsWin"
    password = "opsWinZaq1@wsx"

    today = date.today()
    today_Ymd = today.strftime("%Y-%m-%d")
    yesterday = today  - timedelta(days=1)

    jdbcDF = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", username) \
        .option("password", password) \
        .load()
    
    """
    # query
    table_query = f"SELECT * FROM TRANS_InventoryOnhand where format(UpdateDT, 'yyyy-MM-dd')='{today_Ymd}'"
    jdbcDF = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("query", table_query) \
        .option("user", username) \
        .option("password", password) \
        .load()
    """
    
    jdbcDF.show()

    # load data into hive and create new table
    # managed table
    
    plant_location = "/bsc/opsdw/ods/ods_plant_master/"
    jdbcDF.withColumn('dt', lit(today_Ymd)).write \
        .partitionBy('dt') \
        .mode("overwrite") \
        .option("path", plant_location) \
        .saveAsTable(f"{default_db}.ods_plant_master")

    spark.sql(f"select * from {default_db}.ods_plant_master").show()
    

    ## insert into exists table
    jdbcDF.withColumn('dt', lit(today_Ymd)).write \
        .insertInto(f"{default_db}.ods_plant_master3", True)

    spark.sql(f"select * from {default_db}.ods_plant_master2").show()
    spark.stop()
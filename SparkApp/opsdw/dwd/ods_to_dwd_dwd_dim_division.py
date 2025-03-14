from os.path import abspath
from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StringType

if __name__ == "__main__":
    default_db = "opsdw"
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .appName("Spark Hive PG") \
        .config("spark.sql.warehouse.dir", "/bsc/opsdw") \
        .enableHiveSupport() \
        .getOrCreate()
    
    today = date.today()
    today_Ymd = today.strftime("%Y-%m-%d")
    
    spark.sql(f"use {default_db}")

    dataSqlStr = f"""
        SELECT 
            DivisionID, 
            Division, 
            ShortName, 
            NameCN, 
            DisplayName, 
            case DisplayName
                when 'PION' then 'product_line4_name'
                when 'IC' then 'product_line3_name'
                when 'URO' then 'product_line2_name'
                when 'PUL' then 'product_line2_name'
            else 'product_line1_name'
            end,
            dt
        FROM opsdw.ods_division_master
        where dt='{today_Ymd}' """
    
    division_df = spark.sql(dataSqlStr)

    division_df.write.insertInto(f"{default_db}.dwd_dim_division", True)

    spark.sql(f"select * from opsdw.dwd_dim_division where dt='{today_Ymd}' limit 5").show()
    spark.stop()
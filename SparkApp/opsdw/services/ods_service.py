#!/usr/local/bin/python3

from pyspark.sql.functions import current_date, lit
from datetime import date, timedelta
from opsdw.services.spark_service import SparkService
from opsdw.ods import ods_config


class ODSService(SparkService):

    def __init__(self, master="spark://spark-master:7077", app_name="OPS DW Spark V1", dw_path="/bsc/opsdw", ods_dw_db="opsdw"):
        super().__init__(master=master, app_name=app_name, dw_path=dw_path)
        self.dw_db = ods_dw_db

    def from_csv(self, schema, csv_file, to_table):
        """load data from csv file

        Args:
            schema (string): table struct
            csv_file (path): csv file
            to_table (string): target table
        """
        # step 1: 初始化DataFrame从CSV文件
        self.logger.info("Start initializing DataFrame from CSV file")
        csv_df = self.spark.read.format('csv') \
            .schema(schema) \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(csv_file) \
            .withColumn('dt', lit(self.today_Ymd))
        self.logger.info("Finish initializing DataFrame from CSV file")

        # step 2: 写入ODS layer
        self.logger.info(f"Start writing the data into {to_table}")
        location = f"{self.dw_path}/ods/{to_table}"
        csv_df.write \
            .partitionBy('dt') \
            .mode("overwrite") \
            .option("path",  location)\
            .saveAsTable(f"{self.dw_db}.{to_table}")
        self.logger.info(f"Finish writing the data into {to_table}")
    
    def from_mssql(self, from_table, to_table):
        url = ods_config.mssql_url
        username = ods_config.mssql_username
        password = ods_config.mssql_password
        
        self.logger.info(f"Start reading data from APP_OPS.{from_table}")
        app_ops_df = self.spark.read \
            .format("com.microsoft.sqlserver.jdbc.spark") \
            .option("url", url) \
            .option("dbtable", from_table) \
            .option("user", username) \
            .option("password", password) \
            .load() \
            .withColumn('dt', lit(self.today_Ymd))
        self.logger.info(f"Finish reading data from APP_OPS.{from_table}")

        location = f"{self.dw_path}/ods/{to_table}"
        self.logger.info(f"Start writing the data into {to_table}")
        app_ops_df.write \
            .partitionBy('dt') \
            .mode("overwrite") \
            .option("path",  location)\
            .saveAsTable(f"{self.dw_db}.{to_table}")
        self.logger.info(f"Finish writing data into {to_table}")
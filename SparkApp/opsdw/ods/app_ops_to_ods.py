from os.path import abspath
import time
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import current_date, lit
from datetime import date, timedelta

from pyspark.sql.types import DecimalType, StringType, StructType

def load_into_hive_from_csv(schema, csv_file, to_table):
    csv_df = spark.read.format('csv') \
        .schema(schema) \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(csv_file) \
        .withColumn('dt', lit(today_Ymd))
    location = f"{dw_root_path}/ods/{to_table}"
    csv_df.write \
        .partitionBy('dt') \
        .mode("overwrite") \
        .option("path",  location)\
        .saveAsTable(f"{dw_db}.{to_table}")

def load_into_hive_from_mssql(from_table, to_table):
    url = "jdbc:sqlserver://10.226.99.103:16000;databaseName=APP_OPS;"
    username = "opsWin"
    password = "opsWinZaq1@wsx"
    
    # read
    app_ops_df = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("dbtable", from_table) \
        .option("user", username) \
        .option("password", password) \
        .load() \
        .withColumn('dt', lit(today_Ymd))
    # write
    location = f"{dw_root_path}/ods/{to_table}"
    app_ops_df.write \
        .partitionBy('dt') \
        .mode("overwrite") \
        .option("path",  location)\
        .saveAsTable(f"{dw_db}.{to_table}")

if __name__ == "__main__":
    '''
    Sync APP_OPS data from MS SQL Server to OPSDW.ODS layer
    '''
    dw_root_path = "/bsc/opsdw"

    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .appName("APP_OPS_TO_ODS") \
        .config("spark.sql.warehouse.dir", dw_root_path) \
        .enableHiveSupport() \
        .getOrCreate()

    # 配置信息
    dw_db = "opsdw"
    today = date.today()
    today_Ymd = today.strftime("%Y-%m-%d")
    yesterday = today  - timedelta(days=1)
  
    # 1 APP_OPS.dbo.MDM_CTMCustomerMaster
    ctm_customer_master_table = 'MDM_CTMCustomerMaster'
    ctm_customer_hive_table = "ods_ctm_customer_master"
    load_into_hive_from_mssql(ctm_customer_master_table, ctm_customer_hive_table)
    

    #2 APP_OPS.dbo.MDM_MaterialMaster
    material_master_table = 'MDM_MaterialMaster'
    ods_material_master_table = "ods_material_master"
    load_into_hive_from_mssql(material_master_table, ods_material_master_table)
  
    #3 APP_OPS:MDM_DivisionMaster
    mdm_division_master = 'MDM_DivisionMaster'
    ods_division_master = "ods_division_master"
    load_into_hive_from_mssql(mdm_division_master, ods_division_master)
    
    #4 ods_add_on_duty_list
    add_on_schema = StructType() \
        .add("hs_code", StringType(), True) \
        .add("coo", StringType(), True) \
        .add("add_on_rate", DecimalType(9,2), True)
    add_on_path = "file:///root/bscflow/data/ftp/add_on_duty_list.xlsx.csv"
    add_on_hive_table = "ods_add_on_duty_list"
    load_into_hive_from_csv(add_on_schema, add_on_path, add_on_hive_table)
   
    #5 ods_inbound_rawdata
    ods_inbound_rawdata_schema = StructType() \
        .add("upn", StringType(), True) \
        .add("storage_location", StringType(), True) \
        .add("Jan_inbound_qty", DecimalType(9,2), True) \
        .add("Feb_inbound_qty", DecimalType(9,2), True) \
        .add("Mar_inbound_qty", DecimalType(9,2), True) \
        .add("Apr_inbound_qty", DecimalType(9,2), True) \
        .add("May_inbound_qty", DecimalType(9,2), True) \
        .add("Jun_inbound_qty", DecimalType(9,2), True) \
        .add("Jul_inbound_qty", DecimalType(9,2), True) \
        .add("Aug_inbound_qty", DecimalType(9,2), True) \
        .add("Sep_inbound_qty", DecimalType(9,2), True) \
        .add("Oct_inbound_qty", DecimalType(9,2), True) \
        .add("Nov_inbound_qty", DecimalType(9,2), True) \
        .add("Dec_inbound_qty", DecimalType(9,2), True) \
        .add("Quantity_flag", DecimalType(9,2), True) 
   
    ods_inbound_rawdata_path = "file:///root/bscflow/data/ftp/DP_Inbound_Rawdata.xlsx.csv"
    ods_inbound_rawdata = "ods_inbound_rawdata"
    load_into_hive_from_csv(ods_inbound_rawdata_schema, ods_inbound_rawdata_path, ods_inbound_rawdata)
    
    #6 ods_inventory_rawdata DP_Inventory_Rawdata.xlsx.csv
    ods_inventory_rawdata_schema = StructType() \
        .add("bi", StringType(), True) \
        .add("upn", StringType(), True) \
        .add("storage_location", StringType(), True) \
        .add("Jan_inv_qty", DecimalType(9,2), True) \
        .add("Feb_inv_qty", DecimalType(9,2), True) \
        .add("Mar_inv_qty", DecimalType(9,2), True) \
        .add("Apr_inv_qty", DecimalType(9,2), True) \
        .add("May_inv_qty", DecimalType(9,2), True) \
        .add("Jun_inv_qty", DecimalType(9,2), True) \
        .add("Jul_inv_qty", DecimalType(9,2), True) \
        .add("Aug_inv_qty", DecimalType(9,2), True) \
        .add("Sep_inv_qty", DecimalType(9,2), True) \
        .add("Oct_inv_qty", DecimalType(9,2), True) \
        .add("Nov_inv_qty", DecimalType(9,2), True) \
        .add("Dec_inv_qty", DecimalType(9,2), True) \
        .add("1uantity_flag", DecimalType(9,2), True) \
        .add("penang_or_not" ,StringType(), True)           
    
    ods_inventory_rawdata_path = "file:///root/bscflow/data/ftp/DP_Inventory_Rawdata.xlsx.csv"
    ods_inventory_rawdata = "ods_inventory_rawdata"
    load_into_hive_from_csv(ods_inventory_rawdata_schema, ods_inventory_rawdata_path, ods_inventory_rawdata)
    
    #7 ods_outbound_rawdata
    ods_kju78bound_rawdata_schema = StructType() \
        .add("upn", StringType(), True) \
        .add("storage_location", StringType(), True) \
        .add("Jan_inbound_qty", DecimalType(9,2), True) \
        .add("Feb_inbound_qty", DecimalType(9,2), True) \
        .add("Mar_inbound_qty", DecimalType(9,2), True) \
        .add("Apr_inbound_qty", DecimalType(9,2), True) \
        .add("May_inbound_qty", DecimalType(9,2), True) \
        .add("Jun_inbound_qty", DecimalType(9,2), True) \
        .add("Jul_inbound_qty", DecimalType(9,2), True) \
        .add("Aug_inbound_qty", DecimalType(9,2), True) \
        .add("Sep_inbound_qty", DecimalType(9,2), True) \
        .add("Oct_inbound_qty", DecimalType(9,2), True) \
        .add("Nov_inbound_qty", DecimalType(9,2), True) \
        .add("Dec_inbound_qty", DecimalType(9,2), True) \
        .add("Quantity_flag", DecimalType(9,2), True) 
   
    ods_outbound_rawdata_path = "file:///root/bscflow/data/ftp/DP_Outbound_Rawdata.xlsx.csv"
    ods_outbound_rawdata = "ods_outbound_rawdata"
    load_into_hive_from_csv(ods_inbound_rawdata_schema, ods_outbound_rawdata_path, ods_outbound_rawdata)
    spark.sql("select * from opsdw.ods_outbound_rawdata limit 10").show()
    
    #8 ods_saving_duty_list
    saving_duty_schema = StructType() \
        .add("hs_code", StringType(), True) \
        .add("coo", StringType(), True) \
        .add("saving_duty_rate", DecimalType(9,2), True)
    saving_duty_path = "file:///root/bscflow/data/ftp/saving_duty_list.xlsx.csv"
    saving_duty_hive_table = "ods_add_on_duty_list"
    load_into_hive_from_csv(saving_duty_schema, saving_duty_path, saving_duty_hive_table)
    
   
    spark.stop()
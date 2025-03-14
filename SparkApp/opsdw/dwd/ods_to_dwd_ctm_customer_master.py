from os.path import abspath
from datetime import date, timedelta
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
    
    today = date.today()
    today_Ymd = today.strftime("%Y-%m-%d")
    
    spark.sql(f"use {default_db}")

    dataSqlStr = f"""
        SELECT
            Material,
            ProductType,
            ChineseName,
            EnglishName,
            HSCode,
            HSAdditionalCode,
            EnterpriseUnit,
            DeclareUnit,
            DeclarationScaleFactor,
            Currency,
            UnitPrice,
            OriginCountry,
            FirstLegalUnit,
            FirstScaleFactor,
            SecondLegalUnit,
            SecondScaleFactor,
            StartExpiryDate,
            BondedProperty,
            MaterialsFlag,
            SpecialRemark,
            Quantity,
            NetWeight,
            GrossWeight,
            `Length`,
            Width,
            Height,
            DistributionProperties,
            ImportDocumentRequirements,
            ExportDocumentRequirements,
            EndExpiryDate,
            SupervisionCertificate,
            InspectionRequirements,
            MFNTaxRate,
            ProvisionalTaxRate,
            CreationDate,
            LastModifyDate,
            dt
        FROM opsdw.ods_ctm_customer_master 
        where dt='{today_Ymd}' """
    
    ctm_cust_df = spark.sql(dataSqlStr)

    ctm_cust_df.write.insertInto(f"{default_db}.dwd_ctm_customer_master", True)

    
    spark.stop()
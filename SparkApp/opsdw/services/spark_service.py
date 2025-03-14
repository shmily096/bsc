#!/usr/local/bin/python3
from logging import Logger
from pyspark.sql import SparkSession
from datetime import date, timedelta

from opsdw.logger.dw_logger import DWLogger

class SparkService(object):
    """Spark Session Service
    """

    # init datatime
    today = date.today() 
    yesterday = today - timedelta(days=1)
    today_Ymd = today.strftime("%Y-%m-%d")
    yesterday_Ymd = yesterday.strftime("%Y-%m-%d")

    spark = None
    logger = DWLogger().logger

    def __init__(self, master="spark://spark-master:7077",app_name="OPS DW Spark V1", dw_path="/bsc/opsdw"):
        self.maseter = master
        self.app_name = app_name
        self.dw_path = dw_path

    def start(self):
        """Initialize and start the SparkSession
        """
        self.logger.info("Initialize and start the SparkSession")
        self.spark = SparkSession.builder \
            .master(self.maseter) \
            .appName(self.app_name) \
            .config("spark.sql.warehouse.dir", self.dw_path) \
            .enableHiveSupport() \
            .getOrCreate()

    def stop(self):
        """Stop SparkSession
        """
        self.logger.info("Stopping SparkSession")
        if self.spark is not None:
            self.spark.stop()
    

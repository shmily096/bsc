from opsdw.logger import dw_logger
from opsdw.services.ods_service import ODSService
from opsdw.logger.dw_logger import DWLogger
from opsdw.ods import ods_config

class ODSController:
    ods = None
    table_dict = ods_config.ods_table_map
    logger = DWLogger().logger

    def __init__(self, target_table):
        self.ods = ODSService(app_name="OPS DW Spark ODS App")
        self.target_table = target_table

    def distribute(self):
        """load data from APP_OPS db via MS SQL Server
        """
        self.ods.start()
        self.logger.info("The spark session is running")

        if self.target_table != ods_config.all_table:
            source_table = self.table_dict[self.target_table]
            self.logger.info(f"Find the source table {source_table}")
            if source_table:
                self.logger.info(f"Start loading data from {source_table} to {self.target_table}")
                self.ods.from_mssql(source_table, self.target_table)
                self.logger.info(f"Finish loading data from {source_table} to {self.target_table}")
             
        else: 
            for target, source in self.table_dict.items():
                    self.logger.info(f"Start loading data from {source} to {target}")
                    self.ods.from_mssql(source, target)
                    self.logger.info(f"Finish loading data from {source} to {target}")
        
        self.ods.stop()
        self.logger.info("The spark session was stoped")

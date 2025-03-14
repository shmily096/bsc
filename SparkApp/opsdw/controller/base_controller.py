class BaseController:
    def __init__(self, service, source_table, target_table):
        self.service = service
        self.source_table = source_table
        self.target_table = target_table
    
    def distribute(self):
        pass
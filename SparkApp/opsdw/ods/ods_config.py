#!/usr/local/bin/python3
"""ODS Layer configure file
"""
ods_table_map = {
    'ods_material_master':'MDM_MaterialMaster',
    'ods_customer_master':'MDM_CustomerMaster',
    'ods_calendar_master':'MDM_Calendar',
    'ods_batch_master':'MDM_BatchMaster',
    'ods_plant_master':'MDM_Plant',
    'ods_storage_location_master':'MDM_StorageLocation',
    'ods_plant_master':'MDM_Plant',
    'ods_division_master':'MDM_DivisionMaster',
    'ods_customermaster_knvv':'MDM_CustomerMaster_KNVV',
    'ods_customermaster_knvi':'MDM_CustomerMaster_KNVI',
    'ods_customermaster_knb1':'MDM_CustomerMaster_KNB1',
    'ods_ctm_customer_master':'MDM_CTMCustomerMaster',
    'ods_idd_master':'MDM_IDD',
    'ods_exchange_rate':'MDM_ExchangeRate',
    'ods_cfda':'MDM_CFDA',
    'ods_cfda_upn':'MDM_CFDA_UPN'
    }

all_table = 'all'

mssql_url = "jdbc:sqlserver://10.226.99.103:16000;databaseName=APP_OPS;"
mssql_username = "opsWin"
mssql_password = "opsWinZaq1@wsx"



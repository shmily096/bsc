#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pymssql
import datetime,time  
import pandas as pd  
from dateutil.relativedelta import relativedelta  
# 连接数据库  
conn_sqlserve = pymssql.connect(  
    host='10.226.99.103',  # 数据库主机地址  
    user='opsWin',  # 数据库用户名  
    password='opsWinZaq1@wsx',  # 数据库密码  
    database='APP_OPS',
    port='16000'# 数据库名称  
)  
  
# 创建游标对象  
cursor_sqlserve = conn_sqlserve.cursor()  
  


# In[2]:


current_date = datetime.date.today()
# 计算昨天的日期  
yesterday = (current_date - datetime.timedelta(days=1) ).strftime("%Y-%m-%d") 
backup_data = current_date.strftime("%Y-%m-%d")
current_time=datetime.datetime.now()

# 获取当前日期  
now = current_time  
  
# 计算二个月前的日期  
three_months_ago = now - relativedelta(months=2)  
  
# 获取该月的第一天  
first_day_of_month = three_months_ago.replace(day=1).strftime("%Y-%m-%d") 


# In[3]:


lpg_report = f"""  select id, updatedt, active, dealercode, dealername, dealertype, nbr, salesdate, 
   divisionid, divisionname, upn, batch, qrcode, qty, unitprice, consignmenttype, 
   ordertype, parentdealercode, parentdealername
  ,'2024-04-08 14:17:03.055712'as insertdt  from trans_consignmenttransfert2 """  


# In[4]:


def func_sqlserver(param1):  
    start_time_1 =time.time()
    cursor_sqlserve.execute(param1)
    columns=[i[0] for i in cursor_sqlserve.description]
    extracted_elements = [item.split('.')[-1] for item in columns]  
    print(extracted_elements)
    df = pd.DataFrame(cursor_sqlserve.fetchall(), columns=extracted_elements)  
    end_time_1= time.time()
    times_1 = round(end_time_1-start_time_1,2)
    return df, times_1


# In[5]:


aa,bb=func_sqlserver(lpg_report)
df_list=[]
df_list.append(aa)


# In[6]:


import psycopg2
from sqlalchemy import create_engine
class DataWriter:
    def __init__(self, engine,conn,cursor):
        self.engine = engine
        self.conn = conn
        self.cursor = cursor

    def write_data_to_table(self, df, table_name, df_value=None):  
        if len(df) == 0:  
            print(f"the sheet name is write to {table_name} not find data")
        elif len(df)>1:
            print("len(df)>1")
            result_data = pd.concat(df, ignore_index=True)
            if df_value:
                result_data[df_value] = result_data[df_value].astype(float)
            table_name_new = table_name.replace('"', '')  
            self.cursor.execute(f"SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname='public' AND tablename='{table_name_new}')")
            exists = self.cursor.fetchone()[0]
            if exists:
                self.cursor.execute(f'truncate table {table_name}')
                self.conn.commit() 
                result_data.to_sql(table_name_new, self.engine, if_exists='append', index=False)
            else:
                print(f"表{table_name} not found，need create")
                result_data.to_sql(table_name_new, self.engine, if_exists='replace', index=False)
            print(f"write to pg table: {table_name}")
        else:
            result_data = df[0]
            print("one df")
            if df_value:
                result_data[df_value] = result_data[df_value].astype(float)
            table_name_new = table_name.replace('"', '')  
            self.cursor.execute(f"SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname='public' AND tablename='{table_name_new}')")
            exists = self.cursor.fetchone()[0]
            if exists:
                self.cursor.execute(f'truncate table {table_name}')
                self.conn.commit() 
                print(table_name)
                result_data.to_sql(table_name_new, self.engine, if_exists='append', index=False)
            else:
                print(f"表{table_name} not found，need create")
                result_data.to_sql(table_name_new, self.engine, if_exists='replace', index=False)
            print(f"write to pg table: {table_name}")           
# 创建DataWriter对象并传入数据库连接引擎
engine=create_engine('postgresql://postgres:1qazxsw2@10.226.98.58:55433/tableaudb')
conn = psycopg2.connect(database="tableaudb", user="postgres", password="1qazxsw2", host="10.226.98.58", port="55433")
cursor = conn.cursor()
writer = DataWriter(engine,conn,cursor)


# In[7]:


writer.write_data_to_table(df_list, '"trans_consignmenttransfert2"')


# In[ ]:





#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import psycopg2  
import pandas as pd  
import sys
from sqlalchemy import create_engine
import pymysql
import datetime
current_time=datetime.datetime.now()
from_table =sys.argv[1]
print(f'读取aws的{from_table} 到pg的tableau aws_{from_table},,{current_time}')
# 连接到PostgreSQL数据库   
engine = create_engine('postgresql://postgres:1qazxsw2@10.226.98.58:55433/tableaudb')


# In[ ]:


# 打开文件并读取内容  
with open('/bscflow/dwd/aws_coonect_user.file', 'r') as file:  
    content = file.read()   
# 打印文件内容  
print(content)
 # 连接到 MySQL 数据库
conn = pymysql.connect(host='chinapoms.cqvn7cllbyhs.rds.cn-north-1.amazonaws.com.cn', user='VisXKpsZYV', password=content, database='inbound',
                       port=3306, charset='utf8')
# 创建一个游标对象  
cursor = conn.cursor()
# 执行SQL查询语句  
cursor.execute(f"select * from {from_table}")  
# 获取查询结果并将其存储在DataFrame中  
df = pd.DataFrame(cursor.fetchall(), columns=[i[0] for i in cursor.description])  
  
# 关闭数据库连接  
conn.close()


# In[ ]:


#列名小写并替换掉特殊字符，并且添加两列，分别是插入日期和表名+sheet名称
def process_header(df,file):  
    try:  
        header = list(df.columns)  
        header = [str(h).replace('.', '').replace('(', '').replace(')', '').replace('/', ' ').replace('-', '')
          .replace('\n', ' ').replace('"', '').replace('…', '').replace('\\', '').replace('%', '').replace('#', '')
          .replace(' ', '').replace('$', '').lower() for h in header]
        df.columns = header 
        updatedt_items=[current_time for i in range(len(df))]
        df.insert(loc=0,column='updatedt',value=updatedt_items)
        #生成一个列表把字符串重复行数次
        a_file_name=[file for i in range(len(df))]
        #把文件名放到dataframe里面
        df.insert(loc=0,column='source_name',value=a_file_name)
    except Exception as e:  
        print(f"Error occurred: {e}")  
        return None 


# In[ ]:


process_header(df,from_table)
# 将DataFrame导入PostgreSQL数据库  
table_name = 'aws_' + from_table.lower()  
print(f"开始写入到{table_name}")
df.to_sql(table_name, engine, if_exists='replace', index=False)
# 提交事务并关闭连接  
engine.dispose()
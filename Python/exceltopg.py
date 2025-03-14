import pandas as pd
import os
current_working_directory = os.getcwd()
print(current_working_directory)
excel_file = r'C:\sway\工作文档\perfect_control_list.xlsx'
df = pd.read_excel(excel_file,sheet_name='Sheet2',engine='openpyxl')
header=list(df.columns)
header=[str(h).lower() for h in header]
print(header)


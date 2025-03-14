import pandas as pd

df = pd.DataFrame({'国家': ['中国', '美国', '日本'],
                   '地区': ['亚洲', '北美', '亚洲'],
                   '人口': [13.97, 3.28, 1.26],
                   'GDP': [14.34, 21.43, 5.08],
                  })
df.set_index("国家",inplace=True)
# print(df["人口"])

file_name=r'C:\Python313\testdata\team.xlsx'
df2=pd.read_excel(file_name,index_col='name')
# print(df2.head(5))
# print(df2.shape)
# print(df2.info)
print(df2.index)
import os
path =os.getcwd() #获取当前文件路径
file_path=r'C:\Python313\testdata'
file_list=os.listdir(file_path) #查看文件夹下所有文件和子文件夹名称
file_name=r'C:\Python313\testdata\numbers.json'
separate =os.path.splitext(file_name) #分离文件的文件主名和扩展名
print(separate)

# oldname = 'd:\\list\\test.xlsx'
# newname = 'd:\\list\\example.xlsx'
# os.rename(oldname, newname) #重命名文件或文件夹，修改文件路径


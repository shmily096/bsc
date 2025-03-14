import xlwings as xw

"""
代码启动Excel程序窗口，但不新建工作簿。其中的App()是xlwings模块中的函数，
该函数有两个常用参数：
参数visible用于设置Excel程序窗口的可见性，
如果为True，表示显示Excel程序窗口，如果为False，表示隐藏Excel程序窗口；
参数add_book用于设置启动Excel程序窗口后是否新建工作簿，
如果为True，表示新建一个工作簿，如果为False，表示不新建工作簿。
报错：pywintypes.com_error: (-2147352573, '找不到成员。', None, None)
原因：没有安装office，或者既安装了office又安装了wps,
解决：卸载wps,或者修改wps配置，不关联xls,xlsx文件，不作为excel默认打开方式
"""
file_name=r'C:\Python313\testdata\example2.xlsx'
#app=xw.App(visible=True,add_book=False) 
app=xw.App(visible=False) 
workbook=app.books.add() #新建工作簿
worksheet=workbook.sheets.add('产品统计表')
worksheet.range('A1').value='编号'
workbook.save(file_name) 
workbook.close() #关闭工作簿
app.quit() #退出excel程序

#workbook=app.books.open(file_name) #打开文件
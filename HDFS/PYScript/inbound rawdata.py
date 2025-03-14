#!/usr/bin/python3

import xlrd
import csv
import sys
import getopt
import os
import random

def saveExcelAsCSV(filename, output_path):
    """
    解析Excel文件，并保存为CSV格式，默认从第二行开始取值
    """
    (input_path, input_filename) = os.path.split(filename)
    fail_name = os.path.join(input_path, "fail.txt")
    success_name = os.path.join(input_path, "success.txt")
    try:
        # 1 打开Excel文件
        book = xlrd.open_workbook(filename)
        # 2 读取Sheet内容，从第二行开始
        data_row = []
        for sheet in book.sheets():
            for row in range(1,sheet.nrows):
                data_row.append(sheet.row_values(row))

        #3 写入至CSV文件中
        result_name = os.path.join(output_path, input_filename+str(random.randint(1, 100))+".csv")
        with open(result_name,'w+', newline= '') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerows(data_row)

    except Exception as e:
        print(e)
        with open(fail_name,'w+') as f:
            f.write("Failed")
    else:
        with open(success_name,'w+') as f:
            f.write("Successfully")
    finally:
        print("Finished sync files")


def parseArgv():
    """
    获取命令行的input目录和target目录
    """
    intput_path = None
    output_path = None
 
    argv = sys.argv[1:]
 
    try:
        opts, args = getopt.getopt(argv, "hi:o:", ["inputPath=","outputPath="])
     
    except getopt.GetoptError:
        print ( 'ParseExcel.py -i <inputPath> -o <outputPath>' )
        sys.exit(2)
 
    for opt, arg in opts:
        if opt in ['-i', 'inputPath']:
            intput_path = arg
        elif opt in ['-o', 'outputPath']:
            output_path = arg
        elif opt == '-h':
            print ( 'ParseExcel.py -i <inputPath> -o <outputPath>' )
            sys.exit()

    return (intput_path, output_path)
     

def findAllExcelFile(input_path):
    """
    查找对应目录下的所有Excel文件
    """
    for root, ds, files in os.walk(input_path):
        for file in files:
            if file.endswith('.xlsx'):
                fullname = os.path.join(input_path, file)
                yield fullname

def main():
    #1 获取input & output 目录
    (input_path, output_path) = parseArgv()

    #2 开始解析
    if input_path is None or output_path is None:
        print ('ParseExcel.py -i <inputPath> -o <outputPath>' )
    else:
        for excel_fullname in findAllExcelFile(input_path):
            saveExcelAsCSV(excel_fullname, output_path)
            os.remove(excel_fullname)

 
if __name__ == "__main__":
    main()
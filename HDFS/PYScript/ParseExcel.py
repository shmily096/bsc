#!/usr/bin/python3

from datetime import *
from dateutil.parser import parse
import time
import random
import os
import getopt
import sys
import csv
import xlrd
xlrd.xlsx.ensure_elementtree_imported(False, None)
xlrd.xlsx.Element_has_iter = True


def is_valid_date(str_date):
    new_date = None
    date_format = None
    try:
        if '/' in str_date:
            date_format = "%Y/%m/%d"
        else:
            date_format = "%Y-%m-%d"

        new_date = datetime.strptime(str_date, date_format)
        new_date = parse(str_date).date()
        return (True, new_date)
    except:
        return (False, new_date)


def saveExcelAsCSV(filename, output_path):
    """
    解析Excel文件，并保存为CSV格式，默认从第二行开始取值
    """
    tp_Report_name = 'TP_Validation_Report'
    (input_path, input_filename) = os.path.split(filename)
    fail_name = os.path.join(input_path, "fail.txt")
    success_name = os.path.join(input_path, "success.txt")
    is_tp_report = tp_Report_name.lower() in input_filename.lower()

    try:
        # 1 打开Excel文件
        book = xlrd.open_workbook(filename)
        # 2 读取Sheet内容，从第一行开始
        data_row = []
        # get the first sheet
        sheet = book.sheet_by_index(0)
        for row in range(1, sheet.nrows):
            line_row = []
            for col in range(0, sheet.ncols):
                ctype = sheet.cell(row, col).ctype
                cell_value = sheet.cell(row, col).value

                # TP Report begin  TP Reprot col 7, 8 Valide from date & valide to date is number. 20220101
                if is_tp_report and row > 0 and col in [6, 7]:
                    cell_value = parse(str(int(cell_value))).date()
                # TP Report end
                else:
                    if isinstance(cell_value, float) and int(cell_value) == cell_value:
                        cell_value = int(cell_value)
                    elif isinstance(cell_value, float):
                        cell_value = f'{(cell_value):.2f}'
                    elif isinstance(cell_value, date) or isinstance(cell_value, datetime):
                        cell_value = parse(str(cell_value)).date()
                    elif isinstance(cell_value, str):
                        (ret, ret_result) = is_valid_date(cell_value)
                        if ret:
                            cell_value = ret_result
                    else:
                        cell_value = str(cell_value)

                new_cell_value = str(cell_value).replace('\n', '').strip()
                if len(new_cell_value) == 0 :
                    new_cell_value = '\\N'

                line_row.append(new_cell_value)
            data_row.append(line_row)
        print(len(data_row))
        # 3 写入至CSV文件中
        old_name, f_ext = os.path.splitext(input_filename)
        new_name = os.path.join(output_path, old_name + ".csv")
        with open(new_name, 'w+', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerows(data_row)

    except Exception as e:
        with open(fail_name, 'w+', encoding='utf-8') as f:
            f.write(repr(e))
    else:
        os.remove(filename)
        with open(success_name, 'w+', encoding='utf-8') as f:
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
        opts, args = getopt.getopt(
            argv, "hi:o:", ["inputPath=", "outputPath="])

    except getopt.GetoptError:
        print('ParseExcel.py -i <inputPath> -o <outputPath>')
        sys.exit(2)

    for opt, arg in opts:
        if opt in ['-i', 'inputPath']:
            intput_path = arg
        elif opt in ['-o', 'outputPath']:
            output_path = arg
        elif opt == '-h':
            print('ParseExcel.py -i <inputPath> -o <outputPath>')
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
    # 1 获取input & output 目录
    (input_path, output_path) = parseArgv()

    # 2 开始解析
    if input_path is None or output_path is None:
        print('ParseExcel.py -i <inputPath> -o <outputPath>')
    else:
        for excel_fullname in findAllExcelFile(input_path):
            saveExcelAsCSV(excel_fullname, output_path)


if __name__ == "__main__":
    main()

# -*- coding:utf-8 -*-

'''
Org Ctry , Qty, Ext Price
CNY
'''
import json
import os
import re
import time
from collections import OrderedDict
import pandas as pd
import numpy as np
from Settings import const
from Settings import logger
from .tools import CTMaterialMaster,CountryMapping
import datetime
from shutil import move

#xul12 add
import psycopg2
import getpass
#xul12 end

cm = CountryMapping(const.CTM_COO_MASTER_PATH,const.COUNTRY_JSON)
country_dic = cm.get_country_mapping()
#xul12 add
conn = psycopg2.connect("dbname=postgres user=readonly password='readonly ' host=10.226.98.58 port=55433")
cursor = conn.cursor()

#链接pg读取数据
cursor.execute("SELECT upn, array_agg(coo) AS country_code_pg FROM public.dwd_trans_lpgreport GROUP BY upn")
pg_data = cursor.fetchall()
#xul12 end
class CommonParser:
    def __init__(self,file_dic):

        # {'invoiceNr':invoiceNr,'grossWeight':grossWeight,'dateStr':dateStr,'filepath':fpath,'prefix':prefix}
        self.invoiceNr = file_dic['invoiceNr']
        self.venderNr = self.get_venderNr_by_prefix(file_dic['prefix'])
        self.coo_short = self.get_coo_short_by_prefix(file_dic['prefix'])
        self.planNr = self.get_planNr_by_prefix(file_dic['prefix'])
        self.dateStr = file_dic['dateStr']
        self.grossWeight = file_dic['grossWeight']
        self.excel_path = file_dic['filepath']
        self.column_with_same_value = {
            'Invoic Number': self.invoiceNr,
            'Vendor(Ship From)': self.venderNr,
            'Purchase order': 99999999,
            'UOM': 'EA',
            '进口最终目的国/出口原产国': 'CN',
            '表头贸易国': self.coo_short
        }
        self.template_coumns = ['Invoic Number', '# Pack', 'Vendor(Ship From)', '进口原产国/出口最终目的国',
                                'Purchase order', 'Part Number', 'AMOUNT', 'QTY', 'CURRENCY', 'UOM', 'English Name',
                                'Net Weight(Kg)', 'Weight UOM', 'B/L', 'Vessel/Airline', 'VERSION #', 'COST_CENTER',
                                'PROJECT_NUMBER', 'Gross Weight', 'CBM', 'Tracking ID', 'HS编码', '中文品名', '规格型号',
                                'Origin Country', 'Length', 'Width', 'Height', 'Material Cost', 'Number Of Pieces',
                                '付款方',
                                '订单项号', '销售订单号', '销售订单项号', '客户物料编号', '计划员', '进口最终目的国/出口原产国', '表头贸易国', '其他申报数量',
                                '其他申报单位',
                                '备注', '备注2', '备注3', '备注4','财务发票号','买方卖方','保税属性（Y/N)','供应商(出口)/最终客户(进口)','境内货源地(出口)/境内目的地(进口)','行政区划','生产厂商']
        logger.info('Processing: %s' %self.excel_path)
        self.has_error = False
        self.result_report = []

    def get_venderNr_by_prefix(self,prefix):
        vender_number = const.CTM_SAP_COO.get(str(prefix),{}).get('venderNr','')
        if not vender_number:
            error_msg =  'Vender Number不存在，请核对文件前缀:%s!'%prefix
            logger.error(error_msg)
            self.result_report.append(['ERROR',error_msg])
            self.has_error = True
        return vender_number
    #xul12 add   
    def write_result_waring_report_df_to_pg_logger(self, result_waring_report_df):
        for row in result_waring_report_df:

            level = row[0]
            message = row[1]

            current_user = re.search(r"当前用户：(\w+)", message).group(1)

            current_time_str = re.search(r"当前时间：(.+?),", message).group(1)

            current_time = pd.to_datetime(current_time_str).strftime('%Y-%m-%d %H:%M:%S.%f')
            
            excel_file = re.search(r"Excel file:(.+?),", message).group(1)
            
            exlce_upn = re.search(r"exlce_upn:(.+?),", message).group(1)

            match = re.search(r"exlce的值:(\w+)", message)
            if match:
                exlce_value = match.group(1)
            else:
                exlce_value = '#'            
            prefix_type = re.search(r"filetype:(\w+)", message).group(1)
            invoiceNr_value = re.search(r"invoice:(\w+)", message).group(1)
            grossWeight_value = re.search(r"毛重:(\w+)", message).group(1)
            dateStr = re.search(r"dateStr:(\w+)", message).group(1)
            date_time_obj = datetime.datetime.strptime(dateStr.replace(',', ''), '%Y%m%d%H%M') # 将字符串转换为 datetime 对象
            formatted_date_time = date_time_obj.strftime("%Y-%m-%d %H:%M:%S")

            error_type = re.search(r"错误类型:(\w+)", message).group(1)

            error_details = re.search(r"错误明细:(.+)", message).group(1)
            if error_type:
                error_type = error_type
            else:
                error_type = ""
            cursor.execute("INSERT INTO public.pg_logger(loglevel,prefix_type,invoicenr_value,grossweight_value,datestr,error_type,error_details,exlce_upn,exlce_value,currentuser,currenttime,excel_file) VALUES('%s','%s','%s','%s','%s','%s','%s', '%s', '%s', '%s', '%s', '%s')"%(level,prefix_type,invoiceNr_value, grossWeight_value,formatted_date_time,error_type,error_details, exlce_upn, exlce_value, current_user, current_time, excel_file))
        conn.commit()
        


            #xul12 end
    #xul12 add
    def collect_pg_and_excle_mapping_upn_check_country_code(self, excel_path,df_excel, upn_key, country_code_value):
        result_waring_report = []
        #获取当前用户名称
        username = getpass.getuser()
        # 读取Excel数据并提取需要的列
        df_excel = df_excel.loc[:,[upn_key, country_code_value]].copy()

        #pg的列名加个后缀_pg
        pg_colume_name = country_code_value + "_pg"
        #给值加个列名
        df_pg = pd.DataFrame(pg_data, columns=[upn_key, pg_colume_name])
        #把列里面的值都取出来转成字符串按逗号分割
        df_pg.loc[:,pg_colume_name] = df_pg.loc[:,pg_colume_name].apply(lambda x: ",".join([str(v) for v in x])).copy()
        #把excle的类型转成字符串的类型
        df_excel.loc[:,upn_key] = df_excel.loc[:,upn_key].astype(str).copy()
        #df_excel[upn_key] = df_excel[upn_key].apply(str)
        df_excel.loc[:,country_code_value] = df_excel.loc[:,country_code_value].astype(str).copy()
        #去空格
        df_excel.loc[:,upn_key] =df_excel.loc[:,upn_key].str.strip().copy()
        df_excel.loc[:,country_code_value] = df_excel.loc[:,country_code_value].str.strip().copy()
        #改大写
        df_excel.loc[:,upn_key] =df_excel.loc[:,upn_key].str.upper().copy()
        df_excel.loc[:,country_code_value] = df_excel.loc[:,country_code_value].str.upper().copy()
        df_pg.loc[:,upn_key]=df_pg.loc[:,upn_key].str.upper().copy()
        df_pg.loc[:,pg_colume_name]=df_pg.loc[:,pg_colume_name].str.upper().copy()
        # 合并两个DataFrame，并进行left join操作
        df_merged = pd.merge(df_excel, df_pg, on=upn_key, how='left')

        # 遍历每个行并判断country_code_value是否存在于pg_colume_name中
        mvt_pg_dict = df_pg.set_index(upn_key)[pg_colume_name].apply(lambda x: set(x.split(','))).to_dict()

        for index, row in df_merged.iterrows():
            mvt_x = str(row[country_code_value]).split(",")
            mvt_y = mvt_pg_dict.get(row[upn_key], set())
            #提取文件名称
            exlce_name = os.path.basename(excel_path)
            prefix,invoiceNr,grossWeight,dateStr = exlce_name.split('_')
            excle_value=','.join(mvt_x).strip('[]')
            pg_value=','.join(mvt_y).strip('[]')

            date_time_str = dateStr[:13].replace(" ", "") # 提取日期时间字符串部分
            a=['空值']
            warning_msg = f"当前用户：{username}, 当前时间：{datetime.datetime.now()}, Excel file:{exlce_name}, exlce_upn:{row[upn_key]},exlce的值:{excle_value},filetype:{prefix},invoice:{invoiceNr},毛重:{grossWeight},dateStr:{date_time_str}"
            if pd.isnull(row[pg_colume_name]) or row[pg_colume_name] == "":
                if mvt_x ==a:
                    warning_msg =(f"{warning_msg},错误类型:LDS数据库缺失且发票是空值,错误明细:upn={row[upn_key]}")
                    out_put_summery=f"Excel file:{exlce_name},错误类型:LDS数据库缺失且发票是空值,错误明细:upn={row[upn_key]}"
                    logger.warning(out_put_summery)
                    result_waring_report.append(['WARNING', warning_msg])
                    self.result_report.append(['ERROR',out_put_summery])
                else:
                    warning_msg =(f"{warning_msg},错误类型:仅LDS数据库缺失,错误明细:upn={row[upn_key]},发票是{mvt_x[0]}")
                    out_put_summery=f"Excel file:{exlce_name},错误类型:仅LDS数据库缺失,错误明细:upn={row[upn_key]},发票是{mvt_x[0]}"
                    logger.warning(out_put_summery)
                    result_waring_report.append(['WARNING', warning_msg])
                    self.result_report.append(['ERROR',out_put_summery])
            elif set(mvt_x).issubset(mvt_y):
                pass
            else:
                diff = set(mvt_x) - mvt_y
                excel_value_2=','.join(list(diff)).strip('[]')
                warning_msg =(f"{warning_msg}, 错误类型:数据不一致,错误明细:upn={row[upn_key]},发票是{excel_value_2},LDS数据库是{pg_value}")
                out_put_summery=f"Excel file:{exlce_name},错误类型:数据不一致,错误明细:upn={row[upn_key]},发票是{excel_value_2},LDS数据库是{pg_value}"
                logger.warning(out_put_summery)
                result_waring_report.append(['WARNING', warning_msg])
                self.result_report.append(['ERROR',out_put_summery])
        return result_waring_report

        #xul12 end

    def get_coo_short_by_prefix(self,prefix):
        coo_short = const.CTM_SAP_COO.get(str(prefix),{}).get('COO short','')
        if not coo_short:
            error_msg =  'COO short不存在，请核对prefix:%s!'%prefix
            logger.error(error_msg)
            self.result_report.append(['ERROR',error_msg])
            self.has_error = True
        return coo_short

    def extract_info(self):
    #xul12 add增加一个返回参数
        return '','','',''

    def check_empty_value(self,Part_Number,column_value_list):

        for i,v in enumerate(column_value_list):
            if pd.isnull(v) or not v:
                return Part_Number[i]

    def check_column_name(self,original_column,standard_column):

        res_columns = []

        lower_standard_column = [i.lower() for i in standard_column]

        for colname in original_column:
            if colname not in standard_column:

                if str(colname).strip().lower() in lower_standard_column:

                    lower_column_index = lower_standard_column.index(colname.strip().lower())

                    formated_column_name = standard_column[lower_column_index]
                    res_columns.append(formated_column_name)
                else:
                    # raise Exception('Unkown column name: %s'%colname)
                    res_columns.append(colname)

            else:
                res_columns.append(colname)

        return res_columns

    def get_planNr_by_prefix(self,prefix):
        planNr = const.CTM_SAP_COO.get(str(prefix), {}).get('SAP Plant Nr', '')
        if not planNr:
            error_msg = 'SAP Plant Nr不存在，请核对文件前缀:%s ' % prefix
            logger.error(error_msg)
            self.result_report.append(['ERROR', error_msg])
            self.has_error = True
        return planNr


    def save_delivery(self,delivery_df, out_folder):
        # SAP PlantNr_InvoiceNr.csv
        delivery_filename = '%s_%s.csv'%(self.planNr,self.invoiceNr)
        output_delivery = os.path.join(out_folder,delivery_filename)
        delivery_df.to_csv(output_delivery, index=False, sep=',',float_format='%.2f')


    def check_amount_qty_grossWeight(self,total_df,total_amount,total_qty):
        extracted_amount = total_df['AMOUNT'].sum()
        extracted_qty = total_df['QTY'].sum()
        extracted_grossWight = total_df['Net Weight(Kg)'].sum()


        if round(extracted_amount,2) != round(total_amount,2):
            error_msg = '请检查金额 :%s(%s,%s)'%(self.excel_path,extracted_amount,total_amount)
            logger.error(error_msg)
            self.result_report.append(['ERROR', error_msg])
            self.has_error = True

        if extracted_qty != total_qty:
            error_msg = '请检查数量: %s' % (self.excel_path)
            logger.error(error_msg)
            self.result_report.append(['ERROR', error_msg])
            self.has_error = True

        # check extracted grossweight!
        if round(float(extracted_grossWight),6)>round(float(self.grossWeight),6):
            warning_msg = '净重大于毛重(%s,%s)'%(extracted_grossWight,self.grossWeight)
            logger.warning(warning_msg)
            self.result_report.append(['WARNING', warning_msg])


    def save_template(self,output_folder,archive_folder,delivery_csv_folder):
        output_filename = 'TEM%s_%s_%s_%s' % (self.venderNr, self.invoiceNr, self.grossWeight, self.dateStr)
        name,suffix = os.path.splitext(output_filename)

        if suffix in ['.xls','.XLS']:
            output_filename = name + '.xlsx'
        output_template = os.path.join(output_folder, output_filename)
        #xul12 change add result_waring_report_df
        extracted_df,delivery_df,total_amount,total_qty = self.extract_info()
        #extracted_df,delivery_df,total_amount,total_qty,result_waring_report_df = self.extract_info()
        

        # amount 列保留两位小数 todo check
        amount_column = extracted_df['AMOUNT']

        formated_amount_column = [round(i, 2) for i in amount_column]

        extracted_df['AMOUNT'] = formated_amount_column

        currency_type = extracted_df['CURRENCY'].iloc[0]
        logger.info('总数量：%s, 总金额：%s, 币种：%s'%(total_qty,total_amount,currency_type))

        # 2. calculate net weight, (QTY* CTM Material Master中净重！)
        cmm = CTMaterialMaster(const.CTM_MATERIAL_MASTER_PATH)
        extracted_df['Part Number'] = extracted_df['Part Number'].astype(str)
        total_df = pd.merge(extracted_df, cmm.df.loc[:, ['Part Number', 'Net Weight(Kg)', '特殊备注']], on='Part Number',
                            how='left')

        # check empty net weight
        net_weight = total_df['Net Weight(Kg)']
        if net_weight.isnull().any():
            need_check = self.check_empty_value(total_df['Part Number'].to_list(), net_weight.to_list())
            error_msg = '净重不存在，请核对文件:%s,请核对Part Number:%s' % (self.excel_path,need_check)
            logger.error(error_msg)
            self.result_report.append(['ERROR', error_msg])
            self.has_error = True

        # check the empty '特殊备注' --> material no
        special_comment = total_df['特殊备注']
        if special_comment.isnull().any():
            need_check = self.check_empty_value(total_df['Part Number'].to_list(),special_comment.to_list())
            error_msg = '特殊备注不存在，请核对文件:%s ,请核对Part Number:%s'%(self.excel_path,need_check)
            logger.error(error_msg)
            self.result_report.append(['ERROR', error_msg])
            self.has_error = True

        total_df['Part Number'] = special_comment

        # 4. calculate the amount
        total_df['Net Weight(Kg)'] = total_df['Net Weight(Kg)'].mul(total_df['QTY'])


        # 5. add columns with fixed value
        for k,v in self.column_with_same_value.items():
            total_df[k] = v

        # 6. add columns with empty value.
        total_df_columns = total_df.columns
        for i in self.template_coumns:
            if i not in total_df_columns:
                total_df[i] = np.nan

        # 7. reorder the columns
        total_df = total_df[self.template_coumns]

        # 8. save into excel file
        #设置AMOUNT为保留2位小数
        total_df['AMOUNT'] = total_df['AMOUNT'].round(2)

        total_df.to_excel(output_template,index=False,float_format='%.3f')

        delivery_df['Active'] = 1

        self.save_delivery(delivery_df,delivery_csv_folder)
        self.check_amount_qty_grossWeight(total_df,total_amount,total_qty)

        # move to archive
        if not self.has_error:
            folderpath,filename = os.path.split(self.excel_path)
            today_date = time.strftime('%Y-%m-%d')
            archive_date_folder = os.path.join(archive_folder,today_date)
            if not os.path.exists(archive_date_folder):
                os.mkdir(archive_date_folder)
            archive_path = os.path.join(archive_date_folder,filename)
            move(self.excel_path,archive_path)
            msg = 'Successfully move to achive!'
            logger.info(msg)
            self.result_report.append(['INFO', msg])

        return {
            '总数量':total_qty,
            '总金额': total_amount,
            '币种':currency_type,
            '日志':self.result_report
        }


class Parser525(CommonParser):
    '''
    525: Kerkrade
    '''
    def __init__(self,file_dic):
        super(Parser525,self).__init__(file_dic)

    #xul12add df_excel = df[['Org Ctry']].replace(np.nan, '空值', inplace=True)
    def collect_template_value(self,data,real_data_df_row_index,currency_value):
        extract_columns = ['UPN Number', 'Org Ctry', 'Qty', 'Ext Price']

        df = data.loc[real_data_df_row_index, extract_columns]
        total_amount = df['Ext Price'].sum()
        total_qty = df['Qty'].sum()
        #xul12add
        df['Org Ctry'].fillna('空值', inplace=True)
        #xul12 end
        extraced_df = df.groupby(['UPN Number', 'Org Ctry'], as_index=False, sort=False).sum()
        extraced_df.rename(columns={'UPN Number': 'Part Number',
                                    'Org Ctry': '进口原产国/出口最终目的国',
                                    'Qty': 'QTY',
                                    'Ext Price': 'AMOUNT'}, inplace=True)

        extraced_df['CURRENCY'] = currency_value

        return extraced_df,total_amount,total_qty


    def collect_delivery_value(self,data,real_data_df_row_index):
        delivery_columns = ['Delivery Number','Qty']
        df = data.loc[real_data_df_row_index,delivery_columns]
        extracted_df = df.groupby(['Delivery Number'],as_index=False,sort=False).sum()
        extracted_df.rename(columns={'Delivery Number': 'Delivery',
                                    'Qty': 'Qty'}, inplace=True)

        extracted_df['Invoice'] = self.invoiceNr
        data_str = self.dateStr.split('.')[0]
        day = datetime.datetime.strptime(data_str, '%Y%m%d %H%M')
        formated_date = datetime.datetime.strftime(day, '%Y-%m-%d %H:%M')

        extracted_df['Mail Received'] = formated_date
        return extracted_df


    def extract_info(self):

        self.standard_column = ['SAP Invoice Number','Delivery Number','UPN Number','Description','Batch Number','Expiry Date','Comm No','Org Ctry','Qty','Net Unit','Ext Price']

        currency_abbr = ''
        total_abbr = 0
        data = pd.read_excel(self.excel_path)
        rows = data.shape[0]
        cols = data.shape[1]
        real_colmn_index = 0
        for i in range(rows):
            row_value = data.loc[i].to_list()
            lower_row_value = [str(i).lower() for i in row_value]
            if 'Org Ctry'.strip().lower() in lower_row_value:
                real_colmn_index = i + 1
                original_data_column = data.loc[i].values
                formated_data_columns = self.check_column_name(original_data_column, self.standard_column)
                data.columns = formated_data_columns

                break
        real_data_df_row_index = []
        for j in range(real_colmn_index, rows):
            #slc change 就下面1行
            if not any([pd.isnull(data.loc[j, self.standard_column[0]])]):
            #if not any([pd.isnull(data.loc[j, c]) for c in self.standard_column]):
                real_data_df_row_index.append(j)


            else:
                data_list = data.loc[j].to_list()
                if 'Currency' in data_list:
                    currency_index = data_list.index('Currency')
                    currency_abbr = data_list[currency_index + 1]

                elif 'Total' in data_list:
                    total_index = data_list.index('Total')
                    total_abbr = data_list[total_index + 1]


        delivery_df = self.collect_delivery_value(data,real_data_df_row_index)

        extraced_df,total_amount,total_qty = self.collect_template_value(data,real_data_df_row_index,currency_abbr)

        #return extraced_df,delivery_df,total_amount,total_qty
        #xul12 add
        self.df_excel = extraced_df
        self.upn_key ='Part Number'
        self.country_code_value ='进口原产国/出口最终目的国'
        result_waring_report_df = self.collect_pg_and_excle_mapping_upn_check_country_code(self.excel_path,self.df_excel,self.upn_key,self.country_code_value)
        self.write_result_waring_report_df_to_pg_logger(result_waring_report_df)
        extraced_df['进口原产国/出口最终目的国'] = extraced_df['进口原产国/出口最终目的国'].replace('空值', '')
        if round(total_amount,2) != round(total_abbr,2):
            raise Exception('Error! Please check amount %s, %s,**%s'%(total_abbr,total_amount,self.result_report))
            #raise Exception(self.result_report)
        #xul12 end
        #xul12 change
        return extraced_df,delivery_df,total_amount,total_qty
        #xul12 end

class Parser275(CommonParser):
    def __init__(self,file_dic):
        super(Parser275, self).__init__(file_dic)


    def get_currency(self,currency_str):
        for i in currency_str.split(' '):
            if not i: continue
            elif i in ['Sale', 'Currency']:continue
            else:return i

    def change_co_country_name(self,country_list):
        country_attr_list = []
        #slc add 去空格
        country_list = country_list.str.strip()        
        country_list.fillna('空值', inplace=True)
        #slc end
        for i in country_list:          
            if i in country_dic:
                country_attr = country_dic[i]['C/O']
                country_attr_list.append(country_attr)
            else:
                error_msg = '%s不存在, 请核对 CTM COO Master.xlsx'%i
                logger.error(error_msg)
                self.result_report.append(['ERROR', error_msg])
                self.has_error = True
                #country_attr_list.append('')
                #slc change
                country_attr_list.append(i)
        else:
            return country_attr_list

    def collect_template_value(self,data,real_data_df_row_index,currency_value):
        extract_columns = ['Material No.', 'Qty', 'C/O', 'Amount']
        df = data.loc[real_data_df_row_index, extract_columns]
        total_amount = df['Amount'].sum()
        total_qty = df['Qty'].sum()
        df['C/O'] = self.change_co_country_name(df['C/O'])
        
        extraced_df = df.groupby(['Material No.', 'C/O'], as_index=False, sort=False).sum()
        extraced_df.rename(columns={'Material No.': 'Part Number',
                                    'C/O': '进口原产国/出口最终目的国',
                                    'Qty': 'QTY',
                                    'Amount': 'AMOUNT'}, inplace=True)

        extraced_df['CURRENCY'] = currency_value

        return extraced_df,total_amount,total_qty


    def collect_delivery_value(self,data,real_data_df_row_index):
        delivery_columns = ['Deliver No.','Qty']
        df = data.loc[real_data_df_row_index,delivery_columns]
        extracted_df = df.groupby(['Deliver No.'],as_index=False,sort=False).sum()
        extracted_df.rename(columns={'Deliver No.': 'Delivery',
                                    'Qty': 'Qty'}, inplace=True)

        extracted_df['Invoice'] = self.invoiceNr
        data_str = self.dateStr.split('.')[0]
        day = datetime.datetime.strptime(data_str, '%Y%m%d %H%M')
        formated_date = datetime.datetime.strftime(day, '%Y-%m-%d %H:%M')

        extracted_df['Mail Received'] = formated_date
        return extracted_df


    def extract_info(self):
        self.standard_column = ['Pallet','Deliver No.','Material No.','Description','Qty','Batch No.','HSN Code','Tarrif Code','C/O','Unit Price','Amount']
        data = pd.read_excel(self.excel_path)
        
        rows = data.shape[0]
        real_column_index = 0
        currency_value = ''
        total_units = 0
        total_due = 0
        for i in range(rows):
            row_value_list = data.loc[i].to_list()
            lower_row_value = [str(i).lower() for i in row_value_list]
            if not currency_value:
                first_value = row_value_list[0]
                if str(first_value).startswith('Sale Currency'):
                    currency_value = self.get_currency(first_value)

            if 'Pallet'.strip().lower() in lower_row_value:
                real_column_index = i + 1
                original_data_column = data.loc[i].values
                formated_data_columns = self.check_column_name(original_data_column, self.standard_column)
                data.columns = formated_data_columns
                # data.columns = data.loc[i].values
                break
        real_data_df_row_index = []
        for j in range(real_column_index,rows):
            #slc change 就下面1行
            if not any([pd.isnull(data.loc[j, self.standard_column[0]])]):
            #if not any([pd.isnull(data.loc[j, c]) for c in self.standard_column]):
                real_data_df_row_index.append(j)

            else:
                data_list = [i.strip() if isinstance(i,str) else i for i in data.loc[j].to_list()]
                if 'Total Units' in data_list:
                    total_units_index = data_list.index('Total Units')

                    total_units = data_list[total_units_index+3]
                    total_due_index = data_list.index('Total Due')
                    total_due = data_list[total_due_index]
                    break


        delivery_df = self.collect_delivery_value(data,real_data_df_row_index)
        extraced_df,total_amount,total_qty = self.collect_template_value(data,real_data_df_row_index,currency_value)
        #return extraced_df,delivery_df,total_amount,total_qty
        #xul12 add
        self.df_excel = extraced_df
        self.upn_key ='Part Number'
        self.country_code_value ='进口原产国/出口最终目的国'
        result_waring_report_df = self.collect_pg_and_excle_mapping_upn_check_country_code(self.excel_path,self.df_excel,self.upn_key,self.country_code_value)
        self.write_result_waring_report_df_to_pg_logger(result_waring_report_df)
        extraced_df['进口原产国/出口最终目的国'] = extraced_df['进口原产国/出口最终目的国'].replace('空值', '')

        if total_units != total_qty:
            raise Exception('Error! please check quantity %s, %s'%(total_units,total_qty))
        #xul12 end
        #xul12 changed
        return extraced_df,delivery_df,total_amount,total_qty
        #xul12 end


class Parser10(CommonParser):
    def __init__(self,file_dic):
        super(Parser10, self).__init__(file_dic)

    def collect_template_value(self,data):
        extract_columns = ['Material Number', 'Currency of sales', 'Order Qty', 'Country of Mfg', 'Net Extended Price']
        df = data.loc[:, extract_columns]
        currency_str = df['Currency of sales'].iloc[0]

        total_amount = df['Net Extended Price'].sum()
        total_qty = df['Order Qty'].sum()
        #xul12 add
        df['Country of Mfg'].fillna('空值', inplace=True)
        #xul12 end
        extracted_df = df.groupby(['Material Number', 'Country of Mfg'], as_index=False, sort=False).sum()


        extracted_df.rename(columns={'Material Number': 'Part Number',
                                     'Country of Mfg': '进口原产国/出口最终目的国',
                                     'Order Qty': 'QTY',
                                     'Net Extended Price': 'AMOUNT',
                                     }, inplace=True)
        extracted_df['CURRENCY'] = currency_str


        return extracted_df,total_amount,total_qty

    def collect_delivery_value(self,data):

        delivery_columns = ['Delivery Number', 'Order Qty']
        df = data.loc[:,delivery_columns]
        extracted_df = df.groupby(['Delivery Number'], as_index=False, sort=False).sum()
        extracted_df.rename(columns={'Delivery Number': 'Delivery',
                                     'Order Qty': 'Qty'}, inplace=True)

        extracted_df['Invoice'] = self.invoiceNr
        data_str = self.dateStr.split('.')[0]
        day = datetime.datetime.strptime(data_str, '%Y%m%d %H%M')
        formated_date = datetime.datetime.strftime(day, '%Y-%m-%d %H:%M')

        extracted_df['Mail Received'] = formated_date
        return extracted_df


    def extract_info(self):
        self.standard_column = [
            'Sales #STO #S/A #', 'Delivery Number', 'Invoice Number',
       'Date of Invoice', 'Currency of sales', 'Supply Plant', 'Acct #',
       'Line #', 'Material Number', 'Batch Number', 'Order Qty',
       'Country of Mfg', 'UOM', 'Expiration Date', 'Commodity Code', 'Weight',
       'UO Wt', 'Account Unit Price', 'Ref Doc, PO #, Req Track #',
       'Net Extended Price', 'Sales Organization', 'Old Material Number',
       'SAP Description', 'Descr. DG profile', 'Temp. conditions',
       'Storage conditions', 'Date of Manuf.']


        data = pd.read_excel(self.excel_path,engine='openpyxl')
        data.columns = self.standard_column

        delivery_df = self.collect_delivery_value(data)

        extracted_df,total_amount,total_qty = self.collect_template_value(data)

        #return extracted_df,delivery_df,total_amount,total_qty
        #xul12 add
        self.df_excel = extracted_df
        self.upn_key ='Part Number'
        self.country_code_value ='进口原产国/出口最终目的国'
        result_waring_report_df = self.collect_pg_and_excle_mapping_upn_check_country_code(self.excel_path,self.df_excel,self.upn_key,self.country_code_value)
        self.write_result_waring_report_df_to_pg_logger(result_waring_report_df)
        extracted_df['进口原产国/出口最终目的国'] = extracted_df['进口原产国/出口最终目的国'].replace('空值', '')
        #xul12 end
        #xul12 changed
        return extracted_df,delivery_df,total_amount,total_qty
        #xul12 end

class Parser875(CommonParser):
    def __init__(self,file_dic):
        super(Parser875, self).__init__(file_dic)

    def collect_template_value(self,data):
        extract_columns = ['Material Number', 'Currency of sales', 'Order Qty', 'Country of Mfg', 'Net Extended Price']
        df = data.loc[:, extract_columns]
        currency_str = df['Currency of sales'].iloc[0]


        total_amount = df['Net Extended Price'].sum()
        total_qty = df['Order Qty'].sum()
        #xul12 add
        df['Country of Mfg'].fillna('空值', inplace=True)
        #xul12 end
        extracted_df = df.groupby(['Material Number', 'Country of Mfg'], as_index=False, sort=False).sum()


        extracted_df.rename(columns={'Material Number': 'Part Number',
                                     'Country of Mfg': '进口原产国/出口最终目的国',
                                     'Order Qty': 'QTY',
                                     'Net Extended Price': 'AMOUNT',
                                     }, inplace=True)
        extracted_df['CURRENCY'] = currency_str


        return extracted_df,total_amount,total_qty

    def collect_delivery_value(self,data):

        delivery_columns = ['Delivery Number', 'Order Qty']
        df = data.loc[:,delivery_columns]
        extracted_df = df.groupby(['Delivery Number'], as_index=False, sort=False).sum()
        extracted_df.rename(columns={'Delivery Number': 'Delivery',
                                     'Order Qty': 'Qty'}, inplace=True)

        extracted_df['Invoice'] = self.invoiceNr
        data_str = self.dateStr.split('.')[0]
        day = datetime.datetime.strptime(data_str, '%Y%m%d %H%M')
        formated_date = datetime.datetime.strftime(day, '%Y-%m-%d %H:%M')

        extracted_df['Mail Received'] = formated_date
        return extracted_df


    def extract_info(self):
        self.standard_column = [
            'Sales #STO #S/A #', 'Delivery Number', 'Invoice Number',
       'Date of Invoice', 'Currency of sales', 'Supply Plant', 'Acct #',
       'Line #', 'Material Number', 'Batch Number',
       'Country of Mfg', 'UOM', 'Expiration Date', 'Commodity Code', 'Weight',
       'UO Wt', 'Account Unit Price', 'Ref Doc, PO #, Req Track #',
        'Sales Organization', 'Old Material Number',
       'SAP Description', 'Date of Manuf.','Order Qty',
        'Net Extended Price']

        data = pd.read_excel(self.excel_path,engine='openpyxl',sheet_name='Sheet1')

        data.columns = self.standard_column

        delivery_df = self.collect_delivery_value(data)

        extracted_df,total_amount,total_qty = self.collect_template_value(data)

        #return extracted_df,delivery_df,total_amount,total_qty
        #xul12 add
        extracted_df = extracted_df[extracted_df["QTY"] > 0]
        
        self.df_excel = extracted_df
        #print(self.df_excel)
        self.upn_key ='Part Number'
        self.country_code_value ='进口原产国/出口最终目的国'
        result_waring_report_df = self.collect_pg_and_excle_mapping_upn_check_country_code(self.excel_path,self.df_excel,self.upn_key,self.country_code_value)
        self.write_result_waring_report_df_to_pg_logger(result_waring_report_df)
        #xul12 end
        extracted_df['进口原产国/出口最终目的国'] = extracted_df['进口原产国/出口最终目的国'].replace('空值', '')
        #xul12 changed        
        return extracted_df,delivery_df,total_amount,total_qty
        #xul12 end


class Parser333(CommonParser):
    def __init__(self,file_dic):
        super(Parser333, self).__init__(file_dic)

    def collect_delivery_value(self,data, real_data_df_row_index):
        delivery_columns = ['Delivery','Qty']
        df = data.loc[real_data_df_row_index,delivery_columns]
        extracted_df = df.groupby(['Delivery'],as_index=False,sort=False).sum()

        extracted_df['Invoice'] = self.invoiceNr
        data_str = self.dateStr.split('.')[0]
        day = datetime.datetime.strptime(data_str, '%Y%m%d %H%M')
        formated_date = datetime.datetime.strftime(day, '%Y-%m-%d %H:%M')

        extracted_df['Mail Received'] = formated_date
        return extracted_df

    def collect_template_value(self,data, real_data_df_row_index, currency_value):
        extract_columns = ['Material Number', 'Qty', 'C/O', 'Extended Price']
        df = data.loc[real_data_df_row_index, extract_columns]
        total_amount = df['Extended Price'].sum()
        total_qty = df['Qty'].sum()
        #xul12 add
        df['C/O'].fillna('空值', inplace=True)
        #xul12 end
        extraced_df = df.groupby(['Material Number', 'C/O'], as_index=False, sort=False).sum()

        extraced_df.rename(columns={'Material Number': 'Part Number',
                                    'C/O': '进口原产国/出口最终目的国',
                                    'Qty': 'QTY',
                                    'Extended Price': 'AMOUNT'}, inplace=True)

        extraced_df['CURRENCY'] = currency_value

        return extraced_df,total_amount,total_qty

    def extract_info(self):
        self.standard_column = ['Delivery','Material Number','Description','Qty','Tariff Code','C/O','Unit Price','Extended Price']
        data = pd.read_excel(self.excel_path)
        rows = data.shape[0]
        real_colmn_index = 0
        currency_abbr = ''
        for i in range(rows):
            row_value = data.loc[i].to_list()
            lower_row_value = [str(i).lower() for i in row_value]
            if 'Currency' in row_value:
                cindex = row_value.index('Currency')
                currency_abbr = data.loc[i + 1].to_list()[cindex]

            if 'Delivery'.strip().lower() in lower_row_value:

                real_colmn_index = i + 1
                original_data_column = data.loc[i].values
                formated_data_columns = self.check_column_name(original_data_column, self.standard_column)
                data.columns = formated_data_columns


                # data.columns = data.loc[i].values
                break

        real_data_df_row_index = []
        for j in range(real_colmn_index,rows):
            #slc change 就下面1行
            if not any([pd.isnull(data.loc[j, self.standard_column[0]])]):
            #if not any([pd.isnull(data.loc[j, c]) for c in self.standard_column]):
                real_data_df_row_index.append(j)
            else:break

        delivery_df = self.collect_delivery_value(data, real_data_df_row_index)

        extraced_df,total_amount,total_qty = self.collect_template_value(data, real_data_df_row_index, currency_abbr)

        #return extraced_df, delivery_df,total_amount,total_qty
        #xul12 add
        self.df_excel = extraced_df
        self.upn_key ='Part Number'
        self.country_code_value ='进口原产国/出口最终目的国'
        result_waring_report_df = self.collect_pg_and_excle_mapping_upn_check_country_code(self.excel_path,self.df_excel,self.upn_key,self.country_code_value)
        self.write_result_waring_report_df_to_pg_logger(result_waring_report_df)
        extraced_df['进口原产国/出口最终目的国'] = extraced_df['进口原产国/出口最终目的国'].replace('空值', '')
        #xul12 end
        #xul12 changed
        return extraced_df, delivery_df,total_amount,total_qty,result_waring_report_df
        #xul12 end

class Parser222(CommonParser):


    def __init__(self,file_dic):
        super(Parser222, self).__init__(file_dic)
        self.standard_column = ['PO Number','Delivery note','Material Number','Description','Qty','Tariff Code','C/O','Unit Price','Extended Price']

    def collect_delivery_value(self,data, real_data_df_row_index):
        delivery_columns = ['Delivery note','Qty']
        df = data.loc[real_data_df_row_index,delivery_columns]
        #print(df)
        extracted_df = df.groupby(['Delivery note'], as_index=False, sort=False).sum()

        extracted_df['Invoice'] = self.invoiceNr
        data_str = self.dateStr.split('.')[0]
        day = datetime.datetime.strptime(data_str, '%Y%m%d %H%M')
        formated_date = datetime.datetime.strftime(day, '%Y-%m-%d %H:%M')

        extracted_df['Mail Received'] = formated_date
        extracted_df.rename(columns={'Delivery note': 'Delivery'}, inplace=True)
        return extracted_df

    def collect_template_value(self,data, real_data_df_row_index, currency_value):

        extract_columns = ['Material Number', 'Qty', 'C/O', 'Extended Price']
        df = data.loc[real_data_df_row_index, extract_columns]
        #print(df)


        total_amount = df['Extended Price'].sum()
        total_qty = df['Qty'].sum()
        #xul12add
        df['C/O'].fillna('空值', inplace=True)
        #xul12 end
       
        extraced_df = df.groupby(['Material Number', 'C/O'], as_index=False, sort=False).sum()

        extraced_df.rename(columns={'Material Number': 'Part Number',
                                    'C/O': '进口原产国/出口最终目的国',
                                    'Qty': 'QTY',
                                    'Extended Price': 'AMOUNT'}, inplace=True)

        extraced_df['CURRENCY'] = currency_value

        return extraced_df,total_amount,total_qty


    def extract_info(self):

        data = pd.read_excel(self.excel_path)

        rows = data.shape[0]

        real_colmn_index = 0
        currency_abbr = ''
        for i in range(rows):
            row_value = data.loc[i].to_list()
            #print(row_value)
            lower_row_value = [str(i).lower() for i in row_value]
            if 'Currency' in row_value:
                cindex = row_value.index('Currency')
                currency_abbr = data.loc[i + 1].to_list()[cindex]
                if currency_abbr == 'US Dollars':
                    currency_abbr = 'USD'


            if 'PO Number'.strip().lower() in lower_row_value:
                real_colmn_index = i + 1
                original_data_column = data.loc[i].values
                formated_data_columns = self.check_column_name(original_data_column,self.standard_column)
                data.columns = formated_data_columns
                break

        real_data_df_row_index = []
        for j in range(real_colmn_index,rows):
            #slc change 就下面1行
            if not any([pd.isnull(data.loc[j, self.standard_column[0]])]):
            #if not any([pd.isnull(data.loc[j, c]) for c in self.standard_column]):
                real_data_df_row_index.append(j)
            else:
                break

        #print(data)
        delivery_df = self.collect_delivery_value(data, real_data_df_row_index)
        #print(delivery_df)
        extraced_df,total_amount,total_qty = self.collect_template_value(data, real_data_df_row_index, currency_abbr)

        #return extraced_df, delivery_df,total_amount,total_qty
        #xul12 add
        self.df_excel = extraced_df
        self.upn_key ='Part Number'
        self.country_code_value ='进口原产国/出口最终目的国'
        result_waring_report_df = self.collect_pg_and_excle_mapping_upn_check_country_code(self.excel_path,self.df_excel,self.upn_key,self.country_code_value)
        self.write_result_waring_report_df_to_pg_logger(result_waring_report_df)
        extraced_df['进口原产国/出口最终目的国'] = extraced_df['进口原产国/出口最终目的国'].replace('空值', '')
        #xul12 changed
        return extraced_df, delivery_df,total_amount,total_qty
        #xul12 end


class Parser222_bak(CommonParser):


    def __init__(self,file_dic):
        super(Parser222_bak, self).__init__(file_dic)

    def collect_delivery_value(self,data, real_data_df_row_index,delivery_number_value):
        delivery_columns = ['Qty']
        df = data.loc[real_data_df_row_index,delivery_columns]
        total_qty = df['Qty'].sum()

        extracted_df = pd.DataFrame()
        extracted_df['Delivery'] = [delivery_number_value, ]
        extracted_df['Qty'] = [total_qty,]
        extracted_df['Invoice'] = [self.invoiceNr,]
        data_str = self.dateStr.split('.')[0]
        day = datetime.datetime.strptime(data_str, '%Y%m%d %H%M')
        formated_date = datetime.datetime.strftime(day, '%Y-%m-%d %H:%M')

        extracted_df['Mail Received'] = [formated_date,]
        return extracted_df

    def collect_template_value(self,data, real_data_df_row_index, currency_value):
        extract_columns = ['Material Number', 'Qty', 'C/O', 'Extended Price']
        df = data.loc[real_data_df_row_index, extract_columns]
        total_amount = df['Extended Price'].sum()
        total_qty = df['Qty'].sum()

        extraced_df = df.groupby(['Material Number', 'C/O'], as_index=False, sort=False).sum()

        extraced_df.rename(columns={'Material Number': 'Part Number',
                                    'C/O': '进口原产国/出口最终目的国',
                                    'Qty': 'QTY',
                                    'Extended Price': 'AMOUNT'}, inplace=True)

        extraced_df['CURRENCY'] = currency_value

        return extraced_df,total_amount,total_qty

    def extract_info(self):
        '''
        extract delivery number???
        '''

        column_222 = ['PO Number','Material Number','Description','Qty','Tariff Code','C/O','Unit Price','Extended Price']
        data = pd.read_excel(self.excel_path)
        rows = data.shape[0]
        real_colmn_index = 0
        currency_abbr = ''
        for i in range(rows):
            row_value = data.loc[i].to_list()
            if 'Currency' in row_value:
                cindex = row_value.index('Currency')
                currency_abbr = data.loc[i + 1].to_list()[cindex]
                if currency_abbr == 'US Dollars':
                    currency_abbr = 'USD'


            if 'PO Number' in row_value:
                real_colmn_index = i + 1
                data.columns = data.loc[i].values
                break

        real_data_df_row_index = []
        next_row = 0
        for j in range(real_colmn_index,rows):

            if not any([pd.isnull(data.loc[j, c]) for c in column_222]):
                real_data_df_row_index.append(j)
            else:
                next_row = j
                break

        for d in range(next_row,rows):
            row_value = data.loc[d].to_list()
            if 'Delivery Number' in row_value:
                dindex = row_value.index('Delivery Number')
                delivery_number_value = row_value[dindex+1]
                break
        else:
            raise Exception('can not find the delivery number in %s'%self.excel_path)


        delivery_df = self.collect_delivery_value(data, real_data_df_row_index,delivery_number_value)

        extraced_df,total_amount,total_qty = self.collect_template_value(data, real_data_df_row_index, currency_abbr)

        return extraced_df, delivery_df,total_amount,total_qty


class Parser275b(CommonParser):
    def __init__(self,file_dic):
        super(Parser275b,self).__init__(file_dic)


    def collect_delivery_value(self,data, real_data_df_row_index):
        delivery_columns = ['Delivery Number','Qty']
        df = data.loc[real_data_df_row_index,delivery_columns]
        extracted_df = df.groupby(['Delivery Number'],as_index=False,sort=False).sum()

        extracted_df['Invoice'] = self.invoiceNr

        data_str = self.dateStr.split('.')[0]
        day = datetime.datetime.strptime(data_str, '%Y%m%d %H%M')

        formated_date = datetime.datetime.strftime(day, '%Y-%m-%d %H:%M')
        extracted_df['Mail Received'] = formated_date
        extracted_df.rename(columns={'Delivery Number': 'Delivery',
                                     'Qty': 'QTY',
                                     'Invoice': 'Invoice',
                                     'Mail Received':'Mail Received'}, inplace=True)
        return extracted_df


    def collect_template_value(self,data, real_data_df_row_index, currency_value):
        extract_columns = ['Material Number', 'Qty', 'C/O', 'Extended Price']
        df = data.loc[real_data_df_row_index, extract_columns]
        total_amount = df['Extended Price'].sum()
        total_qty = df['Qty'].sum()
        #xul12 add
        df['C/O'].fillna('空值', inplace=True)
        #xul12 end

        extraced_df = df.groupby(['Material Number', 'C/O'], as_index=False, sort=False).sum()

        extraced_df.rename(columns={'Material Number': 'Part Number',
                                    'C/O': '进口原产国/出口最终目的国',
                                    'Qty': 'QTY',
                                    'Extended Price': 'AMOUNT'}, inplace=True)

        extraced_df['CURRENCY'] = currency_value

        return extraced_df,total_amount,total_qty


    def extract_info(self):
        self.standard_column = ['Delivery Number','Material Number','Description','Qty','Tariff Code','C/O','Unit Price','Extended Price']
        data = pd.read_excel(self.excel_path)
        rows = data.shape[0]
        real_column_index = 0
        currency_abbr = ''
        for i in range(rows):
            row_value = data.loc[i].to_list()
            lower_row_value = [str(i).lower() for i in row_value]
            if 'Currency' in row_value:
                cindex  = row_value.index('Currency')
                currency_abbr = data.loc[i+1].to_list()[cindex]

            if 'Delivery Number'.strip().lower() in lower_row_value:
                real_column_index = i+1
                original_data_column = data.loc[i].values
                formated_data_columns = self.check_column_name(original_data_column, self.standard_column)
                data.columns = formated_data_columns
                # data.columns = data.loc[i].values
                break

        real_data_df_row_index = []
        for j in range(real_column_index,rows):
            #slc change 就下面1行
            if not any([pd.isnull(data.loc[j, self.standard_column[0]])]):
            #if not any([pd.isnull(data.loc[j,c]) for c in self.standard_column]):
                real_data_df_row_index.append(j)
            else:break

        delivery_df = self.collect_delivery_value(data,real_data_df_row_index)

        extraced_df,total_amount,total_qty = self.collect_template_value(data, real_data_df_row_index, currency_abbr)

        #return extraced_df,delivery_df,total_amount,total_qty

        #xul12 add
        self.df_excel = extraced_df
        self.upn_key ='Part Number'
        self.country_code_value ='进口原产国/出口最终目的国'
        result_waring_report_df = self.collect_pg_and_excle_mapping_upn_check_country_code(self.excel_path,self.df_excel,self.upn_key,self.country_code_value)
        self.write_result_waring_report_df_to_pg_logger(result_waring_report_df)
        extraced_df['进口原产国/出口最终目的国'] = extraced_df['进口原产国/出口最终目的国'].replace('空值', '')
        #xul12 end

        #xul12 changed
        return extraced_df,delivery_df,total_amount,total_qty
        #xul12 end



class Parser10_bak20220527(CommonParser):
    def __init__(self,file_dic):
        super(Parser10, self).__init__(file_dic)

    def collect_template_value(self,data):
        extract_columns = ['Material Number', 'Currency of sales', 'Order Qty', 'Country of Mfg', 'Net Extended Price']
        df = data.loc[:, extract_columns]
        currency_str = df['Currency of sales'].iloc[0]

        total_amount = df['Net Extended Price'].sum()
        total_qty = df['Order Qty'].sum()

        extracted_df = df.groupby(['Material Number', 'Country of Mfg'], as_index=False, sort=False).sum()


        extracted_df.rename(columns={'Material Number': 'Part Number',
                                     'Country of Mfg': '进口原产国/出口最终目的国',
                                     'Order Qty': 'QTY',
                                     'Net Extended Price': 'AMOUNT',
                                     }, inplace=True)
        extracted_df['CURRENCY'] = currency_str


        return extracted_df,total_amount,total_qty

    def collect_delivery_value(self,data):

        delivery_columns = ['Delivery Number', 'Order Qty']
        df = data.loc[:,delivery_columns]
        extracted_df = df.groupby(['Delivery Number'], as_index=False, sort=False).sum()
        extracted_df.rename(columns={'Delivery Number': 'Delivery',
                                     'Order Qty': 'Qty'}, inplace=True)

        extracted_df['Invoice'] = self.invoiceNr
        data_str = self.dateStr.split('.')[0]
        day = datetime.datetime.strptime(data_str, '%Y%m%d %H%M')
        formated_date = datetime.datetime.strftime(day, '%Y-%m-%d %H:%M')

        extracted_df['Mail Received'] = formated_date
        return extracted_df


    def extract_info(self):
        self.standard_column = ['Sales #STO #S/A #', 'Delivery Number', 'Invoice Number',
       'Date of Invoice', 'Currency of sales', 'Supply Plant', 'Acct #',
       'Line #', 'Material Number', 'Batch Number', 'Order Qty',
       'Country of Mfg', 'UOM', 'Expiration Date', 'Commodity Code', 'Weight',
       'UO Wt', 'Account Unit Price', 'Ref Doc, PO #, Req Track #',
       'Net Extended Price', 'Sales Organization', 'Old Material Number',
       'SAP Description', 'Descr. DG profile', 'Temp. conditions',
       'Storage conditions', 'Date of Manuf.']

        data = pd.read_excel(self.excel_path,engine='openpyxl')


        data.columns = self.standard_column

        delivery_df = self.collect_delivery_value(data)

        extracted_df,total_amount,total_qty = self.collect_template_value(data)

        return extracted_df,delivery_df,total_amount,total_qty




if __name__ == '__main__':
    # excel_path = r'C:\Users\shif\Desktop\PROJECT\CTM\525_482255_500_20201119 0455.xlsx'
    file_dic = {'invoiceNr': '75291175', 'grossWeight': '1000', 'dateStr': '20201120 0138.XLSX', 'filepath': 'C:\\Users\\shif\\Desktop\\PROJECT\\CTM\\10_75291175_1000_20201120 0138.XLSX', 'venderNr': '10'}
    p10 = Parser10(file_dic)
    p10.extract_info()

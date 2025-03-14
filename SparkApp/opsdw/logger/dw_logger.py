import os
import sys
import logging
from time import strftime
# 输出日志路径
PATH = os.path.dirname(os.path.dirname(__file__)) + '/logs/'
# 设置日志格式#和时间格式
FMT = '%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s: %(message)s'
DATEFMT = '%Y-%m-%d %H:%M:%S'

class DWLogger(object):
    def __init__(self):
        self.logger = logging.getLogger("OPSDW")
        self.formatter = logging.Formatter(fmt=FMT, datefmt=DATEFMT)
        self.log_filename = '{0}{1}.log'.format(PATH, strftime("%Y-%m-%d-%H"))
        if not self.logger.handlers:
            self.logger.addHandler(self.set_file_handler(self.log_filename))
            self.logger.addHandler(self.set_console_handler())
        # 设置日志的默认级别
        self.logger.setLevel(logging.DEBUG)

    # 输出到文件handler的函数定义
    def set_file_handler(self, filename):
        filehandler = logging.FileHandler(filename, encoding="utf-8")
        filehandler.setFormatter(self.formatter)
        filehandler.setLevel(logging.DEBUG)
        return filehandler

    # 输出到控制台handler的函数定义
    def set_console_handler(self):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(self.formatter)
        console_handler.setLevel(logging.WARNING)
        return console_handler
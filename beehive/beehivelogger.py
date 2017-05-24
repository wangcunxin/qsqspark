# -*- coding:utf8 -*-
__author__ = 'wangcx'

import logging
# 创建一个logger
module_name = 'beehive'
logger = logging.getLogger(module_name)
logger.setLevel(logging.DEBUG)
# 创建一个handler，用于写入日志文件
log_file = '/Users/wangcunxin/temp/%s.log' % module_name
fh = logging.FileHandler(log_file)
fh.setLevel(logging.DEBUG)
# 再创建一个handler，用于输出到控制台
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# 定义handler的输出格式
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# 给logger添加handler
logger.addHandler(fh)
logger.addHandler(ch)

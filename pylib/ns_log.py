# /usr/bin/python
# -*- coding:UTF-8 -*-

# @Time    : 2018/2/24
# @Author  : 吴昌文
# @Site    :
# @File    : ns_db.py
# @Software: PyCharm
# @Version : v0.1
# @Description:写程序日志文件


import os
import time
import logging
import ConfigParser

log_default_root = '__default__'


class Logger:
    def __init__(self, prefix, cfg_file, clevel=logging.DEBUG, Flevel=logging.DEBUG):
        global log_default_root
        cf = ConfigParser.ConfigParser()
        cf.read(cfg_file)
        config = {}

        try:
            config['file_path'] = cf.get(prefix, 'file_path')
        except Exception as e:
            config['file_path'] = cf.get(log_default_root, 'file_path')
        try:
            config['file_name'] = cf.get(prefix, 'file_name')
        except Exception as e:
            config['file_name'] = cf.get(log_default_root, 'file_name')

        current = time.strftime("%Y%m%d", time.localtime(time.time()))
        newdir = os.path.join(config['file_path'], current)
        if not os.access(newdir, os.X_OK):
            os.mkdir(newdir)
        path = os.path.join(newdir, config['file_name'])
        self.logger = logging.getLogger(path)
        self.logger.setLevel(logging.DEBUG)
        fmt = logging.Formatter(
            '%(asctime)s <%(levelname)-2s><%(name)s:%(process)d:%(threadName)s><%(filename)s:%(lineno)d>%(message)s',
            '%Y-%m-%d %H:%M:%S')
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        sh.setLevel(clevel)
        fh = logging.FileHandler(path)
        fh.setFormatter(fmt)
        fh.setLevel(Flevel)
        self.logger.addHandler(sh)
        self.logger.addHandler(fh)

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def war(self, message):
        self.logger.warn(message)

    def error(self, message):
        self.logger.error(message)

    def cri(self, message):
        self.logger.critical(message)

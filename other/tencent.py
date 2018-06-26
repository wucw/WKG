#!/usr/bin/python
# -*- coding:utf-8 -*-

import os
import sys
import json
import time
import logging
import requests
import ConfigParser
from pylib import ns_db

reload(sys)
sys.setdefaultencoding("UTF-8")
SELF = os.path.splitext(os.path.basename(sys.argv[0]))[0]
AUTH = 'wuchangwen'
VER = '1.0'

logger = None
config = {}
con = None
cur = None
db_file = '../etc/db.conf'


class Logger:
    def __init__(self, path, clevel=logging.DEBUG, Flevel=logging.DEBUG):
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


def _init_conf():
    global config
    config = {}
    cf = ConfigParser.ConfigParser()
    try:
        cf.read(db_file)
        # str config
        for opt in ['db_host', 'db_user', 'db_pass', 'db_dbname', 'charset']:
            config[opt] = cf.get(SELF, opt)
            logger.info('%s.%s [%s]' % (SELF, opt, config[opt]))
        # int config
        for opt in ['db_port']:
            config[opt] = cf.getint(SELF, opt)
            logger.info('%s.%s [%s]' % (SELF, opt, config[opt]))
    except Exception as e:
        logger.error('cfg [%s] get FAIL [%s]' % (db_file, e))


def get_dates():
    dates = ["201607", "201608", "201609", "201610", "201611", "201612", "201701", "201702", "201703", "201704",
             "201705", "201706", "201707", "201708", "201709", "201710", "201711", "201712", "201801", "201802",
             "201803"]
    return dates


def get_data(date):
    header = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Content-Length': '86',
        'Content-Type': 'application/json;charset=UTF-8',
        'Host': 'tbi.tencent.com',
        'Origin': 'http://tbi.tencent.com',
        'Cookie': 'pgv_pvi=2225501184; pt2gguin=o1067974581; ptcz=a8521bbb6c09c1faabc433f0a4d4d5d8cd1973a3b16a8dd9951e824da35728a1; _ga=GA1.2.960602822.1521540868; _csrf=vNQqbCEFoWABdvWPt_MlBfkT; pgv_si=s1515370496; uin=o1067974581; skey=@9phwJtBLB; ptisp=ctc; p_uin=o1067974581; pt4_token=RXchiebBHuQyNUy6Lu5V3HHaD2VU6FQaSB*K*nWMzwQ_; p_skey=VW1vqEnmPdg798L3pHSUEfZARSGdYoWZs-NTL8J3v0o_; XSRF-TOKEN=Q2a1hmFO--Za-tyivnVc5gtpCU_jLg8ziviU',
        'Referer': 'http://tbi.tencent.com/index?word=%E7%8E%8B%E8%80%85%E8%8D%A3%E8%80%80&date=1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36',
        'X-XSRF-TOKEN': 'Q2a1hmFO--Za-tyivnVc5gtpCU_jLg8ziviU'
    }
    for n in range(0, 6):
        time.sleep(1)
        playload = {'tagId': "", 'tag': "王者荣耀", 'profile': n, 'type': '0', 'start': date, 'end': date}
        url_ = 'http://tbi.tencent.com/tbi/queryTagProfile'
        response = requests.post(url=url_, headers=header, data=json.dumps(playload))
        data_info = response.content
        attrs = ['sProfileValue', 'lProfilePV', 'lTotalPV', 'dProfileRate', 'date']
        if n == 0:
            db_table = "app_wz_age"
            datas = paserdata(data_info, date)
            sava_to_db(datas, db_table, attrs)
        if n == 1:
            db_table = "app_wz_sex"
            datas = paserdata(data_info, date)
            sava_to_db(datas, db_table, attrs)
        if n == 2:
            db_table = "app_wz_pro"
            datas = paserdata(data_info, date)
            sava_to_db(datas, db_table, attrs)
        if n == 3:
            db_table = "app_wz_city"
            datas = paserdata(data_info, date)
            sava_to_db(datas, db_table, attrs)
        if n == 4:
            db_table = "app_wz_con"
            datas = paserdata(data_info, date)
            sava_to_db(datas, db_table, attrs)
        if n == 5:
            db_table = "app_wz_edu"
            datas = paserdata(data_info, date)
            sava_to_db(datas, db_table, attrs)


def paserdata(data_info, date):
    datalst = []
    json_data = json.loads(data_info)
    datastr = json_data["data"]["value"]
    for datas in datastr:
        data_lst = []
        sProfileValue = datas["sProfileValue"]
        data_lst.append(sProfileValue)
        lProfilePV = datas["lProfilePV"]
        data_lst.append(lProfilePV)
        lTotalPV = datas["lTotalPV"]
        data_lst.append(lTotalPV)
        dProfileRate = datas["dProfileRate"]
        data_lst.append(dProfileRate)
        data_lst.append(date)
        datalst.append(data_lst)
    logger.info('The datas of Android to parser is successful')
    return datalst


def sava_to_db(datas, db_table, attrs):
    db = None
    try:
        db = ns_db.mysqldb(logger, config['db_host'], config['db_user'], config['db_pass'], config['db_dbname'],
                           config['charset'])
        logger.info('Connection the database of %s is successful.' % config['db_dbname'])
    except Exception as e:
        logging.error('Connection the database of %s is Filed')
    try:
        db.insert_many(logger, db_table, attrs, datas)
        logger.info('Insert data to the table of [%s] is successful,It\'s size is [%s]' % (db_table, len(datas)))
    except Exception as e:
        logging.error('Insert data to the table of [%s] is Filed' % db_table)
    if db is not None:
        db.close()


def task():
    dates = get_dates()
    for date in dates:
        get_data(date)
        time.sleep(1)


def log_file(SELF):
    path = '../log/'
    current = time.strftime("%Y%m%d", time.localtime(time.time()))
    newdir = os.path.join(path, current)
    if not os.access(newdir, os.X_OK):
        os.mkdir(newdir)
    filename = SELF + '.log'
    newfile = newdir + '/' + filename
    return newfile


def main():
    global logger
    global config
    log_files = log_file(SELF)
    logger = Logger(log_files, logging.INFO, logging.INFO)
    logger.info("START PROCESS [%s], auth:[%s], ver:[%s]" % (SELF, AUTH, VER))
    _init_conf()
    try:
        task()
    except Exception as e:
        logger.error('This is an error,The detailed information is [%s]' % e)
    logger.info('END PROCESS [%s], auth:[%s], ver:[%s]' % (SELF, AUTH, VER))


if __name__ == '__main__':
    main()

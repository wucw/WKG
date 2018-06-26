#!/usr/bin/python
# -*- coding:UTF-8 -*-


import os
import sys
import time
import json
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
        for opt in ['db_host', 'db_user', 'db_pass', 'db_dbname', 'charset', 'ios', 'and']:
            config[opt] = cf.get(SELF, opt)
            logger.info('%s.%s [%s]' % (SELF, opt, config[opt]))
        # int config
        for opt in ['db_port']:
            config[opt] = cf.getint(SELF, opt)
            logger.info('%s.%s [%s]' % (SELF, opt, config[opt]))
    except Exception as e:
        logger.error('cfg [%s] get FAIL [%s]' % (db_file, e))


def get_info(url):
    header = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Host': 'api.qimai.cn',
        'Origin': 'https://www.qimai.cn',
        'Referer': 'https://www.qimai.cn/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.89 Safari/537.36 '
    }
    # proxies = ns_ip_proxys.dailiIP()
    # proxies = proxies
    response = requests.get(url=url, headers=header)
    data_str = response.content
    return data_str


def parse_data_ios(url, datas):
    dat_lst = []
    json_data = json.loads(datas)
    strls = url.split('&')[-2]
    datetime = strls.split('=')[-1]
    datatimes = datetime.replace('-', '')
    for info_lst in json_data['rankInfo']:
        data_lst = []
        index = info_lst['index']
        # appInfo
        data_lst.append(index)
        country = info_lst['appInfo']['country']
        data_lst.append(country)
        publisher = info_lst['appInfo']['publisher']
        data_lst.append(publisher)
        appName = info_lst['appInfo']['appName']
        data_lst.append(appName)
        appId = info_lst['appInfo']['appId']
        data_lst.append(appId)
        # 总榜排名
        rank_a_ranking = info_lst['rank_a']['ranking']
        data_lst.append(rank_a_ranking)
        rank_a_change = info_lst['rank_a']['ranking']
        data_lst.append(rank_a_change)
        # 应用游戏
        rank_b_genre = info_lst['rank_b']['genre']
        data_lst.append(rank_b_genre)
        rank_b_ranking = info_lst['rank_b']['ranking']
        data_lst.append(rank_b_ranking)
        rank_b_change = info_lst['rank_b']['change']
        data_lst.append(rank_b_change)
        # 分类排名
        rank_c_genre = info_lst['rank_c']['genre']
        data_lst.append(rank_c_genre)
        rank_c_ranking = info_lst['rank_c']['ranking']
        data_lst.append(rank_c_ranking)
        rank_c_change = info_lst['rank_c']['change']
        data_lst.append(rank_c_change)
        # 最新版本
        lastReleaseTime = info_lst['lastReleaseTime']
        data_lst.append(lastReleaseTime)
        # 公司名称
        company_name = info_lst['company']['name']
        data_lst.append(company_name)
        data_lst.append(datatimes)
        dat_lst.append(data_lst)
    logger.info('The datas of iPhone to parser is successful')
    return dat_lst


def parse_data_And(url, datas):
    dat_lst = []
    json_data = json.loads(datas)
    strls = url.split('&')[-1]
    datetime = strls.split('=')[-1]
    datatimes = datetime.replace('-', '')
    for info_lst in json_data['rankInfo']:
        data_lst = []
        appName = info_lst['appInfo']['appName']
        data_lst.append(appName)
        ranking = info_lst['rankInfo']['ranking']
        data_lst.append(ranking)
        genre = info_lst['genre']
        data_lst.append(genre)
        downloadNum = info_lst['downloadNum']
        data_lst.append(downloadNum)
        releaseTime = info_lst['releaseTime']
        data_lst.append(releaseTime)
        company = info_lst['company']['name']
        data_lst.append(company)
        data_lst.append(datatimes)
        dat_lst.append(data_lst)
    logger.info('The datas of Android to parser is successful')
    return dat_lst


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


def log_file(SELF):
    path = '../log/'
    current = time.strftime("%Y%m%d", time.localtime(time.time()))
    newdir = os.path.join(path, current)
    if not os.access(newdir, os.X_OK):
        os.mkdir(newdir)
    filename = SELF + '.log'
    newfile = newdir + '/' + filename
    return newfile


def task(url):
    datas_json = get_info(url)
    phone_type = url.split('&')[-5]
    phone = phone_type.split('=')[-1]
    if phone == 'iphone':
        logger.info('Start to parser The datas of iPhone ')
        attrs = ['number', 'region', 'copyright', 'exename', 'exenumber', 'allrank', 'allchange', 'exetype',
                 'exetype_rank', 'exetype_change', 'gamename_type', 'game_rank', 'game_change',
                 'updatetime', 'company', 'date']
        data_lst = parse_data_ios(url, datas_json)
        sava_to_db(data_lst, config['ios'], attrs)
    else:
        logger.info('Start to parser The datas of Android ')
        attrs = ['exename', 'exerank', 'exetype', 'add_download_yes', 'updatetime', 'company', 'date']
        data_lst = parse_data_And(url, datas_json)
        sava_to_db(data_lst, config['and'], attrs)


def main():
    global logger
    global config
    log_files = log_file(SELF)
    logger = Logger(log_files, logging.INFO, logging.INFO)
    logger.info("START PROCESS [%s], auth:[%s], ver:[%s]" % (SELF, AUTH, VER))
    _init_conf()
    url = 'https://api.qimai.cn/rank/marketRank?analysis=dTB4GS8Odk13IABDfmABQCwxK18HC2MVUy19ClBxG0ZbCVUHOAp9WWIyDQlzEB5LAAsOGggEQwhVEWEHXF0jFQ5RAllSXQQNDlRwFwI=&market=3&category=-2&country=cn&collection=topselling_free&page=1&date=2018-01-21'
    try:
        task(url)
    except Exception as e:
        logger.error('This is an error,The detailed information is [%s]' % e)
    logger.info('END PROCESS [%s], auth:[%s], ver:[%s]' % (SELF, AUTH, VER))


if __name__ == "__main__":
    main()

#!/usr/bin/python
# -*- coding:UTF-8 -*-


import os
import sys
import time
import logging
import threading
import ConfigParser
from pylib import ns_db
from pylib import ns_log

SELF = os.path.splitext(os.path.basename(sys.argv[0]))[0]
AUTH = 'wuchangwen'
VER = '1.0'

lgr_conf_file = '../etc/logger.conf'
db_file = '../etc/db.conf'
conf_file = '../etc/' + SELF + '.conf'

global config
global logger


def _init_conf():
    global config
    config = {}
    cf = ConfigParser.ConfigParser()
    try:
        cf.read(db_file)
        # string
        for opt in ['db_host', 'db_user', 'db_pass', 'db_name', 'charset']:
            config[opt] = cf.get(SELF, opt)
            logger.info('%s.%s [%s]' % (SELF, opt, config[opt]))

        # int
        config['db_port'] = cf.getint(SELF, 'db_port')
        logger.info('%s.%s [%s]' % (SELF, 'db_port', 'db_port'))

        cf.read(conf_file)
        # int
        for opt in ['sleep_time']:
            config[opt] = cf.getint(SELF, opt)
            logger.info('%s.%s [%s]' % (SELF, opt, config[opt]))
        # boolean
        for opt in ['db_ensend', 'isdistinct', 'handle']:
            config[opt] = cf.getboolean(SELF, opt)
            logger.info('%s.%s [%s]' % (SELF, opt, config[opt]))
        # str
        for opt in ['tablename', 'attrs', 'insert_attrs', 'insert_tab']:
            config[opt] = cf.get(SELF, opt)
            logger.info('%s.%s [%s]' % (SELF, opt, config[opt]))

    except Exception as e:
        logger.error('cfg [%s] get FAIL [%s]' % (db_file, e))


def select_from_db(db):
    logger.info('Start to get datas from [%s]' % config['db_name'])
    try:
        data_lst = db.select_all(logger, config['tablename'], config['attrs'])
        return data_lst
    except Exception as e:
        logger.error("ERROR: %s" % e)


def inset2db(db, datas):
    logger.info('Start to insert datas to [%s.%s] and Data removal...' % (config['db_name'], config['insert_tab']))
    try:
        data_lst = set(datas)
        db.insertmany(logger, config['insert_tab'], config['insert_attrs'], data_lst)
        logger.info('Insert datas to [%s]' % config['insert_tab'])
    except Exception as e:
        logger.error("ERROR: %s" % e)


def dict2list(data_lst):
    datalist = []
    for datas in data_lst:
        datalst = []
        datalst.append(datas['exename'])
        datalst.append(datas['exenumber'])
        datalst.append(datas['allchange'])
        datalst.append(datas['exetype_rank'])
        datalst.append(datas['exetype_change'])
        datalst.append(datas['game_rank'])
        datalst.append(datas['game_change'])
        datalst.append(datas['company'])
        datalst.append(datas['date'])
        datalist.append(tuple(datalst))
    return datalist


def datatoparser(db):
    logger.info('Start to removal datas in [%s.%s]' % (config['db_name'], config['insert_tab']))
    try:
        db.delete(logger, config['insert_tab'], {'company': '\'-\''})
        logger.info('removal datas is  successfully')
    except Exception as e:
        logger.error("ERROR: %s" % e)


def task():
    db = None
    if config['db_ensend']:
        db = ns_db.mysqldb(logger, config['db_host'], config['db_user'], config['db_pass'], config['db_name'],
                           config['charset'])
        logger.info('Connection the database of [%s] successfully' % config['db_name'])

    if config['isdistinct']:
        data_lst = select_from_db(db)
        datas = dict2list(data_lst)
        inset2db(db, datas)

    if config['handle']:
        datatoparser(db)

    if db != None:
        db.close()
        logger.info('The database is closed!')


def main():
    global config
    global logger

    logger = ns_log.Logger(SELF, lgr_conf_file, logging.INFO, logging.INFO)
    logger.info("START PROCESS [%s], auth:[%s], ver:[%s]" % (SELF, AUTH, VER))
    _init_conf()
    sleep_time = config['sleep_time']
    t1 = threading.Thread(target=task())
    t1.setDaemon(True)
    t1.start()
    time.sleep(sleep_time)
    logger.info('END PROCESS [%s], auth:[%s], ver:[%s]' % (SELF, AUTH, VER))


if __name__ == '__main__':
    main()

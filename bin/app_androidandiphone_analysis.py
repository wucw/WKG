#!/usr/bin/python
# -*- coding:UTF-8 -*-

import numpy as np
import pandas as pd
import MySQLdb as db
import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif'] = ['simHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号


def conn2db():
    host = '127.0.0.1'
    username = 'root'
    passwd = 'root'
    database = 'appdb'
    charset = 'utf8'
    conn = db.connect(host=host, user=username, passwd=passwd, db=database, port=3306,
                      charset=charset)
    return conn


def download_change(conn):
    sql = 'SELECT add_download_yes FROM app_android WHERE exename="王者荣耀" ORDER BY date'
    res = pd.read_sql(sql, conn, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None,
                      chunksize=None)
    print res
    apprank = res['add_download_yes']
    apprank.plot()
    plt.title(u'Android — 王者荣耀下载情况')
    plt.xlabel(u'时间（天数）')
    plt.ylabel(u'数量')
    plt.savefig('../Image/download.png', dpi=400)
    plt.show()


def iphone_rank(conn):
    sql = 'SELECT game_rank FROM app_iphone WHERE exename="王者荣耀" ORDER BY date'
    res = pd.read_sql(sql, conn, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None,
                      chunksize=None)
    print res
    apprank = res['game_rank']
    apprank.plot()
    plt.title(u'iPhone — 王者荣耀排名变化情况')
    plt.xlabel(u'时间（天数）')
    plt.ylabel(u'名次')
    plt.savefig('../Image/iphone.png', dpi=400)
    plt.show()


def android(conn):
    sql='SELECT company,COUNT(*) as num FROM app_android WHERE exename<>"王者荣耀" GROUP BY company '
    res = pd.read_sql(sql, conn, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None,
                      chunksize=None)
    print res
    x = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T']
    y = res['num']
    N = 20
    ind = np.arange(N)
    width = 0.35
    plt.bar(ind, y, width, color='r', label='sum num')
    plt.xticks(ind + width / 2, x, rotation=0)
    plt.title(u'Android——游戏版权商统计图')
    plt.xlabel(u'版权商')
    plt.ylabel(u'次数')
    plt.savefig('../Image/android_com.png', dpi=400)
    plt.show()

def iphone(conn):
    sql = 'SELECT * FROM (SELECT company,COUNT(*) as num FROM app_iphone WHERE exename<>"王者荣耀" GROUP BY company ORDER BY num DESC ) s WHERE num >50'
    res = pd.read_sql(sql, conn, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None,
                      chunksize=None)
    result = res.head(25)
    print result
    x = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',
         'W', 'X', 'Y']
    y = result['num']
    N = 25
    ind = np.arange(N)
    width = 0.35
    plt.bar(ind, y, width, color='r', label='sum num')
    plt.xticks(ind + width / 2, x, rotation=0)
    plt.title(u'iPhone——游戏版权商统计图')
    plt.xlabel(u'版权商')
    plt.ylabel(u'次数')
    plt.savefig('../Image/iPhone_com.png', dpi=400)
    plt.show()


def main():
    conn = conn2db()
    download_change(conn)
    iphone_rank(conn)
    android(conn)
    iphone(conn)


if __name__ == '__main__':
    main()

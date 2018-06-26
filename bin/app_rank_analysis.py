#!/usr/bin/python
# -*- coding:utf-8 -*-

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


def app_wangzhe(conn):
    sql = 'SELECT apprank,rank_change,date FROM app_rankdata WHERE appname="王者荣耀" ORDER BY str_to_date(date,"%Y-%m-%d")'
    res = pd.read_sql(sql, conn, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None,
                      chunksize=None)
    # 王者荣耀的排名情况
    apprank = res['apprank']
    apprank.plot()
    plt.title(u'王者荣耀排名情况')
    plt.xlabel(u'时间（天数）')
    plt.ylabel(u'名次')
    plt.savefig('../Image/rank.png', dpi=400)
    plt.show()

    # 王者荣耀的排名变化情况
    apprank = res['rank_change']
    apprank.plot()
    plt.title(u'王者荣耀排名变化情况')
    plt.xlabel(u'时间（天数）')
    plt.ylabel(u'名次')
    plt.savefig('../Image/rank_change.png', dpi=400)
    plt.show()

    print res.iloc[0:50]
    print res.iloc[50:100]
    print res.iloc[220:275]
    print res.iloc[600:700]


def app_top25(conn):
    sql = 'SELECT * FROM (SELECT appid,appname,COUNT(*) as num FROM app_rankdata  WHERE apprank<= 5 GROUP BY appid ORDER BY num DESC) s WHERE num>1 AND appname <>"王者荣耀" '
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
    plt.title(u'总体app前二十五上榜统计图')
    plt.xlabel(u'app名称')
    plt.ylabel(u'次数')
    plt.savefig('../Image/apptop25.png', dpi=400)
    plt.show()


def app_top5(conn):
    sql = 'SELECT * FROM (SELECT appid,appname,COUNT(*) as num FROM (SELECT appid,appname FROM app_rankdata WHERE apprank<=5 AND appid <>"989673964" AND date like "2018%" ORDER BY str_to_date(date,"%Y-%m-%d"),apprank) s GROUP BY appid) a WHERE num>1 ORDER BY num DESC '
    res = pd.read_sql(sql, conn, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None,
                      chunksize=None)
    result = res.head(10)
    print result
    x = result['appname']
    y = result['num']
    N = 10
    ind = np.arange(N)
    width = 0.35
    plt.bar(ind, y, width, color='r', label='sum num')
    plt.xticks(ind + width / 2, x, rotation=40)
    plt.title(u'2018年app前十上榜统计图')
    plt.xlabel(u'app名称')
    plt.ylabel(u'次数')
    plt.savefig('../Image/apptop10.png', dpi=400)
    plt.show()


def main():
    conn = conn2db()
    app_wangzhe(conn)
    app_top25(conn)
    app_top5(conn)


if __name__ == '__main__':
    main()

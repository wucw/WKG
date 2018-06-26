#!/usr/bin/python
# -*- coding:UTF-8 -*-

import MySQLdb as db
from pylab import *

mpl.rcParams['font.sans-serif'] = ['SimHei']  # 指定默认字体
mpl.rcParams['axes.unicode_minus'] = False
host = '127.0.0.1'
username = 'root'
passwd = 'root'
database = 'appdb'
charset = 'utf8'


def data_show(y, y1, y2, y3):
    x = np.linspace(1, 12, 12)
    y4 = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    plt.plot(x, y, label='activeRate', color='r')
    plt.plot(x, y1, label='activeChangeRate', color='g')
    plt.plot(x, y2, label='coverageRate', color='b')
    plt.plot(x, y3, label='coverageChangeRate', color='c')
    plt.plot(x, y4, color='k')
    plt.title(u'王者荣耀活跃排行情况')
    plt.xlabel(u"日期（月份）")
    plt.ylabel(u"活跃率（%）")
    plt.legend()
    plt.show()


def get_data(cursor, sql):
    cursor.execute(sql)
    result = cursor.fetchall()
    datay = []
    for dstr in result:
        datay.append(dstr[1] * 100)
    y = []
    for datas in datay:
        if datas not in y:
            y.append(datas)
    return y


def main():
    conn = db.connect(host=host, user=username, passwd=passwd, db=database, port=3306,
                      charset=charset)
    cursor = conn.cursor()
    sql = 'select dates,activeRate from activedata WHERE appname="王者荣耀" order BY str_to_date(dates,"%Y-%m-%d")'
    y = get_data(cursor, sql)
    sql = 'select dates,activeChangeRate from activedata WHERE appname="王者荣耀" order BY str_to_date(dates,"%Y-%m-%d")'
    y1 = get_data(cursor, sql)
    sql = 'select dates,coverageRate from activedata WHERE appname="王者荣耀" order BY str_to_date(dates,"%Y-%m-%d")'
    y2 = get_data(cursor, sql)
    sql = 'select dates,coverageChangeRate from activedata WHERE appname="王者荣耀" order BY str_to_date(dates,"%Y-%m-%d")'
    y3 = get_data(cursor, sql)
    data_show(y, y1, y2, y3)
    conn.close()


if __name__ == '__main__':
    main()

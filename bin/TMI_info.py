#!/usr/bin/python
# -*- coding:UTF-8 -*-


import json
from pylab import *

plt.rcParams['font.sans-serif'] = ['simHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号


def age_lProfilePV_show(rates):
    # ind = months
    y = rates[0]
    y1 = rates[1]
    y2 = rates[2]
    y3 = rates[3]
    y4 = rates[4]
    y5 = rates[5]
    y6 = rates[6]
    y7 = rates[7]

    # N = 13
    x = arange(1, 14)
    # width = 0.35

    plt.plot(x, y, label='<=12', color='r')  # 红色
    plt.plot(x, y1, label='13-17', color='g')  # 绿色
    plt.plot(x, y2, label='18-23', color='b')  # 深蓝色
    plt.plot(x, y3, label='24-28', color='k')  # 黑色
    plt.plot(x, y4, label='29-35', color='cyan')  # 浅蓝色
    plt.plot(x, y5, label='36-45', color='m')  # 深红色
    plt.plot(x, y6, label='46-60', color='yellow')  # 黄色
    plt.plot(x, y7, label='>60', color='coral')  # 橙色

    # plt.xticks(x + width / 2, ind, rotation=40)

    plt.title(u'王者荣耀——每月在线人数—年龄')
    plt.xlabel(u'时间（月份）')
    plt.ylabel(u'人数')
    plt.legend()
    plt.savefig('../Image/age_lProfilePV.png', dpi=400)
    plt.show()


def age_dProfileRate_show(rates):
    # ind = months
    y = rates[0]
    y1 = rates[1]
    y2 = rates[2]
    y3 = rates[3]
    y4 = rates[4]
    y5 = rates[5]
    y6 = rates[6]
    y7 = rates[7]

    # N = 13
    x = arange(1, 14)
    # width = 0.35

    plt.plot(x, y, label='<=12', color='r')  # 红色
    plt.plot(x, y1, label='13-17', color='g')  # 绿色
    plt.plot(x, y2, label='18-23', color='b')  # 深蓝色
    plt.plot(x, y3, label='24-28', color='k')  # 黑色
    plt.plot(x, y4, label='29-35', color='cyan')  # 浅蓝色
    plt.plot(x, y5, label='36-45', color='m')  # 深红色
    plt.plot(x, y6, label='46-60', color='yellow')  # 黄色
    plt.plot(x, y7, label='>60', color='coral')  # 橙色

    # plt.xticks(x + width / 2, ind, rotation=40)
    plt.title(u'王者荣耀—每月人数占总人数的比例—年龄')
    plt.xlabel(u'时间（月份）')
    plt.ylabel(u'比率')
    plt.legend()
    plt.savefig('../Image/age_dProfileRate.png', dpi=400)
    plt.show()


def age_info():
    rates = []
    counts = []
    json_str = open('../datas/tmi_datas/datas_json/age.json').read()
    data = json.loads(json_str)
    ages = ['<=12', '13-17', '18-23', '24-28', '29-35', '36-45', '46-60', '>60']
    months = [data['2017-01'], data['2017-02'], data['2017-03'], data['2017-04'], data['2017-05'], data['2017-06'],
              data['2017-07'], data['2017-08'], data['2017-09'], data['2017-10'], data['2017-11'], data['2017-12'],
              data['2018-01']]

    for age in ages:
        rate = []
        count = []
        for month in months:
            for datas in month:
                if datas['sProfileValue'] == age:
                    rate.append(datas['dProfileRate'])
                    count.append(datas['lProfilePV'])
        rates.append(rate)
        counts.append(count)

    age_dProfileRate_show(rates)
    age_lProfilePV_show(counts)
    # print rates
    # print counts


def edu_lProfilePV_show(rates):
    # ind = months
    y = rates[0]
    y1 = rates[1]
    y2 = rates[2]
    y3 = rates[3]
    y4 = rates[4]
    y5 = rates[5]
    y6 = rates[6]

    # N = 13
    x = arange(1, 14)
    # width = 0.35

    plt.plot(x, y, label=u'小学', color='r')  # 红色
    plt.plot(x, y1, label=u'初中', color='g')  # 绿色
    plt.plot(x, y2, label=u'高中', color='b')  # 深蓝色
    plt.plot(x, y3, label=u'大专', color='k')  # 黑色
    plt.plot(x, y4, label=u'本科', color='cyan')  # 浅蓝色
    plt.plot(x, y5, label=u'硕士', color='m')  # 深红色
    plt.plot(x, y6, label=u'博士', color='coral')  # 橙色

    # plt.xticks(x + width / 2, ind, rotation=40)

    plt.title(u'王者荣耀—每月在线人数—学历')
    plt.xlabel(u'时间（月份）')
    plt.ylabel(u'人数')
    plt.legend()
    plt.savefig('../Image/edu_lProfilePV.png', dpi=400)
    plt.show()


def edu_dProfileRate_show(rates):
    # ind = months
    y = rates[0]
    y1 = rates[1]
    y2 = rates[2]
    y3 = rates[3]
    y4 = rates[4]
    y5 = rates[5]
    y6 = rates[6]

    # N = 13
    x = arange(1, 14)
    # width = 0.35

    plt.plot(x, y, label=u'小学', color='r')  # 红色
    plt.plot(x, y1, label=u'初中', color='g')  # 绿色
    plt.plot(x, y2, label=u'高中', color='b')  # 深蓝色
    plt.plot(x, y3, label=u'大专', color='k')  # 黑色
    plt.plot(x, y4, label=u'本科', color='cyan')  # 浅蓝色
    plt.plot(x, y5, label=u'硕士', color='m')  # 深红色
    plt.plot(x, y6, label=u'博士', color='coral')  # 橙色

    # plt.xticks(x + width / 2, ind, rotation=40)
    plt.title(u'王者荣耀—每月人数占总人数的比例—学历')
    plt.xlabel(u'时间（月份）')
    plt.ylabel(u'比率')
    plt.legend()
    plt.savefig('../Image/edu_dProfileRate.png', dpi=400)
    plt.show()


def edu_info():
    rates = []
    counts = []
    json_str = open('../datas/tmi_datas/datas_json/edu.json').read()
    data = json.loads(json_str)
    edus = ['xx', 'cz', 'gz', 'dz', 'bk', 'ss', 'bs']
    months = [data['2017-01'], data['2017-02'], data['2017-03'], data['2017-04'], data['2017-05'], data['2017-06'],
              data['2017-07'], data['2017-08'], data['2017-09'], data['2017-10'], data['2017-11'], data['2017-12'],
              data['2018-01']]

    for edu in edus:
        rate = []
        count = []
        for month in months:
            for datas in month:
                if datas['sProfileValue'] == edu:
                    rate.append(datas['dProfileRate'])
                    count.append(datas['lProfilePV'])
        rates.append(rate)
        counts.append(count)

    # print rates
    # print counts
    edu_dProfileRate_show(rates)
    edu_lProfilePV_show(counts)


def pro_lProfilePV_show(rates):
    # ind = months
    y = rates[0]
    y1 = rates[1]
    y2 = rates[2]
    y3 = rates[3]
    y4 = rates[4]
    y5 = rates[5]
    y6 = rates[6]
    y7 = rates[7]

    # N = 13
    x = arange(1, 14)
    # width = 0.35

    plt.plot(x, y, label=u'广东省', color='r')  # 红色
    plt.plot(x, y1, label=u'河南省', color='g')  # 绿色
    plt.plot(x, y2, label=u'山东省', color='b')  # 深蓝色
    plt.plot(x, y3, label=u'江苏省', color='k')  # 黑色
    plt.plot(x, y4, label=u'河北省', color='cyan')  # 浅蓝色
    plt.plot(x, y5, label=u'江西省', color='m')  # 深红色
    plt.plot(x, y6, label=u'安徽省', color='yellow')  # 黄色
    plt.plot(x, y7, label=u'湖南省', color='coral')  # 橙色

    # plt.xticks(x + width / 2, ind, rotation=40)

    plt.title(u'王者荣耀—每月在线人数—省份')
    plt.xlabel(u'时间（月份）')
    plt.ylabel(u'人数')
    plt.legend()
    plt.savefig('../Image/pro_lProfilePV.png', dpi=400)
    plt.show()


def pro_dProfileRate_show(rates):
    # ind = months
    y = rates[0]
    y1 = rates[1]
    y2 = rates[2]
    y3 = rates[3]
    y4 = rates[4]
    y5 = rates[5]
    y6 = rates[6]
    y7 = rates[7]

    # N = 13
    x = arange(1, 14)
    # width = 0.35

    plt.plot(x, y, label=u'广东省', color='r')  # 红色
    plt.plot(x, y1, label=u'河南省', color='g')  # 绿色
    plt.plot(x, y2, label=u'山东省', color='b')  # 深蓝色
    plt.plot(x, y3, label=u'江苏省', color='k')  # 黑色
    plt.plot(x, y4, label=u'河北省', color='cyan')  # 浅蓝色
    plt.plot(x, y5, label=u'江西省', color='m')  # 深红色
    plt.plot(x, y6, label=u'安徽省', color='yellow')  # 黄色
    plt.plot(x, y7, label=u'湖南省', color='coral')  # 橙色

    # plt.xticks(x + width / 2, ind, rotation=40)
    plt.title(u'王者荣耀—每月人数占总人数的比例—省份')
    plt.xlabel(u'时间（月份）')
    plt.ylabel(u'比率')
    plt.legend()
    plt.savefig('../Image/pro_dProfileRate.png', dpi=400)
    plt.show()


def pro_info():
    rates = []
    counts = []
    json_str = open('../datas/tmi_datas/datas_json/province.json').read()
    data = json.loads(json_str)
    pros = ['guangdong', 'henan', 'shandong', 'jiangsu', 'hebei', 'jiangxi', 'anhui', 'hunan', 'zhejiang', 'fujian',
            'congqing', 'sichuan', 'guangxi', 'beijing', 'liaoning', 'hubei', 'shanghai', 'tianjin', 'heilongjiang',
            'sanxi', 'jilin', 'guizhou', 'yunnan', 'hainan', 'neimenggu', 'xinjiang', 'gaisu', 'ningxia', 'qinghai',
            'xianggang', 'xizang', 'aomen', 'taiwai', 'shanxi']
    months = [data['2017-01'], data['2017-02'], data['2017-03'], data['2017-04'], data['2017-05'], data['2017-06'],
              data['2017-07'], data['2017-08'], data['2017-09'], data['2017-10'], data['2017-11'], data['2017-12'],
              data['2018-01']]

    for pro in pros:
        rate = []
        count = []
        for month in months:
            for datas in month:
                if datas['sProfileValue'] == pro:
                    rate.append(datas['dProfileRate'])
                    count.append(datas['lProfilePV'])
        rates.append(rate)
        counts.append(count)

    pro_dProfileRate_show(rates)
    pro_lProfilePV_show(counts)
    print rates
    print counts


def main():
    age_info()
    edu_info()
    pro_info()


if __name__ == '__main__':
    main()

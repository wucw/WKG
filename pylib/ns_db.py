# /usr/bin/python
# -*- coding:UTF-8 -*-

# @Time    : 2018/2/24
# @Author  : 吴昌文
# @Site    :
# @File    : ns_db.py
# @Software: PyCharm
# @Version : v0.1
# @Description:主要实现对数据库表的创建，数据的增删改查等功能

import sys
import MySQLdb

reload(sys)
sys.setdefaultencoding("UTF-8")


class mysqldb(object):
    conn = ""
    cursor = ""

    def __init__(self, logger, host='localhost', username='root', passwd='root', database='', charset='utf8'):
        try:
            self.conn = MySQLdb.connect(host=host, user=username, passwd=passwd, db=database, port=3306,
                                        charset=charset)
            self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            logger.info('Connecting the database of [%s] successful' % database)
        except MySQLdb.Error, e:
            logger.error('Connect failed! ERROR (%s): %s' % (e.args[0], e.args[1]))
            sys.exit()

    def _exeCute(self, logger, sql=''):
        """
        :function:针对读操作返回结果集
        :param sql:要执行的SQL语句
        :return: list 数据结构
        :field sample:无
        """
        try:
            self.cursor.execute(sql)
            records = self.cursor.fetchall()
            logger.info('Get data from the database,and the data of size is [%s]' % len(records))
            return records
        except MySQLdb.Error as e:
            logger.error('MySQL execute failed! ERROR (%s): %s' % (e.args[0], e.args[1]))

    def _exeCuteCommit(self, logger, sql=''):
        """
        :function:针对更新,删除,事务等操作失败时回滚
        :param sql: 要执行的SQL语句
        :return: 无
        :field sample:无
        """
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(sql)
            self.conn.commit()
            logger.info('SQL execution successfully ... ...')
        except MySQLdb.Error as e:
            self.conn.rollback()
            logger.error('MySQL execute failed! ERROR (%s): %s' % (e.args[0], e.args[1]))

    def createTable(self, logger, table, attr_dict, constraint):
        """
        :function:创建表
        :param table: 表名称
        :param attr_dict: 表的字段，（属性键值对）
        :param constraint: 主外键约束
        :return: 无
        :field sample:attr_dict:{'book_name':'varchar(200) NOT NULL'...};constraint:PRIMARY KEY(`id`)
        """
        sql = ''
        sql_mid = '`id` bigint(11) NOT NULL AUTO_INCREMENT,'
        for attr, value in attr_dict.items():
            sql_mid = sql_mid + '`' + attr + '`' + ' ' + value + ','
        sql = sql + 'CREATE TABLE IF NOT EXISTS %s (' % table
        sql = sql + sql_mid
        sql = sql + constraint
        sql = sql + ') ENGINE=InnoDB DEFAULT CHARSET=utf8'
        logger.info('createTable: %s' % sql)
        self._exeCuteCommit(logger, sql)

    def select_all_field(self, logger, table, cond_dict, order):
        """
        :function:查询表内容
        :param table: 要查询的表
        :param cond_dict:查询的条件，以键值对的形式传入
        :param order:查询结果的排序方式
        :return:查询到的结果集
        :field sample:cond_dict:{'name':'xiaoming'...};order:'order by id desc'
        """
        consql = ""
        if cond_dict != "":
            for key, value in cond_dict.items():
                consql = consql + key + '=' + value + ' and'
            consql = consql + ' 1=1 '
            sql = 'select * from %s where ' % table
            sql = sql + consql + order
            logger.info('select_all_field: %s' % sql)
            return self._exeCute(logger, sql)

    def selectbyfield(self, logger, table, field, cond_dict):
        """
        :function:查找指定的一个或多个字段
        :param logger: 日志对象
        :param table: 数据表名称
        :param field:  查询的字段
        :param cond_dict: 查询的条件（字典格式）
        :return: 查询的结果集
        :field sample:field:id,name,age,...;cond_dict:{'name':'xiaoming'...}
        """
        consql = ""
        if cond_dict != "":
            for key, value in cond_dict.items():
                consql = consql + key + '=' + value + ' and'
            consql = consql + ' 1=1 '
            sql = 'select %s from %s where ' % (field, table)
            sql = sql + consql
            logger.info('selectbyfield: %s' % sql)
            return self._exeCute(logger, sql)

    def select_field(self, logger, table, field, cond_dict, order):
        """
        :function:查找指定的一个或多个字段
        :param table: 表名称
        :param field: 查询指定字段名（字符串格式）
        :param cond_dict: 查询的条件（字典格式）
        :param order: 查询结果的排序方式（字符串格式）
        :return: 查询的结果集
        :field sample:field:id,name,age,...;cond_dict:{'name':'xiaoming'...};order:'order by id desc'
        """
        consql = ""
        if cond_dict != "":
            for key, value in cond_dict.items():
                consql = consql + key + '=' + value + ' and'
            consql = consql + ' 1=1 '
            sql = 'select %s from %s where ' % (field, table)
            sql = sql + consql + order
            logger.info('select_field: %s' % sql)
            return self._exeCute(logger, sql)

    def select_all(self, logger, tablename, attrs):
        """
        :function:查找指定的一个或多个字段
        :param table: 表名称
        :param attrs: 要查询的字段，以字符串形式
        :return: 查询的结果集
        """
        sql = 'select %s from %s' % (attrs, tablename)
        logger.info('select_all: %s' % sql)
        return self._exeCute(logger, sql)

    def insert_one(self, logger, tablename, attrs, value):
        """
        :function:向数据表插入一条数据
        :param tablename: 表名称
        :param attrs: 要插入值的字段名称
        :param vales:要插入的数据
        :return:无
        :field sample:attrs:[id,name,...];values:[1,'jack',...]
        """
        attrs_sql = '(' + ','.join(attrs) + ')'
        value_sql = 'values(' + ','.join(value) + ')'
        sql = 'insert into %s' % tablename
        sql = sql + attrs_sql + value_sql
        logger.info('insert_one: %s' % sql)
        self._exeCuteCommit(logger, sql)

    def insertmany(self, logger, tablename, attrs, values):
        """
        :function:向数据表插入多条数据
        :param logger: 日志对象
        :param tablename: 表名称
        :param attrs: 要插入的字段名称
        :param values: 要插入的数据
        :return: 无
        :field sample:attrs:[id,name,...];values:[(1,'jack',...),(2,'jack1',...)]
        """
        str_list = attrs.split(',')
        consql = '%s,' * len(str_list)
        sql = 'INSERT INTO %s(%s) VALUES (%s)' % (tablename, attrs, consql[:-1])
        self.cursor = self.conn.cursor()
        try:
            self.cursor.executemany(sql, values)
            self.conn.commit()
            logger.info('Insert data to database is successfully')
        except Exception as e:
            self.conn.rollback()
            logger.error('Insert data to database is failure,It\'s [%s]', e)
            sys.exit()

    def insert_many(self, logger, tablename, attrs, values):
        """
        :function:一次向数据库中插入多条数据
        :param tablename: 表名称
        :param attrs: 要插入值的字段名称
        :param value: 要插入的多条数据
        :return:无
        :field sample: attrs:[id,name,...];values:[[1,'jack'],[2,'rose']]
        """
        value_sql = ['%s' for s in attrs]
        attrs_sql = '(' + ','.join(attrs) + ')'
        values_sql = ' values(' + ','.join(value_sql) + ')'
        sql = 'insert into %s' % tablename
        sql = sql + attrs_sql + values_sql
        logger.info('insertMany: %s' % sql)
        try:
            for i in range(0, len(values), 20000):
                self.cursor.executemany(sql, values[i:i + 20000])
                self.conn.commit()
            logger.info('Insert many datas to database,SQL execution successfully ... ...')
        except MySQLdb.Error, e:
            self.conn.rollback()
            logger.error('insertMany executemany failed! ERROR (%s): %s' % (e.args[0], e.args[1]))
            sys.exit()

    def delete(self, logger, tablename, cond_dict):
        """
        :function:根据条件删除数据
        :param tablename: 表名称
        :param cond_dict: 删除条件
        :return:无
        :field sample:cond_dict:{id:1,name:zhangsan,...}
        """
        consql = ""
        if cond_dict != "":
            for key, value in cond_dict.items():
                consql = consql + key + '=' + value + ' and'
            consql = consql + ' 1=1 '
            sql = 'delete from %s where ' % tablename
            sql = sql + consql
            logger.info('delete: %s' % sql)
            self._exeCuteCommit(logger, sql)

    def update(self, logger, tablename, cond_dict, value_dict):
        """
        :function:根据条件对数据进行修改
        :param tablename:表名称
        :param cond_dict:修改条件
        :param value_dict:修改的数据
        :return:无
        :field sample:cond_dict:{id:1,name:zhangsan,...};value_dict:{id:1,name:zhangsan,...}
        """
        consql = ""
        datasql = ""
        if cond_dict != "" and value_dict != "":
            for key, value in cond_dict.items():
                consql = consql + key + '=' + value + ' and '
            consql = consql + '1=1 '
            for key, value in value_dict.items():
                datasql = datasql + key + '=' + value + ','
            sql = 'update %s set ' % tablename + datasql[:-1] + ' where ' + consql
            logger.info('update: %s' % sql)
            self._exeCuteCommit(logger, sql)

    def close(self):
        self.cursor.close()
        self.conn.close()

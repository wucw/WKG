#!/apps/ns/python/bin/python
#-*- encoding: utf-8 -*-
# by chaolm on 2016-03-4
import time
import MySQLdb
from types import *
from ns_log import *
from sqlalchemy import *
from sqlalchemy.schema import Table
from warnings import filterwarnings


# update by mazcheng @20170912
# -------------------------------
# add list2db_byMySQLdb function
# add conn2db_byMySQLdb function
# -------------------------------


filterwarnings('error', category=MySQLdb.Warning)
# ERROR: MySQL server has gone away
# current db once insert number: > show global variables like 'max_allowed_packet';
SINGLE_TRANSFER_NUM = 100000  # 10W/time

def conn_to_db(logger, db_args, db_conn_sql, reconn=False, sleep_time=10, stop_lock=[]):
	db = create_engine(db_args, encoding='utf8', echo=False)
	conn = None
	while not conn:
		logger.info("db.connect() retry [%s]", db_args)
		try:
			conn = db.connect()
			if conn or (not reconn):
				break
		except Exception, e:
			logger.error("db conn fail[%s][%s]", db_args, str(e))
			if conn:
				conn.close()
				conn = None
		if stop_lock[0]:
			return None
		time.sleep(sleep_time)
	logger.debug("conn to db success")
	if db_conn_sql:
		try:
			logger.debug("db execute[%s]", db_conn_sql)
			conn.execute(db_conn_sql)
		except Exception, e:
			conn.close()
			conn = None
			logger.error("db execute fail[%s][%s]", db_conn_sql, str(e))
			return None
	return conn

def select_to_list(logger, conn, sql):
	r = None

	if sql:
		try:
			r = conn.execute(sql).fetchall()
		except Exception, e:
			logger.error("db execute fail[%s][%s]", sql, str(e))
			return False, r

	return True, r

def list2db(logger, conn, table, seq, lst):
	metadata = MetaData(conn)
	try:
		table = Table(table, metadata,
				Column('id', Integer, Sequence(seq), primary_key=True),
				autoload=True, autoload_with=conn)
		trans = conn.begin()
		for x in lst:
			conn.execute(table.insert(), x)
		trans.commit()
	except Exception, e:
		logger.error("db inert fail[%s]", str(e))
		try:
			trans.rollback()
		except:
			logger.error("rollback() fail[%s]", str(e))
		return False
	return True

def ns_list2db(logger, db_args, db_conn_sql, db_table, db_seq, lst, pkey=[], update=False, \
		update_record_time=False, conn=None, conn_close=True, reconn=False, \
		sleep_time=10, stop_lock=[]):

	start = time.time()
	try:
		step = "connect"
		db = create_engine(db_args, encoding='utf8', echo=False)
		#db = create_engine(db_args, encoding='utf8', echo=True)
		metadata = MetaData(db)

		while not conn:
			logger.info("db.connect() retry [%s]", db_args)
			try:
				conn = db.connect()
				if conn or (not reconn):
					break
			except Exception, e:
				logger.error("db conn fail[step:%s][%s]", step, str(e))
				if conn:
					conn.close()
					conn = None
			if stop_lock[0]:
				return None
			time.sleep(sleep_time)

		step = "Table"
		table = Table(db_table, metadata,
				Column('id', Integer, Sequence(db_seq), primary_key=True),
				autoload=True, autoload_with=conn)
		step = "execute"
		if db_conn_sql:
			conn.execute(db_conn_sql)
	except Exception, e:
		logger.error("db conn fail[step:%s][%s]", step, str(e))
		if conn:
			conn.close()
			conn = None
		return None

	trans = conn.begin()
	try:
		inum = unum = num = 0
		for x in lst:
			if pkey:
				cause = []
				if type(pkey) is ListType:
					for p in pkey:
						exec "cause.append(table.c.%s == x[p])" % p
				else:
					exec "cause.append(table.c.%s == x[pkey])" % pkey
				if not cause:
					continue

				if update:
					stmt = table.update().where(and_(*cause))
					if update_record_time:
						x['record_time'] = time.strftime("%Y-%m-%d %H:%M:%S")
					else:
						if x.has_key('record_time'):
							del x['record_time']
					ret = conn.execute(stmt, x)
					if ret.rowcount == 0:
						x['record_time'] = time.strftime("%Y-%m-%d %H:%M:%S")
						conn.execute(table.insert(), x)
						inum += 1
					else:
						unum += 1
				else:
					stmt = table.delete().where(and_(*cause))
					conn.execute(stmt)
					x['record_time'] = time.strftime("%Y-%m-%d %H:%M:%S")
					conn.execute(table.insert(), x)
					inum += 1
			num += 1
		trans.commit()
	except Exception, e:
		logger.error("insert/update db fail[%s]", str(e))
		try:
			trans.rollback()
			if conn_close:
				conn.close()
				conn = None
		except:
			logger.error("rollback() fail[%s]", str(e))
		return None

	end = time.time()
	dur = end - start
	speed = num / dur

	logger.info("insert/update [%d]/[%d] line info [%s], use [%d]s, speed [%d/s]",
			inum, unum, db_table, dur, speed)

	if conn_close:
		conn.close()
		conn = None
		return True

	return conn


def conn2db_byMySQLdb(logger, host, user, passwd, db, port=3306, charset='utf8', reconn=False, sleep_time=10, stop_lock=[]):
	conn = None
	conn_times = 0
	while not conn:
		try:
			conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, port=int(port), charset=charset)
			conn_times += 1
			logger.info('Connect to [mysql://%s:%s@%s:%s/%s] conn_times [%d]', user, passwd, host, port, db, conn_times)
			if conn or (not reconn):
				break
		except Exception as e:
			logger.error('Connect to [mysql://%s:%s@%s:%s/%s] error [%s]', user, passwd, host, port, db, e)
			if conn:
				conn.close()
				conn = None
			if stop_lock[0]:
				return None
		time.sleep(sleep_time)
	return conn


def list2db_byMySQLdb(logger, conn, data_list, table_name, isclose=False, stop_lock=[]):
	"""
	Use Mysqldb module
	Insert data by mysqldb.executemany simplely
	Only support whole_quantity all fields insert
	Not support Update
	:param data_list: [record1, record2, ...]
	:return: insert_status Boolean(ok?)
	----------------------------------------------------
	Note:	must keep up
	table's field numbers == data_list's record length
	----------------------------------------------------
	"""
	if conn:
		cursor = conn.cursor()
	else:
		logger.error('Current Connection is Error')
		return False

	insert_status = False

	single_data_length = len(data_list[0])
	construct_stmt = 'INSERT INTO {} VALUES ('.format(table_name) + ', '.join(['%s'] * single_data_length) + ')'
	s_time = time.time()
	for this_time_num in range(0, len(data_list), SINGLE_TRANSFER_NUM):
		this_data_list = data_list[this_time_num: this_time_num+SINGLE_TRANSFER_NUM]
		if stop_lock[0]:
			break
		try:
			cursor.executemany(construct_stmt, this_data_list)
			conn.commit()
			e_time = time.time()
			logger.info('Insert the data list to table [%s] time [%s] num [%s] speed [%s/s]',
						table_name, e_time-s_time, len(this_data_list), str(len(this_data_list)/(e_time-s_time)))
			insert_status = True
		except Exception as e:
			logger.error('Insert the data list [%d] to table [%s] error [%s]',
					len(this_data_list), table_name, e)
			conn.rollback()
			insert_status = False
			continue
		if stop_lock[0]:
			break

	if isclose:
		cursor.close()
		conn.close()
	return insert_status

#!_NS_INSX/python/bin/python
#-*- encoding: utf-8 -*-
# by chaolm on 2016-03-4
from ns_log import *

import xml.etree.cElementTree as ET

def xml_file2dic(logger, fname, dic_key, tag_fix_func=None, text_fix_func=None):
	lst = []
	try:
		tree = ET.parse(fname)
	except Exception, e:
		logger.error("ET.parse() fail[%s]", str(e))
		return False, None
	num = 0

	root = tree.getroot()
	for x in root:
		dic = {}
		num = num + 1
		rrt = 0
		for y in x:
			#rrt = rrt + 1
			if rrt > 33:
				break
			if tag_fix_func:
				key = tag_fix_func(y.tag)
				if not key:
					continue
			else:
				key = y.tag

			if text_fix_func:
				text = text_fix_func(key, y.text)
			else:
				text = y.text
			dic[key] = text

		lst.append(dic)

	dic = {}
	for x in lst:
		dic[x[dic_key]] = x

	logger.info("read [%d][%d] item from [%s]", num, len(dic.keys()), fname)
	return True, dic

	


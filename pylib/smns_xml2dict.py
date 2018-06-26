#!/apps/ns/python/bin/python
# _*_coding:utf-8_*_

"""
@author: mazhicheng
@file: smns_xml2dict.py
@time: 2017/8/28 10:11
@desc: Support XML convert to Dict or Dict convert to XML
"""

import re
import xml.etree.ElementTree as ET


class XML2Dict(object):
	def __init__(self, isxmlns=False, coding='utf8'):
		# consider XML namespace
		# Refer: https://stackoverflow.com/questions/13412496
		self._isconsider_namespace = isxmlns
		self._coding = coding

	def _parse_node(self, node):
		"""
		By recursion to DFS nodes
		:param node: Element
		:return: tree Dict(current parsed ele)
		"""
		tree = {}

		# parse childrens
		for child in list(node):
			curr_tag = child.tag
			curr_attr = child.attrib
			curr_text = child.text.strip().encode(self._coding) if child.text is not None else ''
			curr_tree = self._parse_node(child)

			if not curr_tree:
				curr_dict = self.__construct_dict(curr_tag, curr_text, curr_attr)
			else:
				curr_dict = self.__construct_dict(curr_tag, curr_tree, curr_attr)

			# trigger @node
			if curr_tag not in tree:
				tree.update(curr_dict)
				continue

			attr_tag = '@' + curr_tag
			attr_tree = tree[curr_tag]
			if not isinstance(attr_tree, list):
				if not isinstance(attr_tree, dict):
					attr_tree = dict()
				if attr_tag in tree:
					attr_tree['#' + curr_tag] = tree[attr_tag]
					del tree[attr_tag]

				tree[curr_tag] = [attr_tree]

			if curr_attr:
				curr_tree['#' + curr_tag] = curr_attr
			tree[curr_tag].append(curr_tree)

		return tree

	def __construct_dict(self, tag, val, attr=None):
		"""
		Generate a new dict with tag and val
		If not attr: convert tag name -> @tag
		i.e.
			```
			<cncpe operator="OR" negate="false">
				<cncpe-lang name="cpe:/a:open-uri-cached_project:open-uri-cached:20120107"/>
			</cncpe>
			```
		Convert tuple or list -> dict
		:param tag: str(as ret's key)
		:param val: str(as ret's val)
		:param attr: dict(stay parsed)
		:return: ret Dict(result)
		"""
		ret = {tag: val}

		# Save attr as @tag value(replace)
		if attr:
			attr_tag = '@' + tag
			attr_attr = {k: v for k, v in attr.items()}
			ret[attr_tag] = attr_attr
			del attr_tag
			del attr_attr
		return ret

	def xml2dict(self, xmlstring):
		"""
		XML2Dict main method to convert XML -> Dict
		:param xmlstring: str()
		:return: converted_dict Dict
		"""
		if not self._isconsider_namespace:
			xmlstring = re.sub('xmlns="[^"]+"', '', xmlstring, count=1)
		Estr = ET.fromstring(xmlstring)
		return self.__construct_dict(Estr.tag, self._parse_node(Estr), Estr.attrib)

	def xmlfile2dict(self, xmlfile_name):
		"""
		By XML file to convert
		:param xmlfile_name: str(abs path;the whole standard xml file)
		:return: converted_dict Dict
		"""
		with open(xmlfile_name, 'r') as xmlfile:
			xmlstring = xmlfile.read().strip()
		return self.xml2dict(xmlstring)


class Dict2XML(object):
	"""
	XXX: Suggest doning not USE this class @20170828
	"""
	def __init__(self, coding='utf8'):
		self._coding = coding
		self._root = None

	def _parse_dict(self, e_dict, e_tree=None):
		"""
		By recursion to parse dict
		:param e_dict: Dict(will be parsed)
		:param e_tree: None(or Element)
		:return: tree Element
		"""
		tree = None

		for tag, val in e_dict.items():
			if not isinstance(val, dict) and not isinstance(val, (list, tuple)):
				val = str(val)
			# Children nodes
			if e_tree is not None and isinstance(val, (list, tuple)):
				e_list = [self._parse_dict({tag: item}) for item in val]
				for e_tag in e_list:
					e_tree.append(e_tag)

				del e_list
				continue
			# Tree attributes
			elif e_tree is None and tag.startswith('@'):
				e_tree = tree
			tree = self.__construct_xml(tag, val, e_tree)
		return tree

	def __construct_xml(self, tag, val, parent):
		"""
		Generate a new xml with dict's key and val
		:param tag: str(dict's key)
		:param val: str(dict's val)
		:param parent: Element
		:return:
		"""
		if tag.startswith('@') and isinstance(val, dict):
			tag = tag[1:]

			if parent is None:
				if self._root is None:
					element = ET.Element(tag, val)
					self._root = element
				else:
					element = self._root
					self._root = None
			else:
				element = parent if tag == parent.tag else parent.find(tag)
				if element is None:
					# Element 1st to add
					element = ET.SubElement(parent, tag, val)
				else:
					# Save attributes
					element.attrib.update(val)
			return element

		s_tag = '#' + tag
		if s_tag in val:
			if isinstance(val[s_tag], dict):
				element = ET.Element(tag, val[s_tag])
			else:
				element = ET.Element(tag)
			del val[s_tag]
		else:
			if parent is None:
				if self._root is None:
					element = ET.Element(tag)
					self._root = element
				else:
					element = self._root
					self._root = None
			else:
				element = parent.find(tag)
				if element is None:
					element = ET.SubElement(parent, tag)
		if isinstance(val, dict):
			self._parse_dict(val, element)
		else:
			element.text = val

		return element

	def dict2xml(self, which_dict):
		"""
		Dict2XML main method to convert Dict -> XML
		:param which_dict: Dict(nested dict)
		:return: element Element
		"""
		for sub_dict in which_dict:
			return ET.tostring(self._parse_dict(which_dict[sub_dict]))

	def dict2xmlfile(self, which_dict, xmlfile_name):
		"""
		By XML file to store converted XML string
		:param which_dict: Dict(converted)
		:param xmlfile_name: str(abs path;the whole standard xml file)
		:return: None
		"""
		with open(xmlfile_name, 'a+') as xmlfile:
			xmlfile.write(self.dict2xml(which_dict))

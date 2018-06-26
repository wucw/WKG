# -*- coding:utf-8 -*-

from pyecharts import Map

value = [102997, 937280, 522626, 521623, 420380, 378162, 254378, 201523, 195371, 192150, 190213, 159797, 148173,
         137014, 115432, 103783, 92196, 90027, 73741, 62257, 57028, 48318, 33716, 31489, 30738, 24086, 20499, 20002,
         13390, 9419, 2186, 2142, 852, 3]
attr = ["山西", "广东", "河南", "山东", "江苏", "河北", "江西", "安徽", "湖南", "浙江",
        "福建", "重庆", "四川", "广西", "北京", "辽宁", "湖北", "上海", "天津",
        "黑龙江", "陕西", "吉林", "贵州", "云南", "海南", "内蒙古", "新疆", "甘肃", "宁夏", "青海", "香港", "西藏", "澳门", "台湾"]
map = Map("各省王者荣耀人数分布", width=1500, height=800, title_pos="center")
map.add("", attr, value, maptype='china', is_visualmap=True,
        visual_text_color='#000')
map.show_config()
map.render()

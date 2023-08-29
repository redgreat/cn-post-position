#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw 
# @generate at 2023/8/29 16:17

import json
import requests
from datetime import datetime

api_key = ''
provinces = ['北京市', '天津市', '河北省', '山西省', '内蒙古自治区', '辽宁省', '吉林省', '黑龙江省', '上海市', '江苏省', '浙江省', '安徽省', '福建省', '江西省',
             '山东省', '河南省', '湖北省', '湖南省', '广东省', '广西壮族自治区', '海南省', '重庆市', '四川省', '贵州省', '云南省', '西藏自治区', '陕西省', '甘肃省',
             '青海省', '宁夏回族自治区', '新疆维吾尔自治区', '台湾省', '香港特别行政区', '澳门特别行政区']
api_url = 'https://restapi.amap.com/v3/config/district?key={}&keywords={}&subdistrict=3&extensions=base'
sql_ins = 'INSERT INTO home_areageo(AreaCode,ALng,ALat) VALUES '
start_time = datetime.now()

"""
create table home_areageo
(
    Id         bigint auto_increment comment '自增主键'
        primary key,
    AreaCode   varchar(50)                        null comment '区域编码',
    ALng       decimal(18, 6)                     null comment '高德经度',
    ALat       decimal(18, 6)                     null comment '高德纬度',
    InsertTime datetime default CURRENT_TIMESTAMP not null comment '数据插入时间'
)
    comment '地区经纬度';
"""

for i in provinces:
    api_url = api_url.format(api_key, i)
    res = requests.get(api_url)
    province = json.loads(res.text)['districts']
    procode = province[0]['adcode']
    prolng = province[0]['center'].split(',')[0]
    prolat = province[0]['center'].split(',')[1]
    citys = province[0]['districts']
    sql_ins += '(\'' + procode + '\',' + str(prolng) + ',' + str(prolat) + '),\n'
    for city in citys:
        citycode = city['adcode']
        citylng = city['center'].split(',')[0]
        citylat = city['center'].split(',')[1]
        areas = city['districts']
        sql_ins += '(\'' + citycode + '\',' + str(citylng) + ',' + str(citylat) + '),\n'
        for area in areas:
            areacode = area['adcode']
            arealng = area['center'].split(',')[0]
            arealat = area['center'].split(',')[1]
            sql_ins += '(\'' + areacode + '\',' + str(arealng) + ',' + str(arealat) + '),\n'
sql_ins = sql_ins.rstrip(',\n') + ';'
with open("cnpost_insert.sql", "w") as file:
    file.write(sql_ins)
end_time = datetime.now()

print('SQL生成完成，总计耗时{}秒！'.format((end_time - start_time).total_seconds()))

#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw 
# @generate at 2023/8/30 11:09

import json
import requests
from datetime import datetime
import configparser
import pymysql

db_config = configparser.ConfigParser()
db_config.read_file(open('db.conf', encoding='utf-8', mode='rt'))
my_host = db_config.get("serviceordercenter", "host")
my_port = int(db_config.get("serviceordercenter", "port"))
my_username = db_config.get("serviceordercenter", "username")
my_password = db_config.get("serviceordercenter", "password")
my_database = db_config.get("serviceordercenter", "database")
my_charset = db_config.get("serviceordercenter", "charset")

api_key = db_config.get("apikey", "api_key")

con = pymysql.connect(host=my_host,
                      port=my_port,
                      user=my_username,
                      password=my_password,
                      database=my_database,
                      charset=my_charset)

provinces = ['北京市', '天津市', '河北省', '山西省', '内蒙古自治区', '辽宁省', '吉林省', '黑龙江省', '上海市', '江苏省',
             '浙江省', '安徽省', '福建省', '江西省',
             '山东省', '河南省', '湖北省', '湖南省', '广东省', '广西壮族自治区', '海南省', '重庆市', '四川省', '贵州省',
             '云南省', '西藏自治区', '陕西省', '甘肃省',
             '青海省', '宁夏回族自治区', '新疆维吾尔自治区', '台湾省', '香港特别行政区', '澳门特别行政区', '台湾省']
api_uri = 'https://restapi.amap.com/v3/config/district?key={}&keywords={}&subdistrict=2&extensions=base'
sql_ins = 'INSERT INTO home_areageo(AreaCode,ALng,ALat) VALUES (%s, %s, %s);'
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
try:
    cur = con.cursor()
    cur.execute('TRUNCATE TABLE home_areageo;')
    for i in provinces:
        pro_start = datetime.now()
        api_url = api_uri.format(api_key, i)
        res = requests.get(api_url)
        province = json.loads(res.text)['districts']
        procode = province[0]['adcode']
        prolng = province[0]['center'].split(',')[0]
        prolat = province[0]['center'].split(',')[1]
        citys = province[0]['districts']
        cur.execute(sql_ins, (procode, float(prolng), float(prolat)))
        for city in citys:
            citycode = city['adcode']
            citylng = city['center'].split(',')[0]
            citylat = city['center'].split(',')[1]
            areas = city['districts']
            cur.execute(sql_ins, (citycode, float(citylng), float(citylat)))
            for area in areas:
                areacode = area['adcode']
                arealng = area['center'].split(',')[0]
                arealat = area['center'].split(',')[1]
                cur.execute(sql_ins, (areacode, float(arealng), float(arealat)))
        pro_end = datetime.now()
        print('省份："{}"处理完成，总计耗时{}秒！'.format(i, (pro_end - pro_start).total_seconds()))

        con.commit()
except Exception as e:
    print(e)
finally:
    cur.close()
    con.close()

end_time = datetime.now()
print('全部处理完成，总计耗时{}秒！'.format((end_time - start_time).total_seconds()))

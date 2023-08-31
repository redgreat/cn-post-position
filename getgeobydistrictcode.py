#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw 
# @generate at 2023/8/30 13:18

import json
import requests
from datetime import datetime
import configparser
import pymysql.cursors

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
                      charset=my_charset,
                      cursorclass=pymysql.cursors.DictCursor)

api_uri = 'https://restapi.amap.com/v3/geocode/geo?address={}&city={}&key={}'
sql_sel = ('SELECT Code, Name FROM basic_district a WHERE a.Deleted=0 AND NOT '
           'EXISTS(SELECT 1 FROM home_areageo b WHERE b.AreaCode = a.Code);')
sql_ins = 'INSERT INTO home_areageo(AreaCode,ALng,ALat) VALUES (%s, %s, %s);'
start_time = datetime.now()

try:
    cur = con.cursor()
    # cur.execute('TRUNCATE TABLE home_areageo;')
    cur.execute(sql_sel)
    src = cur.fetchall()
    for row in src:
        code = row['Code']
        name = row['Name']
        api_url = api_uri.format(name, code, api_key)
        res = requests.get(api_url)
        geos = json.loads(res.text)['geocodes'][0]['location'].split(',')
        alng = geos[0]
        alat = geos[1]
        cur.execute(sql_ins, (code, float(alng), float(alat)))
        # time.sleep(0.5)
        con.commit()
except Exception as e:
    print(e, code, api_url, json.loads(res.text))
finally:
    cur.close()
    con.close()

end_time = datetime.now()
print('全部处理完成，总计耗时{}秒！'.format((end_time - start_time).total_seconds()))



# -*-coding:utf-8
# author:lihaizhen
# date:
# description:
import datetime, time
import decimal
import json
import numpy as np
import math
import paramiko
import itertools
import requests
from reco.utils.connect import BQ_Client
import platform
import csv,os
from collections import Counter
from itertools import chain

bq_client = BQ_Client()
create_time = time.strftime('%Y-%m-%d', time.localtime(time.time()))




print('start 1')



s_time = (datetime.datetime.now() - datetime.timedelta(days=15)).strftime("%Y-%m-%d").replace("-", '')
e_time = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y-%m-%d").replace("-", '')
sql = """
-- 产品id、标题对照表  -- 用v2ProductName1
-- 若一个产品id对应多个标题，则取时间最近的那个标题
select productSKU ,v2ProductName
,trim(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(lower(v2ProductName),'+',''),
'unice',''),'hair',''),'s',''),'blonde','blond'),'frontal','front'),'curly','curl'),'*','x'),'www',''),'com',''),'.','')) as v2ProductName1 from
(
select productSKU ,v2ProductName,row_number() OVER (PARTITION BY productSKU ORDER BY cast(date as INT64) desc) as sort
from
( 
select distinct c.productSKU ,c.v2ProductName,date
from `truemetrics-164606.116719673.ga_sessions_*`
left join unnest(hits) as b
left join unnest(product) as c
where   _TABLE_SUFFIX BETWEEN '{}' AND '{}' and geoNetwork.region not in('Shaanxi')
      and REGEXP_CONTAINS(b.eventInfo.eventCategory,r"(?i)productDetail|(?i)otherPage")
      and REGEXP_CONTAINS(b.eventInfo.eventAction,r"(?i)pageLoading") and REGEXP_CONTAINS(b.eventInfo.eventLabel,r"(?i)productDetail")
and c.productSKU <> "(not set)" and c.productSKU is not null and  REGEXP_CONTAINS(b.page.hostname,r"(?i)unice.com")
) t 
) m where sort=1
""".format(s_time, e_time)
print('query....')
bqListRet = bq_client.query(sql)
print('turn to list....')
bqListRet = [list(i) for i in bqListRet]


# localhost;127.*;10.*;172.16.*;172.17.*;172.18.*;172.19.*;172.20.*;172.21.*;172.22.*;172.23.*;172.24.*;172.25.*;172.26.*;172.27.*;172.28.*;172.29.*;172.30.*;172.31.*;192.168.*

# Deadline of 600.0s exceeded while calling target function, last exception: HTTPSConnectionPool(host='oauth2.googleapis.com', port=443): Max retries exceeded with url: /token (Caused by ProxyError('Cannot connect to proxy.', timeout('_ssl.c:1112: The handshake operation timed out')))
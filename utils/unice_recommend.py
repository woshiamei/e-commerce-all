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
from connect import BQ_Client
import platform
import csv,os
from collections import Counter
from itertools import chain


bq_client = BQ_Client()
create_time = time.strftime('%Y-%m-%d', time.localtime(time.time()))


class Product_id_title_1(object):
    """it takes about 7 seconds to run"""
    def __init__(self):
        print('start 1')
        self.db = ''

    def get_goods_title(self):
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
        print('total prod`uct num is {}'.format(len(bqListRet)))
        self.db.truncate('gess_u_like_product_id_title_mapping')
        print('clear table gess_u_like_product_id_title_mapping')
        base_sql = 'insert into gess_u_like_product_id_title_mapping (product_id,title) VALUES {}'
        datas = []
        for i, result in enumerate(bqListRet):
            product_id = result[0]
            title = result[2]
            datas.append((product_id, title))
            if i % 200 == 0 and i != 0:
                sql = base_sql.format(','.join(str(item) for item in datas))
                self.db.insert(sql)
                datas = []
        sql = base_sql.format(','.join(str(item) for item in datas))
        self.db.insert(sql)

    def run(self):
        self.get_goods_title()
        t = self.db.get_one('select DATE_FORMAT(create_time,"%Y-%m-%d") from gess_u_like_product_id_title_mapping limit 2')
        if t:
            if t[0] == create_time:
                print('finish 1\n')
                return
        self.get_goods_title()
        print('finish 1\n')


class Get_goods_score_into_csv_2(object):
    """it takes about 198 seconds to run"""
    def __init__(self):
        print('start 2')
        if (platform.system() == 'Windows'):
            self.local_file = r'F:\spider\推荐算法--BQ数据处理\gess_u_like_goods_score_source\{}.csv'
        elif (platform.system() == 'Linux'):
            self.local_file = r'/home/unice_recommend_v2/csv/gess_u_like_goods_score_source/{}.csv'
        else:
            raise Exception

    def get_goods_score_into_csv(self):
        s_time = (datetime.datetime.now() - datetime.timedelta(days=15)).strftime("%Y-%m-%d").replace("-", '')
        e_time = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y-%m-%d").replace("-", '')
        sql = """
           -- 用户行为物品相似度数据源  
-- 存入数据库的s_bq_user_like_days_v2表
select * 
from
(
  select clientid,c.productSKU,
      case 
        when sum(case when b.transaction.transactionId is not null and c.productRevenue/1000000>20 then 1 else 0 end)>0 then 4 
        when sum(case when b.eCommerceAction.action_type='3' then 1 else 0 end)>0 then 3
        when sum(case when REGEXP_CONTAINS(b.eventInfo.eventAction,r"(?i)addToWishlist") then 1 else 0 end)>0 then 2
        when sum(case when REGEXP_CONTAINS(b.eventInfo.eventAction,r"(?i)pageLoading") 
            and REGEXP_CONTAINS(b.eventInfo.eventLabel,r"(?i)productDetail") then 1 else 0 end)>0 then 1  
        when sum(case when REGEXP_CONTAINS(b.eventInfo.eventAction,r"(?i)productView") 
                  and REGEXP_CONTAINS(b.eventInfo.eventCategory,r"(?i)productList|(?i)homePage|(?i)productDetail") then 1 else 0 end)>0 
          and sum(case when REGEXP_CONTAINS(b.eventInfo.eventAction,r"(?i)pageLoading") 
                  and REGEXP_CONTAINS(b.eventInfo.eventLabel,r"(?i)productDetail") then 1 else 0 end)=0 then -1
        else 0 end as user_product_matrix_value
  from `truemetrics-164606.116719673.ga_sessions_*`
  left join unnest(hits) as b
  left join unnest(product) as c
  where   _TABLE_SUFFIX BETWEEN '{}' AND '{}' and (geoNetwork.country not in('China') or geoNetwork.region not in('Shaanxi'))
  and c.productSKU <> "(not set)" and c.productSKU is not null 
  group by clientid,c.productSKU  
) t where user_product_matrix_value!=0
           """.format(s_time, e_time)
        print('query....')
        bqListRet = bq_client.query(sql)
        print('turn to list....')
        bqListRet = [list(i) for i in bqListRet]
        print('write csv....')
        f = open(self.local_file.format(create_time), 'w', newline='')
        writer = csv.writer(f)
        [writer.writerow(Item) for Item in bqListRet]
        f.close()

    def delete_yesterday_data(self):
        e_time = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        try:
            os.remove(self.local_file.format(e_time))
        except Exception as e:
            pass

    def run(self):
        if not os.path.exists(self.local_file.format(create_time)):
            self.delete_yesterday_data()
            self.get_goods_score_into_csv()
        print('finish 2\n')


class Get_goods_similar_simple_3(object):
    """it takes about 1283 seconds to run 34w"""
    def __init__(self):
        print('start 3')
        self.db = Connect()
        if (platform.system() == 'Windows'):
            self.local_file = r'F:\spider\推荐算法--BQ数据处理\gess_u_like_goods_score_source\{}.csv'
        elif (platform.system() == 'Linux'):
            self.local_file = r'/home/unice_recommend_v2/csv/gess_u_like_goods_score_source/{}.csv'
        else:
            raise Exception

    def get_distinct_product_id_list(self):
        with open(self.local_file.format(create_time),'r')as f:
            result = f.readlines()
        product_id_list = set([record.split(',')[1] for record in result])
        return product_id_list

    def get_user_product_list(self):
        with open(self.local_file.format(create_time),'r')as f:
            result = f.readlines()
        user_product_dict = {}
        for record in result:
            rec = record.split(',')
            if rec[1] in user_product_dict:
                user_product_dict[rec[1]].append(rec[0])
            else:
                user_product_dict[rec[1]] = []
        return user_product_dict

    def get_parms(self):
        self.db.truncate('gess_u_like_goods_similarity')
        print("clear table gess_u_like_goods_similarity")
        user_product = self.get_user_product_list()
        product_list = self.get_distinct_product_id_list()
        return product_list,user_product

    def cosine_similarity(self,product1,product2,norm =False):
        """ 计算两个product1和product2的余弦相似度 """
        if not product1 or not product2:
            return 0
        sum1 = len(product1)
        sum2 = len(product2)
        # common = len([val for val in product1 if val in product2])
        common = len(list(set(product1).intersection(product2)))
        denominator = math.sqrt(sum1) * math.sqrt(sum2)
        cos = common / denominator
        return cos

    def pailie_zuhe(self,list1):
        list2 = []
        # iter = itertools.combinations(list1, 2)
        iter = itertools.permutations(list1, 2)
        list2.append(list(iter))
        return list2[0]

    def cycle_product(self):
        s_time = time.time()
        product_list,user_product =self.get_parms()
        list2 = self.pailie_zuhe(product_list)
        print('total goods similar num is {}'.format(len(list2)))
        datas,product_list = [],[]
        base_sql = 'insert into gess_u_like_goods_similarity (product1,product2,cos) VALUES {}'
        t1 = time.time()
        for i,record in enumerate(list2):
            product1 = user_product[record[0]]
            product2 = user_product[record[1]]
            cos= self.cosine_similarity(product1,product2)
            datas.append((record[0],record[1],round(cos,4)))
            if i % 800 == 0 and i != 0:
                t2 = time.time()
                # print('insert---{}---{}s'.format(i,t2-t1))
                sql = base_sql.format(','.join(str(item) for item in datas))
                self.db.insert(sql)
                datas = []
                t1 = time.time()
        sql = base_sql.format(','.join(str(item) for item in datas))
        self.db.insert(sql)
        e_time = time.time()
        print(e_time-s_time)

    def delete_yesterday_data(self):
        e_time = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        try:
            os.remove(self.local_file.format(e_time))
        except Exception as e:
            pass

    def run(self):
        t = self.db.get_one('select DATE_FORMAT(create_time,"%Y-%m-%d") from gess_u_like_goods_similarity limit 2')
        if t:
            if t[0] == create_time:
                print('pass 3\n')
                return
        self.delete_yesterday_data()
        self.cycle_product()
        print('finish 3\n')


class Get_browse_data_4(object):
    """it takes about 278 seconds to run 97w"""
    def __init__(self):
        print('start 4')
        self.db = Connect()

    def get_browse_data(self):
        self.db.truncate('gess_u_like_browse_data_source')
        print('clear table gess_u_like_browse_data_source')
        stime = (datetime.datetime.now() - datetime.timedelta(days=15)).strftime("%Y-%m-%d").replace("-", '')
        etime = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y-%m-%d").replace("-", '')
        e_time = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
        sql = """
        -- 用户id、产品id、平均浏览时长、最近一次查看距离现在的时间
        with t1 as
        (  #t1:浏览产品详情的会话
          select date,clientid,visitId,visitstarttime,b.hitnumber,b.time,b.page.pagePath, b.eventInfo.eventCategory
          , b.eventInfo.eventAction, b.eventInfo.eventLabel, c.productSKU ,c.v2ProductName
          from `truemetrics-164606.116719673.ga_sessions_*`
          left join unnest(hits) as b
          left join unnest(product) as c
          where   _TABLE_SUFFIX BETWEEN '{}' AND '{}' and geoNetwork.region not in('Shaanxi')
                and REGEXP_CONTAINS(b.eventInfo.eventCategory,r"(?i)productDetail|(?i)otherPage")
                and REGEXP_CONTAINS(b.eventInfo.eventAction,r"(?i)pageLoading") and REGEXP_CONTAINS(b.eventInfo.eventLabel,r"(?i)productDetail")
         and c.productSKU <> "(not set)" and c.productSKU is not null and  REGEXP_CONTAINS(b.page.hostname,r"(?i)unice.com")
          group by date,clientid,visitId,visitstarttime,b.hitnumber,b.time,b.page.pagePath, b.eventInfo.eventCategory, b.eventInfo.eventAction, b.eventInfo.eventLabel, c.productSKU ,c.v2ProductName
          order by date,clientid,visitId,visitstarttime,b.hitnumber
        ),
        t2 as
        (#type='PAGE'的会话
          select clientid,visitId,visitstarttime,b.hitnumber,b.time,b.page.pagePath
          from `truemetrics-164606.116719673.ga_sessions_*`
          left join unnest(hits) as b
          left join unnest(product) as c
          where   _TABLE_SUFFIX BETWEEN '{}' AND '{}' and geoNetwork.region not in('Shaanxi')
                and b.type='PAGE'
          group by clientid,visitId,visitstarttime,b.hitnumber,b.time,b.page.pagePath
          order by clientid,visitId,visitstarttime,b.hitnumber,b.time
        ),
        t3 as
        (   #每次浏览产品详情页的时间
          select t1.date,t1.clientid,t1.visitId,t1.visitstarttime,t1.hitnumber
          ,case when t.time_next is null then 1 else (t.time_next-t1.time)/1000 end as time_gap,t1.pagePath,t1.productSKU
          from t1
          left join
           (
            select t1.clientid,t1.visitid,t1.visitstarttime,t1.hitnumber,t1.productSKU,t1.v2ProductName,min(t2.time) as time_next
            from t1
            left join t2 on t1.clientid=t2.clientid and t1.visitid=t2.visitid and t1.visitstarttime=t2.visitstarttime and t2.hitnumber>t1.hitnumber and t1.pagepath<>t2.pagepath
            group by t1.clientid,t1.visitid,t1.visitstarttime,t1.hitnumber,t1.productSKU,t1.v2ProductName
            ) as t on t1.clientid=t.clientid and t1.visitid=t.visitid and t1.visitstarttime=t.visitstarttime and t1.hitnumber=t.hitnumber
        )

        -- 用户id、产品id、总浏览时长、最近一次查看距离现在的时间
        select clientid,productSKU,sum(time_gap) as product_view_time
        ,date_diff(DATE '{}',max(date(cast(substr(date,1,4) as int64),cast(substr(date,5,2) as int64),cast(substr(date,7,2) as int64))),DAY) as last_view_day_gap
        from t3
        where productSKU is not null and productSKU <>'(not set)'
        group by clientid,productSKU
        """.format(stime, etime, stime, etime, e_time)
        bq_list = bq_client.query(sql)
        print('total browse data num is {}'.format(len(bq_list)))
        base_sql = 'insert into gess_u_like_browse_data_source (client_id,product_id,product_view_time,last_view_day_gap) VALUES {}'
        datas = []
        s_t = time.time()
        for i, result in enumerate(bq_list):
            client_id = result[0]
            product_id = result[1]
            product_view_time = result[2]
            last_view_day_gap = result[3]
            datas.append((client_id, product_id, product_view_time, last_view_day_gap))
            if i % 300 == 0 and i != 0:
                e_t = time.time()
                sql = base_sql.format(','.join(str(item) for item in datas))
                self.db.insert(sql)
                # print('insert--{}--{}s'.format(i, e_t - s_t))
                datas = []
        sql = base_sql.format(','.join(str(item) for item in datas))
        self.db.insert(sql)

    def run(self):
        t = self.db.get_one('select DATE_FORMAT(create_time,"%Y-%m-%d") from gess_u_like_browse_data_source limit 2')
        if t:
            if t[0] == create_time:
                print('pass 4\n')
                return
        self.get_browse_data()
        print('finish 4\n')


class Get_search_data_5(object):
    """it takes about 15 seconds to run 12w"""
    def __init__(self):
        print('start 5')
        self.db = Connect()

    def get_search_data(self):
        self.db.truncate('gess_u_like_search_data_source')
        print('clear table gess_u_like_search_data_source')
        s_time = (datetime.datetime.now() - datetime.timedelta(days=15)).strftime("%Y-%m-%d").replace("-", '')
        e_time = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y-%m-%d").replace("-", '')
        sql = """
         -- 用户id、搜索关键字|站内搜索词
    -- 搜索引擎用户搜索字词对应的关键字
    select distinct clientid
    ,trim(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(lower(trafficSource.keyword),'+',''),'unice',''),'hair',''),'s',''),'blonde','blond'),'frontal','front'),'curly','curl'),'*','x'),'www',''),'com',''),'.','')) as keyword
    from `truemetrics-164606.116719673.ga_sessions_*` 
    left join unnest(hits) as b
    WHERE  _TABLE_SUFFIX BETWEEN '{}' AND '{}' 
    and geoNetwork.region not in('Shaanxi') 
    and 
    (
      (REGEXP_CONTAINS(trafficSource.source,r"(?i)google|(?i)bing") 
      and not REGEXP_CONTAINS(trafficSource.campaign,r"(?i)shopping|(?i)动态搜索|(?i)Display|销售|(?i)remarketing|展示再营销|动态再营销|(?i)drm|(?i)Undetermined|(?i)discover|网站流量"))
      or REGEXP_CONTAINS(channelgrouping,r"(?i)organic search")
    )
    and trafficSource.keyword not in ('(not provided)','(automatic matching)','(not set)'
    ,'Dynamic Search Ads','(Remarketing/Content targeting)','(automatic matching)','(content targeting)')
    and trafficSource.keyword is not null
    and length(trim(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(lower(trafficSource.keyword),'+',''),'unice',''),'hair',''),'s',''),'blonde','blond'),'frontal','front'),'curly','curl'),'*','x'),'www',''),'com',''),'.','')))>2
    union distinct
    -- 站内搜索词
    select  distinct clientid
    ,trim(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(lower(b.page.searchKeyword),'+',''),'unice',''),'hair',''),'s',''),'blonde','blond'),'frontal','front'),'curly','curl'),'*','x'),'www',''),'com',''),'.','')) as keyword
    from `truemetrics-164606.116719673.ga_sessions_*` 
    left join unnest(hits) as b
    WHERE  _TABLE_SUFFIX BETWEEN '{}' AND '{}' 
    and geoNetwork.region not in('Shaanxi') 
    and b.page.searchKeyword is not null and length(trim(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(lower(b.page.searchKeyword),'+',''),'unice',''),'hair',''),'s',''),'blonde','blond'),'frontal','front'),'curly','curl'),'*','x'),'www',''),'com',''),'.','')))>1
        """.format(s_time, e_time, s_time, e_time)
        bq_list = bq_client.query(sql)
        print('search data num is {}'.format(len(bq_list)))
        base_sql = 'insert into gess_u_like_search_data_source (client_id,keywords) VALUES {}'
        datas = []
        t1 = time.time()
        for i, result in enumerate(bq_list):
            product_id = result[0]
            title = result[1]
            datas.append((product_id, title))
            if i % 500 == 0 and i != 0:
                t2 = time.time()
                # print('5-insert--{}--{}s'.format(i, t2 - t1))
                sql = base_sql.format(','.join(str(item) for item in datas))
                self.db.insert(sql)
                datas = []
        sql = base_sql.format(','.join(str(item) for item in datas))
        self.db.insert(sql)

    def run(self):
        t = self.db.get_one('select DATE_FORMAT(create_time,"%Y-%m-%d") from gess_u_like_search_data_source limit 2')
        if t:
            if t[0] == create_time:
                print('pass 5\n')
                return
        self.get_search_data()
        print('finish 5\n')


class Get_add_purchase_data_source_6(object):
    """it takes about 11 seconds to run 6.5w"""
    def __init__(self):
        print('start 6')
        self.db = Connect()

    def get_add_purchase_data(self):
        self.db.truncate('gess_u_like_add_purchase_data_source')
        print('clear table gess_u_like_add_purchase_data_source')
        stime = (datetime.datetime.now() - datetime.timedelta(days=15)).strftime("%Y-%m-%d").replace("-", '')
        etime = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y-%m-%d").replace("-", '')
        e_time = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
        sql = """
-- 加购数据源
-- 加购用户id、加购产品id、对应的最近一次加购距离现在的时间
select clientid,productSKU
  ,date_diff(DATE '{}',max(date(cast(substr(date,1,4) as int64),cast(substr(date,5,2) as int64),cast(substr(date,7,2) as int64))),DAY) as last_cart_day_gap
  from `truemetrics-164606.116719673.ga_sessions_*`
  left join unnest(hits) as b
  left join unnest(product) as c
  where   _TABLE_SUFFIX BETWEEN '{}' AND '{}' and geoNetwork.region not in('Shaanxi')
         and b.eCommerceAction.action_type='3'
 and c.productSKU <> "(not set)" and c.productSKU is not null 
group by clientid,productSKU

         """.format(e_time, stime, etime)
        bq_list = bq_client.query(sql)
        print('add purchase data num is {}'.format(len(bq_list)))
        base_sql = 'insert into gess_u_like_add_purchase_data_source (client_id,product_id,last_cart_day_gap) VALUES {}'
        datas = []
        for i, result in enumerate(bq_list):
            client_id = result[0]
            product_id = result[1]
            last_cart_day_gap = result[2]
            datas.append((client_id, product_id, last_cart_day_gap))
            if i % 500 == 0 and i != 0:
                sql = base_sql.format(','.join(str(item) for item in datas))
                self.db.insert(sql)
                # print(i)
                datas = []
        sql = base_sql.format(','.join(str(item) for item in datas))
        self.db.insert(sql)

    def run(self):
        t = self.db.get_one('select DATE_FORMAT(create_time,"%Y-%m-%d") from gess_u_like_add_purchase_data_source limit 2')
        if t:
            if t[0] == create_time:
                print('pass 6\n')
                return
        self.get_add_purchase_data()
        print('finish 6\n')


class Get_purchase_data_source_7(object):
    """it takes about 6 seconds to run 1w"""
    def __init__(self):
        print('start 7')
        self.db = Connect()

    def get_purchase_data(self):
        self.db.truncate('gess_u_like_purchase_data_source')
        print('clear table gess_u_like_purchase_data_source')

        stime = (datetime.datetime.now() - datetime.timedelta(days=15)).strftime("%Y-%m-%d").replace("-", '')
        etime = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y-%m-%d").replace("-", '')
        e_time = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
        sql = """
    -- 购买数据源
    -- 购买用户id、购买产品id、对应的最近一次购买距离现在的时间
    select clientid,productSKU
      ,date_diff(DATE '{}',max(date(cast(substr(date,1,4) as int64),cast(substr(date,5,2) as int64),cast(substr(date,7,2) as int64))),DAY) as last_order_day_gap
      from `truemetrics-164606.116719673.ga_sessions_*`
      left join unnest(hits) as b
      left join unnest(product) as c
      where   _TABLE_SUFFIX BETWEEN '{}' AND '{}' and geoNetwork.region not in('Shaanxi')
             and b.transaction.transactionId is not null and c.productRevenue/1000000>20
     and c.productSKU <> "(not set)" and c.productSKU is not null 
    group by clientid,productSKU
    """.format(e_time, stime, etime)
        bq_list = bq_client.query(sql)
        print('purchase num is {}'.format(len(bq_list)))
        base_sql = 'insert into gess_u_like_purchase_data_source (client_id,product_id,last_order_day_gap) VALUES {}'
        datas = []
        for i, result in enumerate(bq_list):
            client_id = result[0]
            product_id = result[1]
            last_order_day_gap = result[2]
            datas.append((client_id, product_id, last_order_day_gap))
            if i % 500 == 0 and i != 0:
                sql = base_sql.format(','.join(str(item) for item in datas))
                self.db.insert(sql)
                # print(i)
                datas = []
        sql = base_sql.format(','.join(str(item) for item in datas))
        self.db.insert(sql)

    def run(self):
        t = self.db.get_one('select DATE_FORMAT(create_time,"%Y-%m-%d") from gess_u_like_purchase_data_source limit 2')
        if t:
            if t[0] == create_time:
                print('pass 7\n')
                return
        self.get_purchase_data()
        print('finish 7\n')


class Count_each_part_score_8(object):
    """it takes about 3600 seconds to run"""
    def __init__(self):
        print('start 8')
        self.db = Connect()
        if (platform.system() == 'Windows'):
            self.local_file = r'F:\spider\推荐算法--BQ数据处理\{}\{}.csv'
        elif (platform.system() == 'Linux'):
            self.local_file = r'/home/unice_recommend_v2/csv/{}/{}.csv'
        else:
            raise Exception

    def get_total_view_avg(self):
        sql = """SELECT AVG(product_view_time) AS total_view_avg,AVG(14-last_view_day_gap) AS last_view_avg FROM gess_u_like_browse_data_source;"""
        results = self.db.select(sql)
        return results

    def get_browse_data(self):
        sql = """select client_id,product_id,product_view_time,last_view_day_gap from gess_u_like_browse_data_source;"""
        results = self.db.select(sql)
        return results

    def get_product_title_data(self):
        sql = """select product_id,title from gess_u_like_product_id_title_mapping;"""
        results = self.db.select(sql)
        return results

    def get_add_purchase_avg(self):
        sql = """select avg(14-last_cart_day_gap) from gess_u_like_add_purchase_data_source;"""
        results = self.db.select(sql)
        return results

    def get_add_purchase_data(self):
        sql = """select client_id,product_id,last_cart_day_gap from gess_u_like_add_purchase_data_source;"""
        results = self.db.select(sql)
        return results

    def get_purchase_avg(self):
        sql = """select avg(14-last_order_day_gap) from gess_u_like_purchase_data_source;"""
        results = self.db.select(sql)
        return results

    def get_purchase_data(self):
        sql = """select client_id,product_id,last_order_day_gap from gess_u_like_purchase_data_source;"""
        results = self.db.select(sql)
        return results

    def get_z_score(self,data, avg_data, std):
        """数据进行z_score标准化处理，新数据=（原数据-均值）/标准差，新数据与权重相乘后再求和"""
        var = (data - avg_data) / decimal.Decimal(std)
        return var

    # 计算 搜索词 Vs 标题 的相似度
    def title_vs_search_repeat(self,search, title):
        search_list = set(search.upper().split(' '))
        if '' in search_list:
            search_list.remove('')
        words_num = len(search_list)
        title_list = set(title.upper().split(' '))
        list_all = []
        list_all.extend(search_list)
        list_all.extend(title_list)
        dic = dict(Counter(list_all))
        repeat_num = 0
        for s in search_list:
            if dic[s] == 2:
                repeat_num += 1
        similarity = repeat_num / words_num
        return round(similarity, 4)

    def get_clientid_keyword_dic(self):
        search_results = self.db.select(
            """SELECT client_id,keywords FROM gess_u_like_search_data_source order BY client_id;""")
        dic = {}
        for i, s_r in enumerate(search_results):
            client_id = s_r[0]
            keyword = s_r[1]
            if client_id not in dic.keys():
                dic[client_id] = {}
                dic[client_id]['keyword'] = [keyword]
            else:
                dic[client_id]['keyword'].append(keyword)
        return dic

    # 计算、合并搜索词相似度
    def count_search_similarity(self):
        if os.path.exists(self.local_file.format('gess_u_like_search_similarity', create_time)):
            return
        title_results = self.get_product_title_data()  # 产品集合
        search_results = self.get_clientid_keyword_dic()
        data_list = []
        f = open(self.local_file.format('gess_u_like_search_similarity', create_time), 'w', newline='', encoding='utf-8')
        writer = csv.writer(f)
        for j, product in enumerate(title_results):  # 同一产品
            for client, keyword_list_dic in search_results.items():
                total_similarity = 0
                total_keyword = ''
                for keyword in keyword_list_dic['keyword']:
                    similarity = self.title_vs_search_repeat(keyword, product[1])
                    total_keyword += keyword
                    total_similarity += similarity
                data_list.append([client, product[0], total_similarity])
            if j % 100 == 0:
                # print('write csv....')
                [writer.writerow(data) for data in data_list]
                data_list = []
        [writer.writerow(data) for data in data_list]
        title_results, search_results, data_list = None, None, None
        f.close()

    # 1-计算浏览总时长得分（权重20%）
    def count_view_score(self):
        if not os.path.exists(self.local_file.format('gess_u_like_browse_score',create_time)):
            results = self.get_browse_data()  # 均值[总浏览时长、浏览间隔天数]
            avg_results = self.get_total_view_avg()
            total_view_std = np.std([i[2] for i in results], ddof=1)  # 总浏览时长标准差
            last_view_std = np.std([i[3] for i in results], ddof=1)  # 浏览间隔天数标准差
            datas = []
            f = open(self.local_file.format('gess_u_like_browse_score', create_time), 'w', newline='', encoding='utf-8')
            writer = csv.writer(f)
            t1 = time.time()
            for i, result in enumerate(results):
                client_id = result[0]
                product_id = result[1]
                product_view_time = result[2]
                last_view_day_gap = result[3]
                total_view_score = self.get_z_score(product_view_time, avg_results[0][0], total_view_std) * decimal.Decimal(
                    0.6)  # 总浏览时长得分
                last_view_day_score = self.get_z_score(last_view_day_gap, avg_results[0][1], last_view_std) * decimal.Decimal(
                    0.4)  # 浏览间隔天数得分
                view_score = round(float((total_view_score + last_view_day_score) * decimal.Decimal(0.2)), 4)  # 浏览时长zong得分
                datas.append((client_id, product_id, view_score))
                if i % 500000 == 0 and i != 0:
                    t2 = time.time()
                    # print('8-write csv--{}--{}'.format(i, t2 - t1))
                    [writer.writerow(data) for data in datas]
                    datas = []
                    t1 = time.time()
            print('browse score num is {}'.format(len(results)))
            [writer.writerow(data) for data in datas]
            f.close()
        print('view score flying to 231....')
        remote_path = '/home/unice_recommend_v2/csv/gess_u_like_browse_score/'
        self.db.ssh_scp_put(self.local_file.format('gess_u_like_browse_score', create_time), remote_path)

    # 2-计算搜索词得分（权重20%）
    def count_search_score(self):
        if not os.path.exists(self.local_file.format('gess_u_like_search_score',create_time)):
            with open(self.local_file.format('gess_u_like_search_similarity', create_time), 'r')as f:
                results = f.readlines()
            avg_results = [float(av.split(',')[2].replace('\n', '')) for av in results]
            avg = decimal.Decimal(np.mean(avg_results))  # 均值
            search_std = np.std(avg_results, ddof=1)  # 标准差
            avg_results = None
            datas = []
            f = open(self.local_file.format('gess_u_like_search_score', create_time), 'w', newline='', encoding='utf-8')
            writer = csv.writer(f)
            t1 = time.time()
            for i, res in enumerate(results):
                result = res.split(',')
                client_id = result[0]
                product_id = result[1]
                search_similarity = decimal.Decimal(result[2].replace('\n', ''))
                search_score = self.get_z_score(search_similarity, avg, search_std)  # 搜索得分
                score = round(float(search_score), 4) * 0.2
                datas.append((client_id, product_id, score))
                if i % 1000000 == 0 and i != 0:
                    t2 = time.time()
                    # print('8-write csv--{}--{}'.format(i, t2 - t1))
                    [writer.writerow(data) for data in datas]
                    datas = []
                    t1 = time.time()
            [writer.writerow(data) for data in datas]
            print('search score num is {}'.format(len(results)))
            results = None
            f.close()
            datas = None
        print('search score flying to 231....')
        remote_path = '/home/unice_recommend_v2/csv/gess_u_like_search_score/'
        self.db.ssh_scp_put(self.local_file.format('gess_u_like_search_score', create_time), remote_path)

    # 3-计算加购得分（权重30%）
    def count_add_purchase_score(self):
        if not os.path.exists(self.local_file.format('gess_u_like_add_purchase_score',create_time)):
            results = self.get_add_purchase_data()
            avg_results = self.get_add_purchase_avg()  # 均值[14-加购间隔天数]
            add_purchase_std = np.std([i[2] for i in results], ddof=1)  # [14-加购间隔天数]标准差
            datas = []
            f = open(self.local_file.format('gess_u_like_add_purchase_score', create_time), 'w', newline='', encoding='utf-8')
            writer = csv.writer(f)
            for i, result in enumerate(results):
                client_id = result[0]
                product_id = result[1]
                last_cart_day_gap = result[2]
                is_cart_score = decimal.Decimal(0.6 * 1)
                last_cart_day_score = self.get_z_score(14 - last_cart_day_gap, avg_results[0][0],
                                                  add_purchase_std) * decimal.Decimal(0.4)  # 14-加购间隔天数 得分
                score = round(float((is_cart_score + last_cart_day_score) * decimal.Decimal(0.3)), 4)  # 加购得分
                datas.append((client_id, product_id, score))
            [writer.writerow(data) for data in datas]
            print('add_purchase score num is {}'.format(len(results)))
            f.close()
        print('add_purchase score flying to 231....')
        remote_path = '/home/unice_recommend_v2/csv/gess_u_like_add_purchase_score/'
        self.db.ssh_scp_put(self.local_file.format('gess_u_like_add_purchase_score', create_time), remote_path)

    # 4-计算购买得分（权重30%）
    def count_purchase_score(self):
        if not os.path.exists(self.local_file.format('gess_u_like_purchase_score',create_time)):
            results = self.get_purchase_data()
            avg_results = self.get_purchase_avg()  # 均值[14-购买间隔天数]
            purchase_std = np.std([14 - i[2] for i in results], ddof=1)  # [14-购买间隔天数]标准差
            is_order_score = decimal.Decimal(0.6 * 1)
            avg = avg_results[0][0]
            datas = []
            f = open(self.local_file.format('gess_u_like_purchase_score', create_time), 'w', newline='', encoding='utf-8')
            writer = csv.writer(f)
            for i, result in enumerate(results):
                client_id = result[0]
                product_id = result[1]
                last_order_day_gap = result[2]
                last_order_day_score = self.get_z_score(14 - last_order_day_gap, avg, purchase_std) * decimal.Decimal(
                    0.4)  # [14-购买间隔天数]得分
                score = round(float((is_order_score + last_order_day_score) * decimal.Decimal(0.3)), 4)  # 加购得分
                datas.append((client_id, product_id, score))
            [writer.writerow(data) for data in datas]
            print('purchase score num is {}'.format(len(results)))
            f.close()
        print('purchase score flying to 231....')
        remote_path = '/home/unice_recommend_v2/csv/gess_u_like_purchase_score/'
        self.db.ssh_scp_put(self.local_file.format('gess_u_like_purchase_score', create_time), remote_path)

    def delete_yesterday_data(self,file):
        try:
            os.remove(file)
        except Exception as e:
            pass

    def run(self):
        yes_time = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        s_time = time.time()
        self.count_view_score()
        e_time1 = time.time()
        print('browse score cost time:{}'.format(e_time1 - s_time)) # 76

        self.count_search_similarity()
        e_time2 = time.time()
        print('search similar cost time:{}'.format(e_time2 - e_time1))

        self.count_search_score()
        e_time3 = time.time()
        print('search score cost time:{}'.format(e_time3 - e_time2))

        self.count_add_purchase_score()
        e_time4 = time.time()
        print('add score cost time:{}'.format(e_time4 - e_time3))

        self.count_purchase_score()
        e_time5 = time.time()
        print('purchase score cost time:{}'.format(e_time5 - e_time4))
        e_time = time.time()
        print('total score cost time:{}'.format(e_time - s_time))

        self.delete_yesterday_data(self.local_file.format('gess_u_like_browse_score', yes_time))
        self.delete_yesterday_data(self.local_file.format('gess_u_like_search_similarity', yes_time))
        self.delete_yesterday_data(self.local_file.format('gess_u_like_search_score', yes_time))
        self.delete_yesterday_data(self.local_file.format('gess_u_like_add_purchase_score', yes_time))
        self.delete_yesterday_data(self.local_file.format('gess_u_like_purchase_score', yes_time))
        print('remove five part score csv')
        print('finish 8\n')


class Control_Remote_csv_into_hive_9(object):
    def __init__(self):
        print('start 9')
        self.ssh1 = paramiko.SSHClient()
        self.key = paramiko.AutoAddPolicy()
        self.host = '192.168.1.231'
        self.port = 22
        self.user = 'root'
        self.pwd = '12348765'

    def setup_remote_sh_231(self):
        self.ssh1.set_missing_host_key_policy(self.key)
        self.ssh1.connect(hostname=self.host, port=22, username=self.user,password=self.pwd, timeout=5)
        stdin1, stdout1, stderr1 = self.ssh1.exec_command('sh /home/unice_recommend_v2/csv/gess_u_like_add_purchase_score.sh')
        print(stdout1.read())
        print(stdout1.read())
        print(stdout1.read())
        stdin2, stdout2, stderr2 = self.ssh1.exec_command('sh /home/unice_recommend_v2/csv/gess_u_like_browse_score.sh')
        print(stdout2.read())
        print(stdout2.read())
        print(stdout2.read())
        stdin3, stdout3, stderr3 = self.ssh1.exec_command('sh /home/unice_recommend_v2/csv/gess_u_like_purchase_score.sh')
        print(stdout3.read())
        print(stdout3.read())
        print(stdout3.read())
        stdin4, stdout4, stderr4 = self.ssh1.exec_command('sh /home/unice_recommend_v2/csv/gess_u_like_search_score.sh')
        print(stdout4.read())
        print(stdout4.read())
        print(stdout4.read())
        self.ssh1.close()

    def run(self):
        self.setup_remote_sh_231()
        print('finish 9')


class Count_top_by_hive_10(object):
    def __init__(self):
        print('start 10')
        self.db = Connect()
        self.hdb = Hive_Connect()

    def get_user_id_match_client_id(self):
        stime = (datetime.datetime.now() - datetime.timedelta(days=15)).strftime("%Y-%m-%d").replace("-", '')
        etime = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y-%m-%d").replace("-", '')
        bsql = """
            select clientid,userid
            from 
              (
              select clientid,visitstarttime,u.value as userid,row_number()over(partition by clientid order by visitstarttime desc) as rank1
              from `truemetrics-164606.116719673.ga_sessions_*`,unnest(customDimensions) as u 
              where _TABLE_SUFFIX BETWEEN '{}' AND '{}'
              and u.index=10
              group by clientid,visitstarttime,u.value
              having u.value <> "" and u.value <> '0'
            )
            where rank1=1;
        """.format(stime,etime)
        print('query....')
        bqListRet = bq_client.query(bsql)
        print('turn to list....')
        cu_results = [list(i) for i in bqListRet]
        return cu_results

    def get_user_top3_product(self):  # cost 120 seconds
        t = self.db.get_one('select DATE_FORMAT(create_time,"%Y-%m-%d") from gess_u_like_user_top3 limit 2')
        if t:
            if t[0] == create_time:
                return
        s_time = time.time()
        self.db.truncate('gess_u_like_user_top3')
        print("clear table gess_u_like_user_top3")
        score_result = self.hdb.get_all("""
        with t1 as 
(
SELECT client_id,product_id,score
from jdm.gess_u_like_add_purchase_score
UNION  all
SELECT client_id,product_id,score
from jdm.gess_u_like_browse_score
UNION  all
SELECT client_id,product_id,score
from jdm.gess_u_like_purchase_score
UNION  all
SELECT client_id,product_id,score
from jdm.gess_u_like_search_score
),
t2 as
(
select client_id,product_id,sum(score) as score
from t1
group by client_id,product_id
),
t3 as
(
    SELECT client_id,product_id,score,
    row_number()over(PARTITION BY client_id ORDER BY score DESC) as rank_desc
    from t2
),
t4 as 
(
	select client_id,product_id
	from t3
	WHERE rank_desc <=3
)
select client_id,group_concat(product_id)
from t4
group by client_id


    ;""")
        e_time1 = time.time()
        print('select over---{}s'.format(e_time1 - s_time))
        base_sql = 'insert into gess_u_like_user_top3 (top3product_id,client_id) VALUES {}'
        datas = []
        for i, view_score in enumerate(score_result):
            client_id = view_score[0]
            product_id = view_score[1]
            datas.append((product_id, client_id))
            if i % 500 == 0 and i != 0:
                sql = base_sql.format(','.join(str(item) for item in datas))
                self.db.insert(sql)
                datas = []
        sql = base_sql.format(','.join(str(item) for item in datas))
        self.db.insert(sql)

    def product_costop_11(self):  # cos sort
        sel = """
          -- 在Navicat中 查询计算相似产品top11
select product1,GROUP_CONCAT(product2 ORDER BY cos desc)
from
(
	select product1,product2,cos,
	@rank1:=case when @product1=product1 then @rank1+1 else 1 end as rank1,
	@product1:=product1 as product11
	from 
	(
			SELECT product1,product2,sum(cos) as cos
			FROM
			(
				SELECT product1,product2,cos
				from gess_u_like_goods_similarity 
				 union all
				SELECT product1,product2,cos
				from gess_u_like_similar2 WHERE brand='unice'
			) t1
			group by product1,product2
	) as p,
	(SELECT @rank1:=1,@product1:='') as q
	order by product1,cos desc
) t
where rank1<=11
group by product1
          """
        product_list = self.db.select(sel)
        top11_list = {item[0]: item[1] for item in product_list}
        return top11_list

    def recommend_product(self):
        top11_dic = self.product_costop_11()
        topsql = """select client_id,top3product_id from gess_u_like_user_top3;"""

        purchase_product = self.hdb.get_all("""select client_id,product_id from gess_u_like_purchase_score;""")
        data = self.db.select(topsql)
        user_top3_dic = {}
        for item in data:
            user_top3_dic[item[0]] = item[1].replace(' ', '').split(",")
        purchase_dic = {}
        for purchase in purchase_product:
            client_id = purchase[0]
            product_id = purchase[1]
            if client_id not in purchase_dic.keys():
                purchase_dic[client_id] = []
            purchase_dic[client_id].append(product_id)
        recommend = {}
        n = 1
        t = len(data)
        for key, value in user_top3_dic.items():
            if len(value) == 1:
                if value[0] in top11_dic.keys():
                    top11 = top11_dic.get(value[0]).split(',')
                    recommend[key] = self.duplicate_removal(top11)[:11]
            else:
                top3_11 = []
                for top in value:
                    if top in top11_dic.keys():
                        top11 = top11_dic.get(top).split(',')
                        top3_11.append(top11)
                top_ = []
                i = 0
                STATUS = 1
                while STATUS:
                    for j, li in enumerate(top3_11):
                        try:
                            top_.append(li[i])
                        except:
                            pass
                    if len(top_) >= len(list(chain(*top3_11))):
                        break
                    i += 1
                if key in purchase_dic.keys():
                    last_top = [i for i in top_ if i not in purchase_dic[key]]
                else:
                    last_top = top_
                recommend[key] = self.duplicate_removal(last_top)[:11]
            n += 1
        return recommend

    def duplicate_removal(self,li):
        li1 = list(set(li))
        li1.sort(key=li.index)
        return li1

    def write_top11(self):
        t = self.db.get_one('select DATE_FORMAT(create_time,"%Y-%m-%d") from gess_u_like_top11 limit 2')
        if t:
            if t[0] == create_time:
                return
        t1 = time.time()
        print('counting recommend.....')
        recommend = self.recommend_product()
        t2 = time.time()
        print('it cost {}s'.format(t2-t1))
        user_dic = self.get_user_id_match_client_id()
        recommend_new = self.userid_relation_clientid(recommend,user_dic)
        self.db.truncate('gess_u_like_top11')
        base_sql = """insert into gess_u_like_top11 (client_id,user_id,top11) VALUES {};"""
        datas = []
        i = 1
        for k, v in recommend_new.items():
            if np.array(v,dtype=object).shape[0] == 2:
                user_id = v[-1][0]
                top11 = v[0]
            else:
                user_id = 'null'
                top11 = v
            datas.append((k, user_id, str(top11)))
            if i % 500 == 0 and i != 0:
                sql = base_sql.format(','.join(str(item) for item in datas))
                self.db.insert(sql.replace("'null'","null"))
                datas = []
            i += 1
        sql = base_sql.format(','.join(str(item) for item in datas))
        self.db.insert(sql.replace("'null'","null"))

    def userid_relation_clientid(self,client_dic,user_dic):
        for userid in user_dic:
            if userid[0] in client_dic.keys():
                client_dic[userid[0]] = [client_dic[userid[0]],[userid[1]]]
        return client_dic

    def run(self):
        s_time = time.time()
        self.get_user_top3_product()
        self.write_top11()
        e_time = time.time()
        print('finish 10---{}s\n'.format(e_time - s_time))


class Upload_11(object):
    def __init__(self):
        print("start 11 --- its often cost 1539s")
        self.db = Connect()
        self.host = '3.23.250.67'
        # self.host = '127.0.0.1'

    def post_user_top11_data(self):  # cost 851s for 41w
        sel = """select client_id,user_id,top11 from gess_u_like_top11;"""
        pro_list = self.db.select(sel)
        datas = [{"store":"unice", "client_id": item[0],"user_id": item[1], "top_list": item[2]} for item in pro_list]
        cut_num = 1000
        if len(datas) > cut_num:
            num = math.ceil(len(datas) / cut_num)
            batch_step = round(len(datas) / num)
            n = 1
            for index in range(0, len(datas), batch_step):
                item_list = datas[index:index + batch_step]
                p_data = json.dumps(item_list)
                url = 'http://{}:8001/user_toplike/set_user_top_v2/'.format(self.host)
                a = 0
                while a != 200:
                    try:
                        response = requests.post(url, data={"data": p_data})
                        a = response.status_code
                    except Exception as e:
                        time.sleep(1)
                        print(e)
                print('{}--{}--{}'.format(len(datas), cut_num * n, a))
                n += 1

        p_data = []

    def post_similar_product_top11(self):  # cost 4s for 580
        sql = """
          -- 在Navicat中 查询计算相似产品top11
select product1,GROUP_CONCAT(product2 ORDER BY cos desc)
from
(
	select product1,product2,cos,
	@rank1:=case when @product1=product1 then @rank1+1 else 1 end as rank1,
	@product1:=product1 as product11
	from 
	(
			SELECT product1,product2,sum(cos) as cos
			FROM
			(
				SELECT product1,product2,cos
				from gess_u_like_goods_similarity 
				 union all
				SELECT product1,product2,cos
				from gess_u_like_similar2 WHERE brand='unice'
			) t1
			group by product1,product2
	) as p,
	(SELECT @rank1:=1,@product1:='') as q
	order by product1,cos desc
) t
where rank1<=11
group by product1
    ;"""
        pro_list = self.db.select(sql)
        datas = {}
        for i in pro_list:
            datas[i[0]] = i[1].split(',')
        cos_data = json.dumps({"store":"unice","product_cos":datas})
        if datas:
            url = 'http://{}:8001/user_toplike/set_product_cos_v2/'.format(self.host)
            a = 0
            while a != 200:
                try:
                    response = requests.post(url, data={"data": cos_data})
                    a = response.status_code
                except Exception as e:
                    time.sleep(1)
                    print(e)
            print('{}----{}(success)'.format(len(datas),a))
            p_data = []
        else:
            print("no data to upload")

    def run(self):
        st = time.time()
        print('uploading top11.....')
        self.post_user_top11_data()
        t1 = time.time()
        print('it cost {}s'.format(t1-st))
        print('uploading cos....')
        self.post_similar_product_top11()
        et = time.time()
        print('cost {}s'.format(et - t1))
        print('finish 11 in {}s'.format(et-st))


if __name__ == '__main__':
    s_time = time.time()
    print('start at : {}'.format(time.ctime()))
    c1 = Product_id_title_1()
    c1.run()

    c2 = Get_goods_score_into_csv_2()
    c2.run()

    c3 = Get_goods_similar_simple_3()
    c3.run()

    c4 = Get_browse_data_4()
    c4.run()

    c5 = Get_search_data_5()
    c5.run()

    c6 = Get_add_purchase_data_source_6()
    c6.run()

    c7 = Get_purchase_data_source_7()
    c7.run()

    c8 = Count_each_part_score_8()
    c8.run()

    c9 = Control_Remote_csv_into_hive_9()
    c9.run()

    c10 = Count_top_by_hive_10()
    c10.run()

    c11 = Upload_11()
    c11.run()

    e_time = time.time()
    message = ' unice_recommend total cost {}s--{}'.format(e_time - s_time,create_time)
    mq.rabbitmq(message)
    print(message)
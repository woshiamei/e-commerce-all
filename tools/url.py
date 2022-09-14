import os
import pandas as pd
import numpy as np
import webbrowser
import json
import datetime
from reco.utils.connect import hive_connect
import time


#显示所有列
pd.set_option('display.max_columns', None)
#显示所有行
pd.set_option('display.max_rows', None)
#设置value的显示长度为100，默认为50
pd.set_option('max_colwidth',100)

cur_time = (datetime.datetime.now()).strftime("%Y-%m-%d").replace("-", "")
cur_time = '20220718'

# w2v swing 算法结果验证： 重合度、多样性
w2v_res_file = "../data/w2v_res_20220716.csv"
swing_res_file = "../data/swing_res_20220714.csv"

# w2v_res = pd.read_csv(w2v_res_file)
w2v_res = {}
swing_res = {}

f = open(w2v_res_file, "r")
# line = f.readline()
for line in f.readlines():
    r = line.strip().split(":")
    w2v_res[int(r[0])] = list(map(int, r[1].split(",")))

f.close()
print(len(w2v_res))
# print(w2v_res)
print(w2v_res[4289])

with open(swing_res_file, "r") as f:
    for line in f.readlines():
        r = line.strip().split(":")
        swing_res[int(r[0])] = list(map(int, r[1].split(",")))

print(len(w2v_res))
print(swing_res[4289])

print(len(set(swing_res[4289]) & set(w2v_res[4289])))

# 查看 distinct pid
pids = list(w2v_res.keys())
print(pids)
len(pids)
# Out[5]: 1501

# 改为 sql 查询
print('start sql: ')
t1 = time.time()
hdb = hive_connect()

etime = (datetime.datetime.now()).strftime("%Y-%m-%d").replace("-", "")
stime = (datetime.datetime.now() - datetime.timedelta(days=180)).strftime("%Y-%m-%d").replace("-", "-")
print("stime - etime: {} - {}".format(stime, etime))
item_picture_file = "./item_pictures_{}.csv".format(cur_time)
if not os.path.exists(item_picture_file):
    raw_item_pictures = hdb.get_all("""
        select product_id, url_path, date_time
        from (
        -- select product_id, pro_type, product_color, product_texture, lace_size, wigtype, 
        select product_id, url_path, date_time,
        row_number() over(partition by product_id order by date_time desc) as rank 
        from jdm.s_bq_produce_summary_qu
        where date_time > '{}'
        and shop_mark = 'unicemall'
        and platfrom = "自营站"
        ) t1
        where rank = 1
        """.format(stime))
    print(len(raw_item_pictures))
    t2 = time.time()
    print("item info sql 查询耗时： {}".format(t2 - t1))

    def data_to_csv(data, file, sep="\t"):
        if os.path.exists(file):
            os.remove(file)
        f = open(file, 'w', encoding='UTF-8')
        # s = ','
        for i in data:
            if (i != ""):
                r = map(str, i)
                f.write(sep.join(r) + '\n')

    data_to_csv(raw_item_pictures, item_picture_file)

# file = "../data/item_info.csv"
data = pd.read_csv(item_picture_file, sep='\t')
data.columns = ["product_id", "url_path", "date"]
data.drop(["date"], axis=1, inplace=True)
print("item-url: ")
print(data.head())

# data = pd.read_csv(item_picture_file, header=0)
# ["product_id","url_path","etl_date"]
# print(data.product_id)
# print(data.head())

data["url"] = data.apply(lambda x: "https://www.unice.com/" + x.url_path, axis=1)

# URL dict
url_dict = {}
for i in range(len(data)):
    url_dict[int(data['product_id'][i])] = data['url'][i]
print("url_dict len: {}".format(len(url_dict)))
# json.dump(url_dict, open("./pid_url.json", "w"))
print(url_dict)
# webbrowser.open(data['url'][0])


# w2v swing 算法结果 load
w2v_res_file = "../data/w2v_res_20220714.csv"
w2v_click_res_file = "../data/w2v_click_res_20220718.csv"
swing_res_file = "../data/swing_res_20220714.csv"
swing_click_res_file = "../data/swing_click_res_20220718.csv"
final_res_file = "../res/final_res_{}.csv".format(cur_time)

# w2v_res = pd.read_csv(w2v_res_file)
w2v_res = {}
swing_res = {}
w2v_click_res = {}
swing_click_res = {}
final_click_res = {}

with open(final_res_file, "r") as f:
    for line in f.readlines():
        r = line.strip().split(":")
        final_click_res[int(r[0])] = list(map(int, r[1].split(",")))

print(len(final_click_res))

f = open(w2v_res_file, "r")
# line = f.readline()
for line in f.readlines():
    r = line.strip().split(":")
    w2v_res[int(r[0])] = list(map(int, r[1].split(",")))

f.close()
print(len(w2v_res))
# print(w2v_res)
print(w2v_res[4289])

with open(swing_res_file, "r") as f:
    for line in f.readlines():
        r = line.strip().split(":")
        swing_res[int(r[0])] = list(map(int, r[1].split(",")))

print(len(swing_res))
# print(swing_res[4289])
#
# print(len(set(swing_res[4289]) & set(w2v_res[4289])))

with open(w2v_click_res_file, "r") as f:
    for line in f.readlines():
        r = line.strip().split(":")
        w2v_click_res[int(r[0])] = list(map(int, r[1].split(",")))

print(len(w2v_click_res))


with open(swing_click_res_file, "r") as f:
    for line in f.readlines():
        r = line.strip().split(":")
        swing_click_res[int(r[0])] = list(map(int, r[1].split(",")))

print(len(swing_click_res))

# 查看 distinct pid
pids = list(w2v_click_res.keys())
print(pids)
len(pids)

limit = 10
for i in pids:
    # print(i + 100)
    i = 3773
    print(i)
    if i in url_dict:
        print(url_dict[i])
        webbrowser.open(url_dict[i])
    for j in final_click_res[i][:limit]:
        print(j)
        if j in url_dict:
            print(url_dict[j])
            webbrowser.open(url_dict[j])
    break


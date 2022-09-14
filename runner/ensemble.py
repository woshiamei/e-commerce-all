import pandas as pd
import datetime
from connect import hive_connect
import time
import gensim
import os
pd.set_option('display.max_columns',20)
pd.set_option('display.max_rows',100)

cur_time = (datetime.datetime.now()).strftime("%Y-%m-%d").replace("-", "")
# cur_time = '20220718'

# 读入 w2v res
w2v_click_res_file = "../data/w2v_click_res_{}.csv".format(cur_time)

with open("../data/w2v_done_file.csv", "r") as f:
    line = f.readline().strip()
w2v_click_res_file = line
print("w2v_click_res_file: ", w2v_click_res_file)

w2v_click_res = {}
with open(w2v_click_res_file, "r") as f:
    for line in f.readlines():
        r = line.strip().split(":")
        w2v_click_res[int(r[0])] = list(map(int, r[1].split(",")))

print(len(w2v_click_res))
# print(w2v_click_res)

# score
w2v_click_res_2 = {}
smooth = 0.01
for k, v in w2v_click_res.items():
    w2v_click_res_2[k] = {}
    for i,item in enumerate(v):
        w2v_click_res_2[k][item] = 1.0 / (i + 1 + smooth)

# print(w2v_click_res_2)
print(len(w2v_click_res_2))


# 读入 swing res
swing_click_res_file = "../data/swing_click_res_{}.csv".format(cur_time)

with open("../data/swing_done_file.csv", "r") as f:
    line = f.readline().strip()
swing_click_res_file = line
print("swing_click_res_file: ", swing_click_res_file)

swing_click_res = {}
with open(swing_click_res_file, "r") as f:
    for line in f.readlines():
        r = line.strip().split(":")
        swing_click_res[int(r[0])] = list(map(int, r[1].split(",")))

print(len(swing_click_res))
# print(swing_click_res)
# score
swing_click_res_2 = {}
smooth = 0.01
for k, v in swing_click_res.items():
    swing_click_res_2[k] = {}
    for i,item in enumerate(v):
        swing_click_res_2[k][item] = 1.0 / (i + 1 + smooth)

# print(swing_click_res_2)
print(len(swing_click_res_2))

# 读入 item info
# 改成 sql , 线上使用

print('start sql: ')
t1 = time.time()
hdb = hive_connect()

etime = (datetime.datetime.now()).strftime("%Y-%m-%d").replace("-", "")
stime = (datetime.datetime.now() - datetime.timedelta(days=180)).strftime("%Y-%m-%d").replace("-", "")
print("stime - etime: {} - {}".format(stime, etime))

item_info_file = "../data/item_info_{}.csv".format(cur_time)
if not os.path.exists(item_info_file):
    raw_item_info = hdb.get_all("""
        SELECT productsku, 
            sum(CASE WHEN p_view = '0' THEN 0 ELSE 1 END) as show_sum, 
            sum(CASE WHEN p_click = '0' THEN 0 ELSE 1 END) as click_sum, 
            sum(CASE WHEN is_addtocart = '0' THEN 0 ELSE 1 END) as add_sum,
            sum(CASE WHEN is_order = '0' THEN 0 ELSE 1 END) as order_sum,
            sum(CASE WHEN p_click = '0' THEN 0 ELSE 1 END) /  sum(CASE WHEN p_view = '0' THEN 0 ELSE 1 END) as ctr,
            sum(CASE WHEN is_addtocart = '0' THEN 0 ELSE 1 END) /  sum(CASE WHEN p_view = '0' THEN 0 ELSE 1 END) as atr,
            sum(CASE WHEN is_order = '0' THEN 0 ELSE 1 END) /  sum(CASE WHEN p_view = '0' THEN 0 ELSE 1 END) as cvr
        from ods.s_bq_custAct
        where `date` > '{}'
        and productsku regexp '^[0-9]+$'
        GROUP BY productsku
        having sum(cast(p_view as BIGINT)) > 1000
        order by  sum(cast(p_view as BIGINT)) DESC
        """.format(stime))
    print(len(raw_item_info))
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

    data_to_csv(raw_item_info, item_info_file)

# file = "../data/item_info.csv"
item_info = pd.read_csv(item_info_file, sep='\t')
item_info.columns = ["productsku", "show_sum", "click_sum", "add_sum", "order_sum", "ctr", "atr", "cvr"]
print("item-info: ")
print(item_info.head())
# 过滤
item_info.sort_values("cvr", ascending=False, inplace=True)
item_info = item_info[item_info.cvr < 0.1]
item_info = item_info[item_info.atr < 0.1]
item_info = item_info[item_info.ctr < 0.1]
item_info = item_info[item_info.show_sum > 10000]

w_ctr, w_atr, w_cvr = 0.3, 0.3, 0.4
item_info['score'] = item_info.apply(lambda x: w_ctr * x.ctr + w_atr * x.atr + w_cvr * x.cvr, axis=1)

print(item_info.describe())
print(item_info.head())

# ensemble sort
final_ensemble_score = {}
# for k, v in enumerate(w2v_click_res_2):
#     if k in swing_click_res_2:
#         k_res = set(v.keys() + swing_click_res_2[k].keys())
#         print(len(k_res))

# 查看两个算法的结果重合度
# 计算出 xtr 排序分
xtr_click_res = {}
for k , v in w2v_click_res_2.items():
    list_pids = list(item_info['productsku'])
    xtr_click_res[k] = {}
    if k in swing_click_res_2.keys():
        k_res = list(set(list(v.keys()) + list(swing_click_res_2[k].keys())))
        for i in k_res:
            if i in list_pids:
                xtr_click_res[k][i] = item_info.loc[item_info['productsku'] == i, 'score'].values[0]
            else:
                xtr_click_res[k][i] = 0
    else:
        # print('{} not in swing!'.format(k))
        k_res = list(set(v.keys()))
        for i in k_res:
            if i in list_pids:
                xtr_click_res[k][i] = item_info.loc[item_info['productsku'] == i, 'score'].values[0]
            else:
                xtr_click_res[k][i] = 0
    # print('len k_res is {}'.format(len(k_res)))
    # print(xtr_click_res)

xtr_click_res_2 = {}
smooth = 0.01
for k, items in xtr_click_res.items():
    xtr_click_res_2[k] = {}
    # print(items)
    items = sorted(items.items(), key=lambda k: k[1], reverse=True)
    # print(items)
    for i, item in enumerate(items):
        xtr_click_res_2[k][item[0]] = 1.0 / (i + 1 + smooth)
    # print(xtr_click_res_2[k])
    # break
print(len(xtr_click_res_2))


# 将 score 融合
w_w2v, w_swing, w_xtr = 0.5, 0.3, 0.2

def merge_dict(x, y, z):
    res = {}
    for k, v in x.items():
        # w2v +  swing
        if k in y.keys():
            if k in z.keys():
                res[k] = w_w2v * v + w_swing * y[k] + w_xtr * z[k]
            else:
                res[k] = w_w2v * v + w_swing * y[k] + w_xtr * 0

        elif k in z.keys():
            res[k] = w_w2v * v + w_swing * 0 + w_xtr * z[k]
        # print(res[k])
        # break
    return res

# 将 score 融合
sep = ","
topN = 30
file = open("../res/final_res_{}.csv".format(cur_time), "w")
for k , v in w2v_click_res_2.items():
    final_ensemble_score[k] = {}
    if k in swing_click_res_2.keys():
        if k in xtr_click_res_2.keys():
            final_ensemble_score[k] = merge_dict(w2v_click_res_2[k], swing_click_res_2[k], xtr_click_res_2[k])
        else:
            final_ensemble_score[k] = merge_dict(w2v_click_res_2[k], swing_click_res_2[k], {})
        # print(final_ensemble_score[k])
        # break
    elif k in xtr_click_res_2.keys():
        final_ensemble_score[k] = merge_dict(w2v_click_res_2[k], {},  xtr_click_res_2[k])
        # print(final_ensemble_score[k])
        # break
    final_ensemble_score[k] = list(sorted(final_ensemble_score[k].items(), key=lambda k: k[1], reverse=True))[:topN]
    res_str = ""
    res_str = sep.join([str(sim_pair[0]) for sim_pair in final_ensemble_score[k]])
    file.write(str(k) + ' : ' + res_str + '\n')
# print(final_ensemble_score)

#import json
#json.dump(final_ensemble_score, open("../res/final_ensemble_score_{}.json".format(cur_time), "w"))
#json.dump(swing_click_res_2, open("../res/swing_click_res_{}.json".format(cur_time), "w"))
#json.dump(w2v_click_res_2, open("../res/w2v_click_res_{}.json".format(cur_time), "w"))
#json.dump(xtr_click_res_2, open("../res/xtr_click_res_{}.json".format(cur_time), "w"))

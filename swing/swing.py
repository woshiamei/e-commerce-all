import time
import datetime
import pandas as pd
import os

from reco.utils.connect import hive_connect
import logging
from itertools import combinations
import json

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

def raw_data_by_sql():
    print('start sql: ')
    hdb = hive_connect()
    t1 = time.time()

    # stime = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime("%Y-%m-%d").replace("-", '')
    etime = (datetime.datetime.now()).strftime("%Y-%m-%d").replace("-", "")
    stime = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime("%Y-%m-%d").replace("-", "")
    print("stime - etime: {} - {}".format(stime, etime))

    user_item_action = hdb.get_all("""
        select fullvisitorid, productsku, ecommerceaction, `date`
        from s_bq_customer_action
        where `date` > '{}'  
        and shop_mark = 'unicemall'
        and productsku regexp '^[0-9]+$'
        order by fullvisitorid, `date` 
    """.format(stime))

    # print(type(user_item_action))
    print(len(user_item_action))

    t2 = time.time()
    print("sql 查询耗时： {}".format(t2 - t1))

    hdb.close()
    return user_item_action

def set_default(obj):
    if isinstance(obj[1], set):
        return list(obj[1])
    raise TypeError

def get_uitems_iusers(train, is_save=False):
    u_items = dict()
    i_users = dict()
    for index, row in train.iterrows():
        u_items.setdefault(row["uid"], set())
        i_users.setdefault(row["pid"], set())

        u_items[row["uid"]].add(row["pid"])
        i_users[row["pid"]].add(row["uid"])
    print("用户个数为：{}".format(len(u_items)))
    print("item个数为：{}".format(len(i_users)))
    if is_save:
        f = open(swing_item_dict_tmp_path, "w")
        f2 = open(swing_user_dict_tmp_path, "w")
        for item, users in i_users.items():
            f.write(str(item) + ":" + ",".join(map(str, list(users))) + '\n')
        for user, items in u_items.items():
            f2.write(str(user) + ":" + ",".join(map(str, list(items))) + '\n')
        f.close()
        f2.close()
        #     json.dump(u_items, open(swing_user_dict_tmp_path, "w"), cls=set_default)
        #     json.dump(i_users, open(swing_item_dict_tmp_path, "w"), cls=set_default)
    return u_items, i_users


def cal_similarity(u_items, i_users):
    item_pairs = list(combinations(i_users.keys(), 2))
    print("item pairs length：{}".format(len(item_pairs)))
    item_sim_dict = dict()
    cnt = 0
    num_less_thre = 0
    for (i, j) in item_pairs:
        cnt += 1
        if cnt % 10000 == 0:
            print(cnt)
        user_pairs = list(combinations(i_users[i] & i_users[j], 2))
        result = 0.0
        for (u, v) in user_pairs:
            if len(u_items[u]) < threshold or len(u_items[v]) < threshold:
                #                 print("user len(items) less {}".format(threshold))
                num_less_thre += 1
                continue
            result += 1 / (alpha + list(u_items[u] & u_items[v]).__len__())

        item_sim_dict.setdefault(i, dict())
        item_sim_dict[i][j] = result
        # print(item_sim_dict[i][j])
    print("user len(item) less {} ,num is {}".format(threshold, num_less_thre))
    return item_sim_dict

def save_item_sims(item_sim_dict, path):
    new_item_sim_dict = dict()
    for item, sim_items in item_sim_dict.items():
        new_item_sim_dict.setdefault(item, dict())
        new_item_sim_dict[item] = dict(sorted(sim_items.items(), key = lambda k:k[1], reverse=True)[:topN])
    json.dump(new_item_sim_dict, open(path, "w"))
    print("item 相似 item（{}）保存成功！".format(topN))
    return new_item_sim_dict

def data_to_csv(data, file, sep="\t"):
    if os.path.exists(file):
        os.remove(file)
    f = open(file, 'w', encoding='UTF-8')
    # s = ','
    for i in data:
        if (i != ""):
            r = map(str, i)
            f.write(sep.join(r) + '\n')

def checkout_path(path):
    if os.path.exists(path):
        os.remove(path)

def final_res_to_file(path):
    f = open(path, 'w', encoding='UTF-8')
    s = ","
    res_str = ""
    for item, sim_items in new_item_sim_dict.items():
        # topN_dict = dict(sorted(sim_items.items(), key=lambda k: k[1], reverse=True)[:topN])
        res_str = s.join(map(str, sim_items.keys()))
        f.write(str(item) + " : " + res_str + '\n')
        # print(sim_items)
    f.close()

if __name__ == '__main__':
    s_time = time.time()
    cur_time = (datetime.datetime.now()).strftime("%Y-%m-%d").replace("-", "")
    # init data
    days = 180
    topN = 30
    alpha = 0.5
    threshold = 2

    print('start time: {}'.format(time.ctime()))

    file_raw_data = "../data/raw_sample_{}.csv".format(cur_time)
    swing_user_dict_tmp_path = "../data/swing_user_dict_tmp_{}.json".format(cur_time)
    swing_item_dict_tmp_path = "../data/swing_item_dict_tmp_{}.json".format(cur_time)
    swing_sim_tmp_path = "../data/swing_sim_tmp_{}.json".format(cur_time)
    swing_sim_res_path = '../data/swing_res_{}.csv'.format(cur_time)

    new_item_sim_dict = {}
    u_items = {}
    i_users = {}

    if not os.path.exists(swing_sim_tmp_path):
        # if not os.path.exists(swing_user_dict_tmp_path) or not os.path.exists(swing_item_dict_tmp_path):
        if not os.path.exists(file_raw_data):
            raw_data = raw_data_by_sql()
            data_to_csv(raw_data, file_raw_data)
        raw_data = pd.read_csv(file_raw_data, sep="\t")
        raw_data.columns = ["uid", "pid", "action", "date"]
        print(raw_data.describe())

        # 优化计算量（> threshold）
        u_items, i_users = get_uitems_iusers(raw_data, False)
        # else:
        #     u_items = json.load(open(swing_user_dict_tmp_path, "r"))
        #     i_users = json.load(open(swing_user_dict_tmp_path, "r"))

        item_sim_dict = cal_similarity(u_items, i_users)

        # checkout_path(swing_sim_tmp_path)
        new_item_sim_dict = save_item_sims(item_sim_dict, swing_sim_tmp_path)
    else:
        new_item_sim_dict = json.load(open(swing_sim_tmp_path, "r"))

    # checkout_path(swing_sim_res_path)
    final_res_to_file(swing_sim_res_path)

    e_time = time.time()
    message = ' swing total cost {}s-- 开始时间： {}'.format(e_time - s_time, cur_time)
    print(message)

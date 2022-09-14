import time
import datetime
import pandas as pd
import os
import sys

sys.path.append("/root/reco/i2i/")
from utils.connect import hive_connect
from gensim.models import Word2Vec
import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

def raw_data_by_sql():
    print('start sql: ')
    hdb = hive_connect()
    t1 = time.time()

    # stime = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime("%Y-%m-%d").replace("-", '')
    etime = (datetime.datetime.now()).strftime("%Y-%m-%d").replace("-", "")
    stime = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime("%Y-%m-%d").replace("-", "")
    print("stime - etime: {} - {}".format(stime, etime))

    # user_item_action = hdb.get_all("""
    #     select fullvisitorid, productsku, ecommerceaction, `date`
    #     from s_bq_customer_action
    #     where `date` > '{}'
    #     and shop_mark = 'unicemall'
    #     and productsku regexp '^[0-9]+$'
    #     order by fullvisitorid, `date`
    # """.format(stime))

    user_item_action = hdb.get_all("""
        select fullvisitorid, productsku, p_click, `date`
        from ods.s_bq_custAct
        where `date` > '{}'
        and productsku regexp '^[0-9]+$'
        and (cast(p_click as bigint) > 0 or cast(is_addtocart as bigint) > 0 or cast(is_order as bigint) > 0)
        order by fullvisitorid, `date`
    """.format(stime))

    # print(type(user_item_action))
    print(len(user_item_action))

    # user_item_cnt = hdb.get_all(
    #     """
    #     SELECT count(*)
    #     from (
    #         SELECT
    #             fullvisitorid, count(*) as cnt
    #         from
    #             s_bq_customer_action
    #         where `date` > '{}'
    #         and shop_mark = 'unicemall'
    #         group by fullvisitorid
    #     ) t1
    #     where t1.cnt >= 3
    #     """.format(stime)
    # )
    t2 = time.time()
    print("sql 查询耗时： {}".format(t2 - t1))
    # print("count(cnt >= 3): {}".format(user_item_cnt))
    # for i in range(10):
    #     print(user_item_action[i])
    hdb.close()
    return user_item_action

def gen_item_list(items):
    # print results
    action_items = []
    result_strs = []
    #shuffle(click_items)
    for click_item in items:
        action_items.append(click_item)
    return action_items

def samples_list(data):
    samples = []
    is_start = True
    before_user = ""
    items = set()
    for i, row in data.iterrows():
    # for row in data:
        if len(row) < 3:
            continue
        if is_start:
            is_start = False
            items.add(row[1])
            before_user = row[0]
            continue
        if before_user == row[0]:
            items.add(row[1])
            continue
        if len(items) >= threshold:
            res_list = gen_item_list(items)
            samples.append(res_list)
        #     for res_str in res_strs:
        #         print(res_str)
        #         file.write(str(res_str)+'\n')
        items.clear()
        items.add(row[1])
        before_user = row[0]
    if len(items) > 3:
        res_list = gen_item_list(items)
        samples.append(res_list)
    return samples

def data_to_csv(data, file, sep="\t"):
    if os.path.exists(file):
        os.remove(file)
    f = open(file, 'w', encoding='UTF-8')
    # s = ','
    for i in data:
        if (i != ""):
            r = map(str, i)
            f.write(sep.join(r) + '\n')

def checkout_all_nums(file_raw_data, file_samples):
    len_raw_data , len_sample = 0, 0
    data = pd.read_csv(file_raw_data)
    data.columns = ["uid", "pid", "action", "date"]
    data = data.groupby(['uid']).count()
    # print(data.head())
    len_raw = data[data.pid > threshold].count()[0]

    f = open(file_samples, "r")
    # i = 0
    line = f.readline()
    while line:
        # len_sample += len(line.strip().split(","))
        len_sample += 1
        line = f.readline()
    print("raw_data num is {},  samples num is {}".format(len_raw, len_sample))

if __name__ == '__main__':
    s_time = time.time()
    cur_time = (datetime.datetime.now()).strftime("%Y-%m-%d").replace("-", "")
    # init data
    days = 180
    topN = 50
    threshold = 3

    print('start time: {}'.format(time.ctime()))
    print('init params: days: {}, topN: {}, threshold: {}'.format(days, topN, threshold))

    file_raw_data = "../data/raw_sample_click_{}.csv".format(cur_time)
    file_sample = "../data/w2v_sample_click_{}.csv".format(cur_time)

    if not os.path.exists(file_raw_data):
        raw_data = raw_data_by_sql()
        data_to_csv(raw_data, file_raw_data)
    raw_data = pd.read_csv(file_raw_data, sep="\t")
    raw_data.columns = ["uid", "pid", "action", "date"]
    # string -> int
    # raw_data["pid"] = raw_data['pid'].astype(int)
    # 遍历 series
    raw_data["date"] = raw_data['date'].astype(object)
    print(raw_data.describe())

    samples = samples_list(raw_data)
    if not os.path.exists(file_sample):
        data_to_csv(samples, file_sample)

    # print("samples len: {}".format(len(samples)))
    # res = checkout_all_nums(file_raw_data, file_sample)
    print(samples[:3])
    print("sample len: {}".format(len(samples)))

    model = Word2Vec(samples, vector_size=64, min_count=5, sg=0, window=3, workers=4, negative=5, epochs=10)

    print(model.wv.similarity(3873, 3459))
    w2v_click_path = './model/w2v_click_{}.model'.format(cur_time)
    model.save(w2v_click_path)

    model1 = Word2Vec.load(w2v_click_path)

    # model.wv.save_word2vec_format('../data/text.model.bin', binary=True)
    # print(samples[:5])
    # model.wv.most_similar("3873",topn=topN)
    # model.wv.save_word2vec_format('../data/w2v_emb.txt', binary=False)

    print(model.wv.most_similar(3873))

    # w2v_emb_file = '../data/w2v_emb.txt'
    # res_map = {}
    # with open(w2v_emb_file) as f:
    #     line = f.readline()
    #     while line:
    #         # print(line)
    #         line = f.readline()
    #         line = line.strip().split()
    #         if len(line) != 65:
    #             continue
    #         res_map[str(line[0])] = line[1:]
    # print(res_map['3795'])

    pids = raw_data.pid.unique()

    print(model.wv.most_similar(3264, topn=topN))

    topN_dict = {}
    for pid in pids:
        try:
            topN_dict[pid] = model.wv.most_similar(pid, topn=topN)
        except Exception as e:
            pass

    w2v_res_file = '../data/w2v_click_res_{}.csv'.format(cur_time)
    # if os.path.exists(w2v_res_file):
    #     os.remove(w2v_res_file)
    file = open(w2v_res_file, 'w', encoding='UTF-8')

    final_res = {}
    sep = ","
    for pid in pids:
        if pid in topN_dict:
            #         print("topN ", topN[pid])
            res_str = ""
            res_str = sep.join([str(sim_pair[0]) for sim_pair in topN_dict[pid]])
            final_res[pid] = res_str
            file.write(str(pid) + ' : ' + res_str + '\n')
    # print(final_res)
    file.close()
    print(len(final_res))

    # cur_time -> done_file
    with open("/root/reco/i2i/data/w2v_done_file.csv", "w") as f:
        f.write("{}".format(w2v_res_file) + '\n')


    e_time = time.time()
    message = ' w2v total cost {}s-- 开始时间： {}'.format(e_time - s_time, cur_time)
    print(message)

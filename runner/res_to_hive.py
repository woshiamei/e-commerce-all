import time
import datetime
import os
from connect import hive_connect

cur_time = (datetime.datetime.now()).strftime("%Y-%m-%d").replace("-", "")
#cur_time = '20220720'
file = "../res/final_res_{}.csv".format(cur_time)
print("cur_time: ", cur_time)

print('start sql: ')
t1 = time.time()
hdb = hive_connect()

etime = (datetime.datetime.now()).strftime("%Y-%m-%d").replace("-", "")
stime = (datetime.datetime.now() - datetime.timedelta(days=180)).strftime("%Y-%m-%d").replace("-", "")
print("stime - etime: {} - {}".format(stime, etime))

datas = {}
data_list = []
product_sim_file = "../res/final_res_{}.csv".format(cur_time)
if os.path.exists(product_sim_file):
    with open(product_sim_file, "r") as f:
        for line in f.readlines():
            r = line.strip().split(" : ")
            if len(r) == 2:
                datas[r[0]] = r[1].strip()
            else:
                print("error res: {}".format(line))
    base_sql = 'insert into gdm.s_bq_product_sim_top30 (product_id, sim_product, day) VALUES {}'
    t1 = time.time()
    for i, k in enumerate(datas):
        product_id = k
        sim_product = datas[k]
        day = cur_time
        data_list.append((product_id, sim_product, day))
        if i % 1000 == 0 and i != 0:
            t2 = time.time()
            # print('insert---{}---{}s'.format(i,t2-t1))
            sql = base_sql.format(','.join(str(item) for item in data_list))
            hdb.insert(sql)
            data_list = []
            t1 = time.time()
    sql = base_sql.format(','.join(str(item) for item in data_list))
    hdb.insert(sql)

    t2 = time.time()
    print("item info sql 查询耗时： {}".format(t2 - t1))

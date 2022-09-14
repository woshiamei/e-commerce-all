import pandas as pd
import datetime
import time
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
        # print(line)
        r = line.strip().split(":")
        w2v_click_res[int(r[0])] = list(map(int, r[1].split(",")))

print(len(w2v_click_res))
print(w2v_click_res)

# score
w2v_click_res_2 = {}
smooth = 0.01
for k, v in w2v_click_res.items():
    w2v_click_res_2[k] = {}
    for i,item in enumerate(v):
        w2v_click_res_2[k][item] = 1.0 / (i + 1 + smooth)

# print(w2v_click_res_2)
print(len(w2v_click_res_2))



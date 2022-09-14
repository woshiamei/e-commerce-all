import json
import os
import pandas as pd
import numpy as np

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

# w2v  swing res 重合情况
inter_len_list = []
for pid in pids:
    if pid in swing_res.keys() and pid in w2v_res.keys():
        # print(len(set(swing_res[pid]) & set(w2v_res[pid])))
        inter_len_list.append(len(set(swing_res[pid]) & set(w2v_res[pid])))
print(inter_len_list)

np.mean(inter_len_list)
# Out[3]: 3.63
np.sum(inter_len_list)
# Out[4]: 5445

frequency = {}
for k in w2v_res.items():
    # print(k[1])
    for i in k[1]:
        if i in frequency.keys():
            frequency[i] += 1
        else:
            frequency[i] = 1
print("frequency len: {}".format(len(frequency)))
cnt_value = 0
for v in frequency.values():
    if v > 10:
        cnt_value += 1
print("frequency cnt > 10: {}".format(cnt_value))
json.dump(frequency, open("./frequency.json", "w"))
# w2v
# frequency len: 1488
# frequency cnt > 10: 1273

frequency = {}
for k in swing_res.items():
    # print(k[1])
    for i in k[1]:
        if i in frequency.keys():
            frequency[i] += 1
        else:
            frequency[i] = 1
print("frequency len: {}".format(len(frequency)))
cnt_value = 0
for v in frequency.values():
    if v > 10:
        cnt_value += 1
print("frequency cnt > 10: {}".format(cnt_value))
json.dump(frequency, open("./frequency.json", "w"))
# swing
# frequency len: 1611
# frequency cnt > 10: 1432
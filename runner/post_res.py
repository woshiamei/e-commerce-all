# 将结果写到接口里面
# from urllib import request,parse

import requests
import json
import time
import datetime
import os

import argparse



#host= '127.0.0.1'
host= '172.30.145.102'

def set_data(datas):
    # datas = {}
    # for i in pro_list:
    #     datas[i[0]] = i[1].split(',')
    # datas = {'1': ['2', '3', '4']}
    print("datas: ", datas)
    cos_data = json.dumps({"store": "unice", "product_sim": datas})
    if datas:
        url = 'http://{}:8000/set_data/'.format(host)
        a = 0
        i = 0
        while a != 200 and i < 10:
            i += 1
            try:
                response = requests.post(url, data={"data": cos_data})
                a = response.status_code
            except Exception as e:
                time.sleep(1)
                print(e)
        print('{}----{}(success)'.format(len(datas), a))
        p_data = []
    else:
        print("no data to upload")


def get_data():
    url = 'http://{}:8000/get_product_sim/'.format(host)

    a = 0
    i = 0
    while a != 200 and i < 10:
        i += 1
        try:
            response = requests.get(url, params={"store": 'unice'})
            a = response.status_code
        except Exception as e:
            time.sleep(1)
            print(e)
    print('{}----{}(success)'.format(len(response.json()), a))
    datas = response.json()
    print(datas)
    print(type(datas))

def post_data():
    datas = {}
    cur_time = (datetime.datetime.now()).strftime("%Y-%m-%d").replace("-", "")
    #cur_time = '20220726'
    file = "../res/final_res_{}.csv".format(cur_time)
    if os.path.exists(file):
        with open(file, "r") as f:
            for line in f.readlines():
                r = line.strip().split(" : ")
                if len(r) == 2:
                    datas[r[0]] = list(r[1].strip().split(','))
                else:
                    print("error res: {}".format(line))
    # print(datas)
        if len(datas) > 350:
            set_data(datas)
        else:
            print("len(datas) is short: {}".format(len(datas)))
            exit()
    else:
        print("file not exit: {}".format(file))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--type', type=str, default = 'get')
    #parser.add_argument('--bb', type=int, default=32)
    args = parser.parse_args()
    print(args.type)
    if args.type == 'post':
        post_data()
    else:
        get_data()

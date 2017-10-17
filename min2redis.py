#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
min2redis

Usage:
  min2redis -h | --help | --version
  min2redis <host> [-p <port>] [-d <date>] -f <file>

Arguments:
  <host>            redis host

Options:
  -h --help         show help
  -v --version      show version
  -p <port>         redis port
  -d <date>         date of today, write to db1
  -f <file>         min1.dat file
"""

import os
import sys
from docopt import docopt
import configparser
from dfparse import DataFile
from dfparse import Day
import redis
import json

__author__ = "wangyx"
__version__ = "0.5.1"

if __name__ == '__main__':
    arguments = docopt(__doc__, version='min2redis {}'.format(__version__))
    datafile = arguments["-f"]
    redishost = arguments["<host>"]
    redisport = int(arguments["-p"]) if arguments["-p"] != None else 6379
    today = arguments["-d"]

    print("file: {}".format(datafile))
    print("host: {}".format(redishost))
    print("port: {}".format(redisport))

    df = DataFile(datafile, Day)
    if today :
        r = redis.Redis(host=redishost, port=redisport, db=1)
        for goodsid in df.goodsidx:
            k = 'min1:{:0>7}'.format(goodsid)
            print(k)
            ts = df.getgoodstms(goodsid)
            for i in ts:
                time = '20{}'.format(i.time)
                if today == time[0:8]:
                    data = {
                                'time':time,
                                'open':'{}'.format(i.open),
                                'high':'{}'.format(i.high),
                                'low':'{}'.format(i.low),
                                'close':'{}'.format(i.close),
                                'volume':'{}'.format(i.volume),
                                'amount':'{}'.format(i.amount)
                            }
                    v = json.dumps(data)
                    r.rpush(k, v)
        exit(0)

    r = redis.Redis(host=redishost, port=redisport)
    p = r.pipeline(transaction=False)
    for goodsid in df.goodsidx:
        mk = 'min1:{:0>7}'.format(goodsid)
        print(mk)
        ts = df.getgoodstms(goodsid)
        for i in ts:
            data = {'open':'{}'.format(i.open),
                    'high':'{}'.format(i.high),
                    'low':'{}'.format(i.low),
                    'close':'{}'.format(i.close),
                    'volume':'{}'.format(i.volume),
                    'amount':'{}'.format(i.amount)}
            v = json.dumps(data)
            sk = '20{}'.format(i.time)
            #print('{}:{}'.format(sk, v))
            p.hsetnx(mk, sk, v)
        p.execute()

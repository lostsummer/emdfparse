#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
min2redis

Usage:
  min2redis -h | --help | --version
  min2redis <host> [-p <port>] -d <date> [-b <db>] -f <file>

Arguments:
  <host>            redis host

Options:
  -h --help         show help
  -v --version      show version
  -p <port>         redis port
  -d <date>         date of today, write to db1
  -b <db>           n of redis db
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

def getjv(min):
    data = {
        'time':min.time + 200000000000,
        'open':min.open/1000.0,
        'high':min.high/1000.0,
        'low':min.low/1000.0,
        'close':min.close/1000.0,
        'volume':min.volume,
        'amount':min.amount
    }
    return json.dumps(data)

if __name__ == '__main__':
    arguments = docopt(__doc__, version='min2redis {}'.format(__version__))
    datafile = arguments["-f"]
    redishost = arguments["<host>"]
    redisport = int(arguments["-p"]) if arguments["-p"] != None else 6379
    today = arguments["-d"]
    redisdb = arguments["-b"]
    ntoday = int(today)
    nredisdb = 1 if (not redisdb) else int(redisdb)


    df = DataFile(datafile, Day)
    if today :
        r = redis.Redis(host=redishost, port=redisport, db=nredisdb)
        p = r.pipeline(transaction=False)
        for goodsid, timeseries in df.items():
            k = 'min1:{:0>7}'.format(goodsid)
            for i in timeseries:
                time = i.time + 200000000000
                if ntoday == time // 10000:
                    v = getjv(i)
                    p.rpush(k, v)
                    print("{}:{}".format(k, v))

            p.execute()
            print('finish key: {}'.format(k))


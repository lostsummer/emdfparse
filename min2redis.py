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
    filename = arguments["-f"]
    redishost = arguments["<host>"]
    redisport = int(arguments["-p"]) if arguments["-p"] != None else 6379
    today = arguments["-d"]
    redisdb = arguments["-b"]
    nredisdb = 1 if (not redisdb) else int(redisdb)


    df = DataFile(filename, Day)
    if today:
        todaynum = int(today)
        cli = redis.Redis(host=redishost, port=redisport, db=nredisdb)
        pl = cli.pipeline(transaction=False)
        sumkeys = 0
        for goodsid, timeseries in df.items():
            key = 'min1:{:0>7}'.format(goodsid)
            print("{}:".format(key))
            havetoday = False
            for i in timeseries:
                datenum = (i.time + 200000000000) // 10000
                if todaynum == datenum:
                    value = getjv(i)
                    pl.rpush(key, value)
                    print("{}:{}".format(key, value))
                    havetoday = True
            if havetoday:
                pl.execute()
                sumkeys += 1
        print('complete to write {s} keys of {t}'\
                .format(s=sumkeys, t=today))


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

if __name__ == '__main__':
    arguments = docopt(__doc__, version='min2redis {}'.format(__version__))
    datafile = arguments["-f"]
    redishost = arguments["<host>"]
    redisport = int(arguments["-p"]) if arguments["-p"] != None else 6379
    today = arguments["-d"]
    redisdb = arguments["-b"]
    nredisdb = 1 if (not redisdb) else int(redisdb)

    print("file: {}".format(datafile))
    print("host: {}".format(redishost))
    print("port: {}".format(redisport))

    df = DataFile(datafile, Day)
    if today :
        r = redis.Redis(host=redishost, port=redisport, db=nredisdb)
        p = r.pipeline(transaction=False)
        for goodsid in df.goodsidx:
            k = 'min1:{:0>7}'.format(goodsid)
            ts = df.getgoodstms(goodsid)
            for i in ts:
                time = i.time + 200000000000
                ntoday = int(today)
                if ntoday == time // 10000:
                    data = {
                                'time':time,
                                'open':i.open/1000.0,
                                'high':i.high/1000.0,
                                'low':i.low/1000.0,
                                'close':i.close/1000.0,
                                'volume':i.volume,
                                'amount':i.amount
                            }
                    v = json.dumps(data)
                    print("{}:{}".format(k, v))
                    p.rpush(k, v)
            p.execute()


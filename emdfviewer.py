#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
emdfviewer

Usage:
  emdfviewer -h | --help | --version
  emdfviewer -t <type> (-c| -a| -l| -i <goodsid>) <filename>

Arguments:
  filename          name of data file

Options:
  -h --help         show help
  -v --version      show version
  -t <type>         specify the file type ( d| m| h| b ) d: Day, m: Minute, h: HisMin, b: Bargain
  -c                output goods nubmer
  -a                ouput all goods time data in file
  -l                list goods id in file
  -i <goodsid>      output the time data of specified good
"""

import sys
import emdatafile as emdf
from docopt import docopt


class DfInfo(object):
    def __init__(self, filename, datacls):
        self.df = emdf.DataFile(filename, datacls)

    def printgoodscount(self):
        print(len(self.df))

    def printgoodsids(self):
        for gid in self.df:
            print(gid)

    def printgoodsbyid(self, gid):
        for d in self.df[gid]:
            print(d)

    def printgoodsall(self):
        for gid, tms in self.df.items():
            print("id:{0}".format(gid))
            for d in tms:
                print(d)

if __name__ == '__main__':
    arguments = docopt(__doc__, version="emdfviewer {0}".format(emdf.__version__))
    # 取得各个命令行参数及选项值
    filename = arguments["<filename>"]
    filetype = arguments["-t"]
    outputcounts = arguments["-c"]
    outputids = arguments["-l"]
    outputall = arguments["-a"]
    goodsid = arguments["-i"]
    # 命令行数据类型标识 => 数据类
    clstype = {
        "d": emdf.Day,
        "m": emdf.Minute,
        "h": emdf.HisMin,
        "b": emdf.Bargain,
    }
    datacls = clstype[filetype]
    dfinfo = DfInfo(filename, datacls)

    # 指定 -c
    if outputcounts:
        dfinfo.printgoodscount()

    # 指定 -l
    elif outputids:
        dfinfo.printgoodsids()

    # 指定 -a
    elif outputall:
        dfinfo.printgoodsall()

    # 指定 -i <goodsid>
    elif goodsid:
        gid = int(goodsid)
        dfinfo.printgoodsbyid(gid)

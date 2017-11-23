#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
dfparse

Usage:
  dfparse -h | --help | --version
  dfparse -t <type> (-a| -l| -i <goodsid>) <filename>

Arguments:
  filename          name of data file

Options:
  -h --help         show help
  -v --version      show version
  -t <type>         specify the file type ( d| m| b )
  -a                ouput all goods time data in file
  -l                list goods id in file
  -i <goodsid>      output the time data of specified good
"""

import os
import sys
import struct
import ctypes
from docopt import docopt

__author__ = "wangyx"
__version__ = "0.7.5"

DATAFILE_HEADER = "EM_DataFile"
DATAFILE2_HEADER = "EM_DataFile2"
DF_BLOCK_SIZE = 8192
DF2_BLOCK_SIZE = 65536
DF_BLOCK_GROWBY = 64
DF_MAX_GOODSUM = 21840
SIZEOF_DATA_FILE_INFO = 0x100
SIZEOF_DATA_FILE_GOODS = 0x30
SIZEOF_DATA_FILE_HEAD = SIZEOF_DATA_FILE_INFO + \
    SIZEOF_DATA_FILE_GOODS * DF_MAX_GOODSUM

LEN_STOCKCOE = 24


def xint32value(x):
    v = ctypes.c_uint32(x).value
    return v % (2**29) * (16**(v >> 29))


class DataBase(object):
    fmt = '5I'
    brieflist = ['time', 'open', 'high', 'low', 'close']

    @classmethod
    def getsize(cls):
        return struct.calcsize(cls.fmt)

    def __init__(self):
        self.time = 0
        self.open = 0
        self.high = 0
        self.low = 0
        self.close = 0

    def printbrief(self):
        fields = []
        od = vars(self)
        for i in self.brieflist:
            if i in od:
                fields.append("{0:4}:{1:<12}".format(i, od[i]))
        s = "".join(fields)
        print(s)

    def getbrief(self):
        fields = {}
        od = vars(self)
        for i in self.brieflist:
            if i in od:
                fields[i] = od[i]
        return fields


class Day(DataBase):
    fmt = '=23I2hi'
    brieflist = ['time', 'open', 'high', 'low', 'close', 'volume', 'amount']

    def __init__(self):
        self.time = 0
        self.open = 0
        self.high = 0
        self.low = 0
        self.close = 0
        self.tradenum = 0
        self.volume = 0
        self.amount = 0
        self.neipan = 0
        self.buy = 0
        self.sell = 0
        self.volbuy = [0, 0, 0]
        self.volsell = [0, 0, 0]
        self.amtbuy = [0, 0, 0]
        self.amtsell = [0, 0, 0]
        self.rise = 0
        self.fall = 0
        self.reserve = 0

    def read(self, data):
        (self.time,
            self.open,
            self.high,
            self.low,
            self.close,
            self.tradenum,
            self.volume,
            self.amount,
            self.neipan,
            self.buy,
            self.sell,
            self.volbuy[0],
            self.volbuy[1],
            self.volbuy[2],
            self.volsell[0],
            self.volsell[1],
            self.volsell[2],
            self.amtbuy[0],
            self.amtbuy[1],
            self.amtbuy[2],
            self.amtsell[0],
            self.amtsell[1],
            self.amtsell[2],
            self.rise,
            self.fall,
            self.reserve) = struct.unpack(self.fmt, data)
        self.volume = xint32value(self.volume)
        self.amount = xint32value(self.amount)
        self.neipan = xint32value(self.neipan)
        self.volbuy = [xint32value(x) for x in self.volbuy]
        self.volsell = [xint32value(x) for x in self.volsell]
        self.amtbuy = [xint32value(x) for x in self.amtbuy]
        self.amtsell = [xint32value(x) for x in self.amtsell]


class OrderCounts():
    def __init__(self):
        self.numbuy = [0, 0, 0, 0]
        self.numsell = [0, 0, 0, 0]
        self.volbuy = [0, 0, 0, 0]
        self.volsell = [0, 0, 0, 0]
        self.amtbuy = [0, 0, 0, 0]
        self.amtsell = [0, 0, 0, 0]


class Minute(DataBase):
    fmt = '=66I2h3i'
    brieflist = ['time', 'close', 'ave', 'amount']

    def __init__(self):
        self.time = 0
        self.open = 0
        self.high = 0
        self.low = 0
        self.close = 0
        self.volume = 0
        self.amount = 0
        self.tradenum = 0
        self.ave = 0
        self.sell = 0
        self.buy = 0
        self.volsell = 0
        self.volbuy = 0
        self.order = OrderCounts()
        self.trade = OrderCounts()
        self.neworder = [0, 0]
        self.delorder = [0, 0]
        self.strong = 0
        self.rise = 0
        self.fall = 0
        self.volsell5 = 0
        self.volbuy = 0
        self.count = 0

    def read(self, data):
        (self.time,
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
            self.amount,
            self.tradenum,
            self.ave,
            self.buy,
            self.sell,
            self.volbuy,
            self.volsell,
            self.order.numbuy[0],
            self.order.numbuy[1],
            self.order.numbuy[2],
            self.order.numbuy[3],
            self.order.numsell[0],
            self.order.numsell[1],
            self.order.numsell[2],
            self.order.numsell[3],
            self.order.volbuy[0],
            self.order.volbuy[1],
            self.order.volbuy[2],
            self.order.volbuy[3],
            self.order.volsell[0],
            self.order.volsell[1],
            self.order.volsell[2],
            self.order.volsell[3],
            self.order.amtbuy[0],
            self.order.amtbuy[1],
            self.order.amtbuy[2],
            self.order.amtbuy[3],
            self.order.amtsell[0],
            self.order.amtsell[1],
            self.order.amtsell[2],
            self.order.amtsell[3],
            self.trade.numbuy[0],
            self.trade.numbuy[1],
            self.trade.numbuy[2],
            self.trade.numbuy[3],
            self.trade.numsell[0],
            self.trade.numsell[1],
            self.trade.numsell[2],
            self.trade.numsell[3],
            self.trade.volbuy[0],
            self.trade.volbuy[1],
            self.trade.volbuy[2],
            self.trade.volbuy[3],
            self.trade.volsell[0],
            self.trade.volsell[1],
            self.trade.volsell[2],
            self.trade.volsell[3],
            self.trade.amtbuy[0],
            self.trade.amtbuy[1],
            self.trade.amtbuy[2],
            self.trade.amtbuy[3],
            self.trade.amtsell[0],
            self.trade.amtsell[1],
            self.trade.amtsell[2],
            self.trade.amtsell[3],
            self.neworder[0],
            self.neworder[1],
            self.delorder[0],
            self.delorder[1],
            self.strong,
            self.rise,
            self.fall,
            self.volsell5,
            self.volbuy5,
            self.count) = struct.unpack(self.fmt, data)
        self.amount = xint32value(self.amount)


class Bargain(DataBase):
    fmt = '=5Ib'
    brieflist = ['date', 'time', 'price', 'volume', 'tradenum', 'bs']

    def __init__(self):
        self.date = 0
        self.time = 0
        self.price = 0
        self.volume = 0
        self.tradenum = 0
        self.bs = 0

    def read(self, data):
        (self.date,
            self.time,
            self.price,
            self.volume,
            self.tradenum,
            self.bs) = struct.unpack(self.fmt, data)

class HisMin(DataBase):
    fmt = '=5I'
    brieflist = ['time', 'price', 'ave', 'volume', 'zjjl']

    def __init__(self):
        self.time = 0
        self.price = 0
        self.ave = 0
        self.volume = 0
        self.zjjl = 0

    def read(self, data):
        (self.time,
            self.price,
            self.ave,
            self.volume,
            self.zjjl) = struct.unpack(self.fmt, data)
        self.volume = xint32value(self.volume)
        self.zjjl = xint32value(self.zjjl)

class TMSReader():
    def __init__(self, datacls):
        self._datacls = datacls

    def read(self, data):
        plist = []
        length = len(data)
        step = self._datacls.getsize()
        for i in range(length // step):
            start = i * step
            end = (i + 1) * step
            block = data[start:end]
            point = self._datacls()
            point.read(block)
            plist.append(point)
        return plist


class DataFileInfo():
    def __init__(self):
        self.header = ""
        self.version = 0
        self.blockstotal = 0
        self.blocksuse = 0
        self.goodsnum = 0
        self.reserved = ""

    def read(self, data):
        (
            self.header,
            self.version,
            self.blockstotal,
            self.blocksuse,
            self.goodsnum,
            self.reserved
        ) = struct.unpack('32s4I208s', data)


class DataFileGoods():

    def __int__(self):
        self.goodsid = 0
        self.datanum = 0
        self.blockfirst = 0
        self.blockdata = 0
        self.blocklast = 0
        self.datalastidx = 0
        self.code = ""

    def read(self, data):
        (
            self.goodsid,
            self.datanum,
            self.blockfirst,
            self.blockdata,
            self.blocklast,
            self.datalastidx,
            self.code
        ) = struct.unpack('6I{0}s'.format(LEN_STOCKCOE), data)


class DataFileHead():

    def __init__(self):
        self.info = DataFileInfo()
        self.goodslist = [DataFileGoods() for i in range(DF_MAX_GOODSUM)]

    def read(self, data):
        self.info.read(data[0:SIZEOF_DATA_FILE_INFO])

        start = SIZEOF_DATA_FILE_INFO
        end = start + SIZEOF_DATA_FILE_GOODS
        for goods in self.goodslist:
            goods.read(data[start:end])
            start = end
            end += SIZEOF_DATA_FILE_GOODS


class DataFile():
    def __init__(self, filename, datacls):
        self.filename = filename
        try:
            self.f = open(self.filename, 'rb')
        except Exception as e:
            print(e)
            exit(1)
        self.head = DataFileHead()
        self.goodsidx = {}
        self.datasize = datacls.getsize()
        self.blockdatanum = (DF_BLOCK_SIZE - 4) // self.datasize
        self.tmsreader = TMSReader(datacls)
        self._readhead()

    def __def__(self):
        self.f.close()

    def _getraw(self, goodsid):
        index = self.goodsidx[goodsid]
        datanum = self.head.goodslist[index].datanum
        blockid = self.head.goodslist[index].blockfirst
        readtime = (datanum - 1) // self.blockdatanum + 1
        data = b''
        #with open(self.filename, mode='rb') as f:
        try:
            for i in range(readtime):
                offset = blockid * DF_BLOCK_SIZE
                if offset > self.length():
                    break
                self.f.seek(offset, 0)
                nextblockid, = struct.unpack('I', self.f.read(4))
                if nextblockid > self.head.goodslist[index].blocklast:
                    break
                if i == readtime - 1:
                    length = (datanum %
                              self.blockdatanum) * self.datasize
                else:
                    length = self.blockdatanum * self.datasize
                data += self.f.read(length)
                blockid = nextblockid
        except Exception as e:
            print(e)
            exit(1)
        return data

    def getgoodstms(self, goodsid):
        return self.tmsreader.read(self._getraw(goodsid))

    def printgoodstms(self, goodsid):
        ts = self.getgoodstms(goodsid)
        for unit in ts:
            unit.printbrief()

    def _readhead(self):
        try:
            self.f.seek(0)
            data = self.f.read(SIZEOF_DATA_FILE_HEAD)
        except Exception as e:
            print(e)
            exit(1)

        self.head.read(data)
        for i in range(self.head.info.goodsnum):
            self.goodsidx[self.head.goodslist[i].goodsid] = i

    def length(self):
        return os.path.getsize(self.filename)

    def printgoodsids(self):
        for i in self.goodsidx:
            print(i)

    def printallgoods(self):
        for i in self.goodsidx:
            print("id:{0}".format(i))
            self.printgoodstms(i)


if __name__ == '__main__':
    arguments = docopt(__doc__, version="dfparse {0}".format(__version__))

    filename = arguments["<filename>"]
    filetype = arguments["-t"]
    goodsid = arguments["-i"]
    listids = arguments["-l"]
    printallgoods = arguments["-a"]

    clstype = {
        "d": Day,
        "m": Minute,
        "h": HisMin,
        "b": Bargain,
    }

    datacls = clstype[filetype]
    df = DataFile(filename, datacls)

    if printallgoods:
        df.printallgoods()
        sys.exit()

    if listids:
        df.printgoodsids()
        sys.exit()

    df.printgoodstms(int(goodsid))

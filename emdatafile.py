#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os
import sys
import struct
import ctypes
import platform
import threading
import traceback

__author__ = "wangyx"
__version__ = "0.8.2"

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


_IS_WINDOWS = True if platform.system() == 'Windows' else False
_IS_PY2 = True if platform.python_version_tuple()[0] == '2' else False

def saferead(lock, fd, size, offset):
    """linux python3 提供线程安全的原子读os.pread, 此外版本多线程使用需要加锁

    :param lock: 线程锁
    :param fd:   打开的文件描述符
    :param size: 读取长度
    :returns:    读出的数据
    """
    if _IS_WINDOWS or _IS_PY2:
        with lock:
            os.lseek(fd, offset, os.SEEK_SET)
            return os.read(fd, size)
    else:
        return  os.pread(fd, size, offset)


def safewrite(lock, fd, data, offset):
    """linux python3 提供线程安全的原子读os.pread, 此外版本多线程使用需要加锁

    :param lock: 线程锁
    :param fd:   打开的文件描述符
    :param size: 读取长度
    :returns:    读出的数据
    """
    if _IS_WINDOWS or _IS_PY2:
        with lock:
            os.lseek(fd, offset, os.SEEK_SET)
            return os.write(fd, data)
    else:
        return  os.pwrite(fd, data, offset)


def xint32value(x):
    """相当于先后调用CPP代码中XInt32的SetRawData()和GetValue()方法

    :param x: 未解析前32位数据
    :returns: 解析出来的实际值

    """
    v = ctypes.c_uint32(x).value
    # 低29位为基数
    base = v & 0x1FFFFFFF
    # 29位为基数符号位, 补码规则取其值
    if base & 0x10000000:
        base = ~base + 1
        base &= 0x1FFFFFFF
        base = -base
    # 高三位为指数
    return base * (16 ** (v >> 29))


class DataBase(object):
    """数据单元基类.

    实现可打印方法__str__,
    打印brieflist中定义的摘要字段.
    类变量fmt, brieflist应被子类覆盖.


    """
    fmt = '5I'
    brieflist = ['time', 'open', 'high', 'low', 'close']

    @classmethod
    def getsize(cls):
        """ """
        return struct.calcsize(cls.fmt)

    def __init__(self):
        self.time = 0
        self.open = 0
        self.high = 0
        self.low = 0
        self.close = 0

    def __str__(self):
        fields = []
        od = vars(self)
        for i in self.brieflist:
            if i in od:
                fields.append("{0:4}:{1:<12}".format(i, od[i]))
        return "".join(fields)


class Day(DataBase):
    """对应CPP中结构CDay"""
    fmt = '=23I2hi'
    brieflist = ['time', 'open', 'high', 'low', 'close', 'volume', 'amount']

    def __init__(self):
        self.time = 0
        self.open = 0
        self.high = 0
        self.low = 0
        self.close = 0
        self.tradenum = 0
        self._volume = 0
        self._amount = 0
        self._neipan = 0
        self.buy = 0
        self.sell = 0
        self._volbuy = [0, 0, 0]
        self.volbuy = [0, 0, 0]
        self._volsell = [0, 0, 0]
        self.volsell = [0, 0, 0]
        self._amtbuy = [0, 0, 0]
        self.amtbuy = [0, 0, 0]
        self._amtsell = [0, 0, 0]
        self.amtsell = [0, 0, 0]
        self.rise = 0
        self.fall = 0
        self.reserve = 0

    def read(self, data):
        """

        :param data: 原始bin数据

        """
        (
            self.time,
            self.open,
            self.high,
            self.low,
            self.close,
            self.tradenum,
            self._volume,
            self._amount,
            self._neipan,
            self.buy,
            self.sell,
            self._volbuy[0],
            self._volbuy[1],
            self._volbuy[2],
            self._volsell[0],
            self._volsell[1],
            self._volsell[2],
            self._amtbuy[0],
            self._amtbuy[1],
            self._amtbuy[2],
            self._amtsell[0],
            self._amtsell[1],
            self._amtsell[2],
            self.rise,
            self.fall,
            self.reserve
        ) = struct.unpack(self.fmt, data)
        self.volume = xint32value(self._volume)
        self.amount = xint32value(self._amount)
        self.neipan = xint32value(self._neipan)
        self.volbuy = [xint32value(x) for x in self._volbuy]
        self.volsell = [xint32value(x) for x in self._volsell]
        self.amtbuy = [xint32value(x) for x in self._amtbuy]
        self.amtsell = [xint32value(x) for x in self._amtsell]

    def pack(self):
        return struct.pack(self.fmt,
            self.time,
            self.open,
            self.high,
            self.low,
            self.close,
            self.tradenum,
            self._volume,
            self._amount,
            self._neipan,
            self.buy,
            self.sell,
            self._volbuy[0],
            self._volbuy[1],
            self._volbuy[2],
            self._volsell[0],
            self._volsell[1],
            self._volsell[2],
            self._amtbuy[0],
            self._amtbuy[1],
            self._amtbuy[2],
            self._amtsell[0],
            self._amtsell[1],
            self._amtsell[2],
            self.rise,
            self.fall,
            self.reserve
        )


class OrderCounts():
    """对应CPP中结构COrderCounts"""
    def __init__(self):
        self.numbuy = [0, 0, 0, 0]
        self.numsell = [0, 0, 0, 0]
        self.volbuy = [0, 0, 0, 0]
        self.volsell = [0, 0, 0, 0]
        self.amtbuy = [0, 0, 0, 0]
        self.amtsell = [0, 0, 0, 0]


class Minute(DataBase):
    """对应CPP中结构CMinute"""
    fmt = '=66I2h3i'
    brieflist = ['time', 'close', 'ave', 'amount']

    def __init__(self):
        self.time = 0
        self.open = 0
        self.high = 0
        self.low = 0
        self.close = 0
        self.volume = 0
        self._amount = 0
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
        """

        :param data: 原始bin数据

        """
        (
            self.time,
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
            self._amount,
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
            self.count
        ) = struct.unpack(self.fmt, data)
        self.amount = xint32value(self._amount)

    def pack(self):
        return struct.pack(self.fmt,
            self.time,
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
            self._amount,
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
            self.count
        )



class Bargain(DataBase):
    """对应CPP中结构CBargain"""
    fmt = '=5Ib'
    brieflist = ['date', 'time', 'price', 'volume', 'tradenum', 'bs']

    def __init__(self):
        self.date = 0
        self.time = 0
        self.price = 0
        self._volume = 0
        self.volume = 0
        self.tradenum = 0
        self.bs = 0

    def read(self, data):
        """

        :param data: 原始bin数据

        """
        (
            self.date,
            self.time,
            self.price,
            self._volume,
            self.tradenum,
            self.bs
        ) = struct.unpack(self.fmt, data)
        self.volume = xint32value(self._volume)

    def pack(self):
        return struct.pack(self.fmt,
            self.date,
            self.time,
            self.price,
            self._volume,
            self.tradenum,
            self.bs
        )

class HisMin(DataBase):
    """对应CPP中结构CHisMin"""
    fmt = '=5I'
    brieflist = ['time', 'price', 'ave', 'volume', 'zjjl']

    def __init__(self):
        self.time = 0
        self.price = 0
        self.ave = 0
        self._volume = 0
        self.volume = 0
        self._zjjl = 0
        self.zjjl = 0

    def read(self, data):
        """

        :param data: 原始bin数据

        """
        (
            self.time,
            self.price,
            self.ave,
            self._volume,
            self._zjjl
        ) = struct.unpack(self.fmt, data)
        self.volume = xint32value(self._volume)
        self.zjjl = xint32value(self._zjjl)

    def pack(self):
        return struct.pack(self.fmt,
            self.time,
            self.price,
            self.ave,
            self._volume,
            self._zjjl
        )

class DataFileInfo():
    """对应CPP中CDataFileInfo"""
    fmt = '32s4I208s'
    def __init__(self, version=1):
        self.header = DATAFILE_HEADER if version == 1 else DATAFILE2_HEADER
        self._header = self.header.encode('ascii')
        self.version = 0
        self.blockstotal = 0
        self.blocksuse = 0
        self.goodsnum = 0
        self.reserved = ""

    def read(self, data):
        """

        :param data: 原始bin数据

        """
        (
            self._header,
            self.version,
            self.blockstotal,
            self.blocksuse,
            self.goodsnum,
            self.reserved
        ) = struct.unpack(self.fmt, data)
        self.header = self._header.decode('ascii')

    def pack(self):
        return struct.pack(self.fmt,
            self._header,
            self.version,
            self.blockstotal,
            self.blocksuse,
            self.goodsnum,
            self.reserved
        )


class DataFileGoods():
    """对应CPP中CDataFileGoods"""
    fmt = '6I{0}s'.format(LEN_STOCKCOE)

    def __init__(self):
        self.goodsid = 0
        self.datanum = 0
        self.blockfirst = 0
        self.blockdata = 0
        self.blocklast = 0
        self.datalastidx = 0
        self.code = ""

    def read(self, data):
        """

        :param data: 原始bin数据

        """
        (
            self.goodsid,
            self.datanum,
            self.blockfirst,
            self.blockdata,
            self.blocklast,
            self.datalastidx,
            self.code
        ) = struct.unpack(self.fmt, data)

    def pack(self):
        return struct.pack(self.fmt,
            self.goodsid,
            self.datanum,
            self.blockfirst,
            self.blockdata,
            self.blocklast,
            self.datalastidx,
            self.code
        )


class DataFileHead():
    """对应CPP中CDataFileHead"""
    def __init__(self, version=1):
        self.info = DataFileInfo(version)
        self.dfgs = [DataFileGoods() for i in range(DF_MAX_GOODSUM)]

    def read(self, data):
        """

        :param data: 原始bin数据

        """
        self.info.read(data[0:SIZEOF_DATA_FILE_INFO])

        start = SIZEOF_DATA_FILE_INFO
        step = SIZEOF_DATA_FILE_GOODS
        end = start + step
        for g in self.dfgs:
            g.read(data[start:end])
            start = end
            end += step

    def pack(self):
        infodata = self.info.pack()
        dfgsdata = b''.join([i.pack() for i in self.dfgs])
        return infodata + dfgsdata


class DataFile():
    """对应CPP中CDataFile

    self.goodsidx 对应CPP中m_aGoodsIndex, id => index 字典


    """
    def __init__(self, filename, datacls):
        self.filename = filename
        self.datacls = datacls
        self.thlk = threading.RLock()
        self.head = DataFileHead()
        self.goodsidx = {}
        try:
            if os.path.exists(self.filename):
                self._f = os.open(self.filename, os.O_RDWR)
                self._readhead()
            else:
                self._f = os.open(self.filename, os.O_CREAT|os.O_RDWR)
                self._writehead()
            self.filesize = os.path.getsize(self.filename)
        except Exception as e:
            traceback.print_exc()
            sys.exit(1)
        if self.head.info.header[:12] == DATAFILE2_HEADER:
            self.blocksize = DF2_BLOCK_SIZE
            self.version = 2
        else:
            self.blocksize = DF_BLOCK_SIZE
            self.version = 1
        self.datasize = datacls.getsize()
        self.blockdatanum = (self.blocksize - 4) // self.datasize

    def __del__(self):
        try:
            os.close(self._f)
        except Exception as e:
            traceback.print_exc()

    def __iter__(self):
        self.goodsidxiter = iter(self.goodsidx)
        return self

    def __next__(self):
        goodsid = self.goodsidxiter.__next__()
        return goodsid

    def next(self):
        goodsid = self.goodsidxiter.next()
        return goodsid

    def readat(self, size, offset):
        return saferead(self.thlk, self._f, size, offset)

    def writeat(self, data, offset):
        safewrite(self.thlk, self._f, data, offset)

    def items(self):
        """实现类似字典items列表方法, 生成器语法, key为goodsid, value为时序数据."""
        return ((i, self.getgoodstms(i)) for i in self)

    def _getgoodsraw(self, goodsid):
        """读取并连接一只股票的原始数据块.

        :param goodsid: 股票id
        :returns: 拼接好的连续原始数据
        """
        index = self.goodsidx[goodsid]
        datanum = self.head.dfgs[index].datanum
        blockid = self.head.dfgs[index].blockfirst
        readtime = (datanum - 1) // self.blockdatanum + 1
        try:
            for i in range(readtime):
                offset = blockid * self.blocksize
                if offset > self.filesize:
                    break
                nextblockid, = struct.unpack('I', self.readat(4, offset))
                if nextblockid > self.head.dfgs[index].blocklast:
                    break
                if i == readtime - 1:
                    length = (datanum %
                              self.blockdatanum) * self.datasize
                else:
                    length = self.blockdatanum * self.datasize
                blockid = nextblockid
                offset += 4
                block = self.readat(length, offset)
                yield block
        except Exception as e:
            traceback.print_exc()
            sys.exit(1)

    def getgoodstms(self, goodsid):
        """返回指定goodsid的股票时序数据

        :param goodsid: 股票id
        :returns: 指定股票的时序数据的生成器
        """
        blocks = self._getgoodsraw(goodsid)
        cls = self.datacls
        step = self.datasize
        head = 0
        buf = b''
        for block in blocks:
            blocklen = len(block)
            buflen = len(buf)
            if step > buflen > 0:
                head = step - buflen
                point = self._datacls()
                point.read(buf + block[:head])
                yield point
            buf = b''
            for start in range(head, blocklen, step):
                end = start + step
                if end <= blocklen:
                    point = cls()
                    point.read(block[start:end])
                    yield point
                else:
                    buf = block[start:]

    def __getitem__(self, gid):
        """重载下标运算符[], 返回一个指定股票的所有数据的list而不是生成器"""
        return [t for t in self.getgoodstms(gid)]

    def setgoodstms(gid, tms):
        pass

    def __setitem__(self, gid, tms):
        self.setgoodstms(gid, tms)

    def appendgoodstms(self, gid, tms):
        pass

    def _readhead(self):
        """读文件头部, 并生成一个 goodsid => index 字典 goodsidx"""
        try:
            data = self.readat(SIZEOF_DATA_FILE_HEAD, 0)
        except Exception as e:
            traceback.print_exc()
            sys.exit(1)

        self.head.read(data)
        for index in range(self.head.info.goodsnum):
            """
            goodsnum 有可能比实际股票数多,
            按此值会读到goodsid为0的, 需要排除
            """
            goodsid = self.head.dfgs[index].goodsid
            if goodsid > 0:
                self.goodsidx[goodsid] = index

    def _writehead(self):
        self.writeat(self.head.pack(), 0)

    def __len__(self):
        """len函数可获取DataFile对象中股票数量"""
        return len(self.goodsidx)


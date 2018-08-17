#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os
import sys
import struct
import ctypes
import traceback


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


def dataclasscommon(cls):
    """数据类通用方法装饰器

    """
    @classmethod
    def _getsize(kls):
        return struct.calcsize(kls.fmt)

    cls.getsize = _getsize

    def _str(obj):
        fields = []
        od = vars(obj)
        for i in obj.brieflist:
            if i in od:
                fields.append("{0:4}:{1:<12}".format(i, od[i]))
        return "".join(fields)

    cls.__str__ =  _str
    cls.__repr__ = _str

    return cls


@dataclasscommon
class Day:
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


class OrderCounts:
    """对应CPP中结构COrderCounts"""
    def __init__(self):
        self.numbuy = [0, 0, 0, 0]
        self.numsell = [0, 0, 0, 0]
        self.volbuy = [0, 0, 0, 0]
        self.volsell = [0, 0, 0, 0]
        self.amtbuy = [0, 0, 0, 0]
        self.amtsell = [0, 0, 0, 0]


@dataclasscommon
class Minute:
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


@dataclasscommon
class Bargain:
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


@dataclasscommon
class HisMin:
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


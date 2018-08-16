#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os
import sys
import struct
import ctypes
import platform
import threading
import traceback


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
        self.reserved = b'\x00' * 8 

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
        self.code = b'\x00' * LEN_STOCKCOE

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
    def __init__(self, filename, datacls, mode='r'):
        self.filename = filename
        self.datacls = datacls
        self.thlk = threading.RLock()
        self.head = DataFileHead()
        self.goodsidx = {}
        try:
            if os.path.exists(self.filename):
                self._f = os.open(self.filename, os.O_RDWR)
                self._readhead()
            elif mode == 'w':
                self._f = os.open(self.filename, os.O_CREAT|os.O_RDWR)
                self._writehead()
            else:
                print('{f} is not exist!'.format(f=filename))
                sys.exit(1)
            self._filesize = os.path.getsize(self.filename)
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
            if hasattr(self, '_f'):
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
                if offset > self._filesize:
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


# dfparse

## 用途

解析DS几种落地文件(基于移动加强版Linux MDS, 猜测和金融平台使用的文件格式相同)

- K线类型: Day.dat, Day_*.dat, Min1.dat, Min1_*.dat, Min5.dat, Min5_*.dat, Min30.dat, Min30_*.dat 等 
- 分时数据: Minute.dat_n 等
- 成交数据: Bargain.dat_n 等
- HisMin (我不知道怎么文字描述这个类型): HisMin.dat_n

可作为一个包供其他python脚本使用，也可以作为一个打印数据到标准输出的命令行工具使用

### 作为包

参考min2redis.py使用DataFile类的方法

### 作为命令行工具

```
Usage:
  dfparse -h | --help | --version
  dfparse -t <type> (-a| -l| -i <goodsid>) <filename>

Arguments:
  filename          name of data file

Options:
  -h --help         show help
  -v --version      show version
  -t <type>         specify the file type ( d| m| h| b ) d: Day, m: Minute, h: HisMin, b: Bargain
  -a                ouput all goods time data in file
  -l                list goods id in file
  -i <goodsid>      output the time data of specified good

```

示例:

1. 列出Day.dat 中所有股票
```
python dfparse.py -t d -l Day.dat

1835474
136611
1835086
1833114
1835479
1835483
1835485
1835486
1835488
1835489
1835490
1150067
1835492
1107513
1835494
1835495
1870213
1835501
1835502
1835503
...

```
后接 "|less" 可浏览, 接 "|wc -l" 可统计股票数量, 不赘述


2. 列出HisMin.dat_1中goodsId为1的股票数据(最后先用1命令查看文件中是否有该goodsId, -i 指定文件中不存在的goodsId会抛KeyError异常)
```
python dfparse.py -t h -i 1 HisMin.dat_1|less

time:1711130931  price:3439398     ave :3438242     volume:4194014     zjjl:105805922
time:1711130932  price:3439423     ave :3437565     volume:1872804     zjjl:366456452
time:1711130933  price:3440603     ave :3437058     volume:1744091     zjjl:439886382
time:1711130934  price:3442064     ave :3437636     volume:1878208     zjjl:464182285
time:1711130935  price:3443901     ave :3438182     volume:1949975     zjjl:423776842
time:1711130936  price:3445536     ave :3438190     volume:1760475     zjjl:496812106
time:1711130937  price:3446067     ave :3437862     volume:1827598     zjjl:487830651
...
```

3. 列出Bargain.dat_1中所有股票数据(数据较多)
```
python dfparse.py -t b -a Bargain.dat_1

id:1002161
date:100548      time:14830       price:20940       volume:26253       tradenum:1           bs  :0
date:100551      time:14830       price:223500      volume:26287       tradenum:255         bs  :0
date:100554      time:14830       price:11100       volume:26301       tradenum:255         bs  :0
date:100557      time:14830       price:8600        volume:26308       tradenum:255         bs  :0
date:100600      time:14830       price:15800       volume:26324       tradenum:255         bs  :0
date:100603      time:14830       price:14800       volume:26333       tradenum:255         bs  :0
...

id:1002201
date:101039      time:5730        price:6400        volume:5107        tradenum:1           bs  :0
date:101042      time:5730        price:1700        volume:5110        tradenum:1           bs  :0
date:101045      time:5720        price:35400       volume:5114        tradenum:255         bs  :0
date:101048      time:5720        price:800         volume:5115        tradenum:255         bs  :0
date:101051      time:5720        price:87600       volume:5144        tradenum:255         bs  :0
date:101054      time:5720        price:7000        volume:5151        tradenum:255         bs  :0
...
```

__注__: 2, 3 命名打印的可能并不是指定数据类型的所有字段, 可以根据需要修改Day, Minute等数据子类的brieflist, 或重写覆盖基类printbrief方法


# min2redis.py

使用dfparse解析Min1.dat数据文件并写入redis的一个示例小程序, 写入redis的数据格式参见 [K Line](http://git.emoney.cn/EMStockData/DataImporter/wikis/k-line)


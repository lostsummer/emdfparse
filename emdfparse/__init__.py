#!/usr/bin/env python

"""
emdfparse tool
~~~~~~~~~~~~

emdfparse is a moudle for parse DS data file day.dat, min1.dat, minute.dat, and so on.
usage:

    >>> from emdfparse import DataFile, Day
    >>> df = DataFile('/usr/local/EMoney/Data/Day.dat', Day)
    >>> for id, tms in df.items():
            print('id: {}'.format(id))
            print(tms)

    id: 1
    time:20171009    open:3403245     high:3410170     low :3366965     close:3374378     volume:191736057   amount:227440594944
    time:20171016    open:3393205     high:3400511     low :3374768     close:3378470     volume:174330620   amount:221650690048
    ...
"""


from emdfparse.datafile import *
from emdfparse.datatype import *

__author__ = "yushin"
__version__ = "1.0.3"

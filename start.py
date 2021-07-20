#!/usr/bin/python
from service import prpinsdb3
import datetime
import time
import sys
import os
print("程序开始运行...")
prpins = prpinsdb3()

def start():
    startdate = prpins.getMaxCarUpdate()
    startdate = (startdate + datetime.timedelta(1)).strftime("%Y-%m-%d")
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    while startdate < today:
        print(f"提取数据  {startdate}")
        prpins.saveClaimCar(startdate=startdate)
        prpins.saveClaimProp(startdate=startdate)
        time.sleep(2)
        startdate = (datetime.datetime.strptime(startdate, "%Y-%m-%d") + datetime.timedelta(1)).strftime("%Y-%m-%d")
    print(f"数据提取完成")

def start_1(startdate):
    print(f"提取单日数据  {startdate}")
    prpins.saveClaimCar(startdate=startdate)
    prpins.saveClaimProp(startdate=startdate)
    print(f"数据提取完成")

if len(sys.argv) == 1:
    print("开始自动提取业务数据")
    start()

else:
#     #print(sys.argv[1])
    start_1(sys.argv[1])

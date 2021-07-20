
import pymysql
from base import MyDbUtil
from lib import cwp
import config

class prpinsdb3():
    def __init__(self):
        self.conn=pymysql.connect(host=config.DB_HOST,user=config.DB_USER,password=config.DB_PWD,
                        database=config.DB_DATABASE,cursorclass=pymysql.cursors.DictCursor)
        self.cwp = cwp.down_cwp()
        # self.itemcar = MyDbUtil(conn=self.conn,tabname='item_car',id='endorseno')

    def saveClaimCar(self,startdate):
        newclaim = MyDbUtil(conn=self.conn, tabname='newclaim', id='endorseno', fileds=config.newclaim_fields)
        data = self.cwp.getCarClaim(startdate)
        for i, item in enumerate(data):
            if item['endorseno'] == '': continue
            newclaim.new()
            for k,v in item.items():
                newclaim[k]=v

            newclaim['riskcode'] = newclaim['endorseno'][1:4]
            newclaim['classcode'] = '05'
            if newclaim['endorseno'][0:1] == 'P':
                newclaim['documenttype'] = '01'
            else:
                newclaim["documenttype"] = '02'
            newclaim.replace()
            print(f"\r car-no: {i + 1} endorseno: {newclaim['endorseno']} -- success",end='')
    def saveClaimProp(self,startdate):
        newclaim = MyDbUtil(conn=self.conn, tabname='newclaim', id='endorseno', fileds=config.newclaim_fields)
        data = self.cwp.getPropClaim(startdate)
        for i, item in enumerate(data):
            if item['endorseno'] == '': continue
            newclaim.new()
            for k,v in item.items():
                newclaim[k]=v
            newclaim['riskcode'] = newclaim['endorseno'][1:4]
            newclaim['classcode'] = '02'
            if newclaim['endorseno'][0:1] == 'P':
                newclaim['documenttype'] = '01'
            else:
                newclaim["documenttype"] = '02'
            newclaim.replace()
            print(f"\r prop-no: {i + 1} endorseno: {newclaim['endorseno']} -- success",end='')
    def saveItemCar(self,startdate):
        itemcar = MyDbUtil(conn=self.conn,tabname='item_car',id='endorseno')
        data = self.cwp.getCarItem(startdate)
        for i, item in enumerate(data):
            if item['endorseno'] == '': continue
            for k,v in item.items():
                itemcar[k]=v
            itemcar.replace()
            print(f"\r no: {i + 1} endorseno: {item['endorseno']} -- success",end='')

    def getMaxCarUpdate(self):
        sql_car_max = "SELECT MAX(operatedate) as operatedate  FROM newclaim WHERE classcode='05'"
        cur = self.conn.cursor()
        cur.execute(sql_car_max)
        car_data = cur.fetchone()
        cur.close()
        return car_data["operatedate"]

    def getMaxPropUpdate(self):
        sql_car_max = "SELECT MAX(operatedate) as operatedate  FROM newclaim WHERE classcode!='05'"
        cur = self.conn.cursor()
        cur.execute(sql_car_max)
        car_data = cur.fetchone()
        cur.close()
        return car_data["operatedate"]




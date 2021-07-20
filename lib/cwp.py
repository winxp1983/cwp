#协同平台数据提取
import datetime
import requests as req
import pandas as pd
import os
class down_cwp():
    def __init__(self):
        self.headers={
          "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.146 Safari/537.36"
        }

    # 车险清分数据
    def getCarClaim(self,startdate):
        url = "http://64.17.32.199/item/service/newclaimItemList"
        page = 1
        limit=1000
        data = []
        while True:
            params = {
                "datatype": "json",
                "comcodeClaim": "510123",
                "hrcode": "09034455",
                "comcode": "",
                "risktype": "cx",
                "querydatestart": startdate,
                "querydateend": startdate,
                "splittype": "1",
                "firstchannel": "",
                "secondchannel": "",
                "caliber": "cd_item",
                "page": page,
                "start": "0",
                "usercodeClaim": "",
                "limit": limit,
                "jsonPCallback": "Ext.data.JsonP.callback7"
            }
            res = req.get(url=url, params=params)
            if res.status_code == 200:
                res = res.json()
                data = data + res
                if len(res) < limit:
                    break
                else:
                    page +=1
        return data

    # 非车险清分数据
    def getPropClaim(self,startdate):
        url = "http://64.17.32.199/item/service/newclaimItemList"
        page = 1
        limit=1000
        data = []
        while True:
            params = {
                "datatype": "json",
                "comcodeClaim": "510123",
                "hrcode": "09034455",
                "comcode": "",
                "risktype": "fcx",
                "querydatestart": startdate,
                "querydateend": startdate,
                "splittype": "1",
                "firstchannel": "",
                "secondchannel": "",
                "caliber": "cd_item",
                "page": page,
                "start": "0",
                "usercodeClaim": "",
                "limit": limit,
                "jsonPCallback": "Ext.data.JsonP.callback9"
            }
            res = req.get(url=url, params=params)
            if res.status_code == 200:
                res = res.json()
                data = data + res
                if len(res) < limit:
                    break
                else:
                    page +=1
        return data

    # 车险清单数据
    def getCarItem(self,startdate):
        url='http://64.17.32.131/cwp-ui/forward/newitem/modtotal/caritemList'
        params = {
            "datatype": "jsonp",
            "queryStartdate": startdate,
            "queryEnddate": startdate,
            "comcode": "510123",
            "caliber": "cd",
        }
        try:
            res = req.get(url=url,params=params,headers=self.headers)
            filename=f'car-{self.startdate}.xlsx'
            if res.status_code == 200:
                file = open(filename, 'wb')
                file.write(res.content)
                file.close()
                df = pd.read_excel(filename,engine="openpyxl",dtype='str')
                headar = {
                    "投保单号": "proposalno",
                    "保单号": "policyno",
                    "批单号": "endorseno",
                    "保额": "sumamount",
                    "保费": "sumpremium",
                    "净保费": "sumnetpremium",
                    "税额": "sumtaxpremium",
                    "标准保费": "bzbf",
                    "折扣率": "discount",
                    "手续费上限": "costrateupper",
                    "手续费率": "costrate",
                    "手续费": "costfee",
                    "三者险保额": "amountb",
                    "车牌号码": "licenseno",
                    "号牌底色": "licensecolorcode",
                    "车辆种类": "carkindcode",
                    "初登日期": "enrolldate",
                    "使用年限": "useyears",
                    "VIN码": "vinno",
                    "发动机号": "engineno",
                    "车架号": "frameno",
                    "车型代码(车系+车型)": "modelcode",
                    "厂牌型号": "brandname",
                    "车型类别": "carkindtype",
                    "车型种类": "carkindflag",
                    "品牌名称": "brand_name_new",
                    "车系名称": "family_name_new",
                    "条款代码": "clausetype",
                    "使用性质": "usenaturecode",
                    "座位数": "seatcount",
                    "吨位数": "toncount",
                    "新车购置价": "purchaseprice",
                    "起保日期": "startdate",
                    "终保日期": "enddate",
                    "签单日期": "operatedate",
                    "营销系统标志": "markeflag",
                    "交叉销售标志": "crossflag",
                    "电网销标志": "openflag",
                    "备注1": "remark",
                    "备注2": "remark2",
                    "备注3": "remark3",
                    "备注4": "remark4",
                    "是否V盟出单": "vleagueflag",
                    "车船税": "planfee",
                    "是否电子投保": "isnetprop",
                    "保单类型": "netsales",
                    "实名验证": "checkcode",
                    "是否打印纸质保单": "printdocflag",
                    "渠道名称": "agentname",
                    "渠道代码": "agentcode",
                    "推荐送修码": "monopolycode",
                    "汽修厂": "monopolyname",
                    "归属业务员代码": "handler1code",
                    "经办人员代码": "handlercode",
                    "归属部门代码": "comcode",
                    "被保险人名称": "insuredname",
                    "投保人名称": "appliname",
                    "险种代码": "riskcode",
                }
                df.rename(columns=headar, inplace=True)
                return df.to_dict('records')
        except Exception as e:
            print(e)

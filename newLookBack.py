import sys
import LibMongo
from datetime import datetime
import csv

db = None
y_coll = None
yb_coll = None
x_coll = None
xb_coll = None
per_copy_amt = 1000

def getDB(ip,port,dbname):
    mongo = LibMongo.LibMongo(ip,port)
    mongo_client = mongo.connection()
    db = mongo_client[dbname]

def getColl(y_code,x_code):
    y_coll = db['day_'+y_code]
    yb_coll = db['bonus_'+y_code]
    x_coll = db['day_'+x_code]
    xb_coll= db['bonus_'+x_code]

def setTitle(csv_writer):
    list = []
    list.append("日期")
    list.append("沪深300涨跌幅")
    list.append("投资标的")
    list.append("当日基金净值")
    list.append("当日持仓市值预估")
    list.append("触发购买份数")
    list.append("触发购买总份数")
    list.append("触发购买总金额")
    list.append("今日购买市值")
    list.append("今日累计投入")
    list.append("累计现金")
    list.append("今日购买基金份数")
    list.append("累计基金份数")
    list.append("今日基金总市值")
    csv_writer.writerow(list)

def getBonus(coll,start_day,end_day):
    b_doc = coll.find({"_id":{'$gte':start_day,'$lte':end_day}})
    dict_line = {}
    for line in b_doc :
        dict_line[line["_id"]] = line['bonus']
    return dict_line

def process(csv_writer,start_day,end_day,x_code):
    y_doc = y_coll.find({"_id":{'$gte':start_day,'$lte':end_day}})
    dict_x_bonus = getBonus(xb_coll,start_day,end_day)
    dict_y_bonus = getBonus(yb_coll,start_day,end_day)

    yesterday_list = [None,None,None,None,None,None,None,None,None,None,None,None,None,None]
    yesterday_y_close = None
    for line in y_doc :
        today_list = [None,None,None,None,None,None,None,None,None,None,None,None,None,None]
        today_list[0] = line[_id]
        today_list[2] = x_code
        x_doc_line = x_coll.find_one({"_id":line["_id"]})
        if x_doc_line!=None:
            today_list[3] = x_doc_line["close"]
            today_list[4] = x_doc_line["close"] * (yesterday_y_close[12] or 0)
        else:
            ## 基金不开盘
            today_list[3] = 0
            today_list[4] = (yesterday_y_close[4] or 0)
            today_list[5] = 0
            today_list[6] = (yesterday_y_close[6] or 0)
            today_list[7] = (yesterday_y_close[7] or 0)
            today_list[8] = 0
            today_list[9] = (yesterday_y_close[9] or 0)
            today_list[10] = (yesterday_y_close[10] or 0)
            today_list[11] = 0
            today_list[12] = (yesterday_y_close[12] or 0)
            today_list[13] = (yesterday_y_close[13] or 0)
            csv_writer.writerow(today_list)
            yesterday_list = today_list
            yesterday_y_close = line["close"]
            continue;

        if yesterday_y_close==None :
            ## 昨日沪深300未有收盘价，无法计算涨跌幅
            x_doc_line = x_coll.find_one({"_id":line["_id"]})
            if x_doc_line!=None:
                today_list[5] = 0
                today_list[6] = (yesterday_y_close[6] or 0)
                today_list[7] = (yesterday_y_close[7] or 0)
                today_list[8] = 0
                today_list[9] = (yesterday_y_close[9] or 0)
                today_list[10] = (yesterday_y_close[10] or 0)
                today_list[11] = 0
                today_list[12] = (yesterday_y_close[12] or 0)
                today_list[13] = x_doc_line["close"] * (yesterday_y_close[12] or 0)
            csv_writer.writerow(today_list)
            yesterday_list = today_list
            yesterday_y_close = line["close"]
            continue;

        # 今日沪深300涨跌幅度
        today_list[1] = round((line["close"] - yesterday_y_close)/yesterday_y_close,6)

def main():
    mongo_ip='127.0.0.1'
    mongo_port=27017
    y_code = 'hs300'

    out = open('./newLookBack.csv', 'w', newline='')
    csv_writer = csv.writer(out, dialect='excel')
    setTitle(csv_writer)

    listss = []
    listss.append({'start':'20180101','end':'20200711'})
    listss.append({'start':'20180701','end':'20200711'})
    listss.append({'start':'20190101','end':'20200711'})

    fund_list=['hs300','110011','519732']

    getDB(mongo_ip,mongo_port,"test")
    for fund in fund_list :
        for list in listss :
            getColl(y_code,fund)
            process(csv_writer,list['start'],list['end'],fund)
        print(list['start'],list['end'])

if __name__ == '__main__':
    '''
        以沪深300跌幅计算买入份数
     '''
     main()
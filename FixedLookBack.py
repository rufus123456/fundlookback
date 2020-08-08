import sys
import LibMongo
from datetime import datetime
import csv

per_copy_amt = 1000

def getDB(ip,port,dbname):
    mongo = LibMongo.LibMongo(ip,port)
    mongo_client = mongo.connection()
    global db
    db = mongo_client[dbname]

def getColl(y_code,x_code):
    global y_coll
    y_coll = db['day_'+y_code]
    global yb_coll
    yb_coll = db['bonus_'+y_code]
    global x_coll
    x_coll = db['day_'+x_code]
    global xb_coll
    xb_coll = db['bonus_'+x_code]


def setTitle(csv_writer):
    list = []
    list.append("日期")                   # 0
    list.append("投资标的")               # 1
    list.append("当日基金净值")           # 2
    list.append("今日实际投入")           # 3
    list.append("今日累计投入")           # 4
    list.append("今日购买基金份数")       # 5
    list.append("累计基金份数")           # 6
    list.append("今日基金总市值")         # 7
    list.append("截止至今日收益率")       # 8
    list.append("累计投资次数")           # 9
    list.append("沪深300点位")            # 10
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

    yesterday_list = [None,None,None,None,None,None,None,None,None,None,None]
    yesterday_x = None
    yesterday_close = None
    for y_line in y_doc :
        line = x_coll.find_one({"_id":y_line["_id"]})
        today_list = [None,None,None,None,None,None,None,None,None,None,None]
        today_list[0] = line["_id"]
        today_list[1] = x_code
        today_list[2] = line["close"]

        if  line["_id"] in dict_x_bonus:
            #昨日总份数 * 每份分红额 = 分红总金额
            b_total_amt = (yesterday_list[6] or 0)*dict_x_bonus[line["_id"]]
            #计算分红再投资，今日可以添加总份数，累计到昨天总份数中
            bonus_add_fund_copies = round(b_total_amt/line["close"],2)
            yesterday_list[6] = (yesterday_list[6] or 0) + bonus_add_fund_copies

        if yesterday_x==None :
            ## 第一天买入一份
            today_list[3] = 1000
            today_list[10] = 0
            yesterday_close = y_line["close"]
        else:
            today_list[3] = 0
            today_list[10] = round((y_line["close"]-yesterday_close)/yesterday_close,5)

        today_list[4] = (yesterday_list[4] or 0) + today_list[3]
        today_list[5] = round(today_list[3]/line["close"],2)
        today_list[6] = (yesterday_list[6] or 0) + today_list[5]
        today_list[7] = today_list[6] * line["close"]
        today_list[8] = round(((today_list[7] or 0)- (today_list[4] or 0)) / (today_list[4] or 1),5)
        today_list[9] = 1
        

        csv_writer.writerow(today_list)
        yesterday_list = today_list
        yesterday_x = line["_id"]

def main():
    mongo_ip='127.0.0.1'
    mongo_port=27017
    y_code = 'hs300'

    out = open('./FixedLookBack.csv', 'w', newline='')
    csv_writer = csv.writer(out, dialect='excel')
    setTitle(csv_writer)

    listss = []
    listss.append({'start':'20180101','end':'20200711'})
    #listss.append({'start':'20180701','end':'20200711'})
    #listss.append({'start':'20190101','end':'20200711'})

    #fund_list=['hs300','110011','519732']
    fund_list=['110011']

    getDB(mongo_ip,mongo_port,"test")
    for fund in fund_list :
        for list in listss :
            getColl(y_code,fund)
            process(csv_writer,list['start'],list['end'],fund)
        print(list['start'],list['end'])

if __name__ == '__main__':
    '''
        每个周第一个开盘日买入
    '''

    main()



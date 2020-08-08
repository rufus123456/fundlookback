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

def getCopies(rate):
    ''' 计算跌幅索取份数
        <=-0.005,>-0.015 = 1份
        <=-0.015,>-0.025 = 2份
        <=-0.025,>-0.035 = 3份
        <=-0.035,>-0.045 = 4份
        <=-0.045,>-0.055 = 5份
        <=-0.055,>-0.065 = 6份
        ……
    '''
    get_copies_num = 0
    if rate<=0 :
        get_copies_num = abs(int(round(rate-0.00001,2)*100))
    return get_copies_num

def setTitle(csv_writer):
    list = []
    list.append("日期")                   # 0
    list.append("沪深300涨跌幅")          # 1
    list.append("投资标的")               # 2
    list.append("当日基金净值")           # 3
    list.append("触发购买份数")           # 4
    list.append("触发购买总份数")         # 5
    list.append("触发购买总金额")         # 6
    list.append("今日购买市值")           # 7
    list.append("今日实际投入")           # 8
    list.append("累计投入")               # 9
    list.append("累计现金")               # 10
    list.append("今日购买基金份数")       # 11
    list.append("累计基金份数")           # 12
    list.append("今日基金总市值")         # 13
    list.append("截止至今日收益率")       # 14
    list.append("累计投资次数")           # 15
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

    yesterday_list = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
    yesterday_y_close = None
    for line in y_doc :
        today_list = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
        today_list[0] = line["_id"]
        today_list[2] = x_code
        x_doc_line = x_coll.find_one({"_id":line["_id"]})

        if  line["_id"] in dict_x_bonus:
            #昨日总份数 * 每份分红额 = 分红总金额
            b_total_amt = (yesterday_list[12] or 0)*dict_x_bonus[line["_id"]]
            #计算分红再投资，今日可以添加总份数，累计到昨天总份数中
            bonus_add_fund_copies = round(b_total_amt/x_doc_line["close"],2)
            yesterday_list[12] = (yesterday_list[12] or 0) + bonus_add_fund_copies

        if x_doc_line!=None:
            today_list[3] = x_doc_line["close"]
        else:
            ## 基金不开盘
            today_list[3] = yesterday_list[3] or 0.
            today_list[4] = 0
            today_list[5] = (yesterday_list[5] or 0)
            today_list[6] = (yesterday_list[6] or 0.)
            today_list[7] = 0.
            today_list[8] = 0.
            today_list[9] = (yesterday_list[9] or 0.)
            today_list[10] = (yesterday_list[10] or 0.)
            today_list[11] = 0.
            today_list[12] = (yesterday_list[12] or 0.)
            today_list[13] = (yesterday_list[13] or 0.)
            today_list[14] = ((today_list[13] or 0.) + (today_list[10] or 0.) - (today_list[9] or 0.))/(today_list[9] or 1.)
            today_list[15] = (yesterday_list[15] or 0.)

            csv_writer.writerow(today_list)
            yesterday_list = today_list
            yesterday_y_close = line["close"]
            continue;

        if yesterday_y_close==None :
            ## 昨日沪深300未有收盘价，无法计算涨跌幅
            today_list[4] = 0
            today_list[5] = (yesterday_list[5] or 0)
            today_list[6] = (yesterday_list[6] or 0.)
            today_list[7] = 0.
            today_list[8] = 0.
            today_list[9] = (yesterday_list[9] or 0.)
            today_list[10] = (yesterday_list[10] or 0.)
            today_list[11] = 0.
            today_list[12] = (yesterday_list[12] or 0.)
            today_list[13] = today_list[3] * (today_list[12] or 0.)
            today_list[14] = ((today_list[13] or 0.) + (today_list[10] or 0.) - (today_list[9] or 0.))/(today_list[9] or 1.)
            today_list[15] = (yesterday_list[15] or 0.)

            csv_writer.writerow(today_list)
            yesterday_list = today_list
            yesterday_y_close = line["close"]
            continue;

        # 今日沪深300涨跌幅度
        today_list[1] = round((line["close"] - yesterday_y_close)/yesterday_y_close,6)
        # 触发购买份数、总份数、总金额(目标总市值)
        today_list[4] = getCopies(today_list[1])
        today_list[5] = (yesterday_list[5] or 0) + today_list[4]
        today_list[6] = (today_list[5] or 0.) * per_copy_amt

        # 无触发购买
        if today_list[4]<=0 :
            today_list[7] = 0.
            today_list[8] = 0.
            today_list[9] = (yesterday_list[9] or 0.)
            today_list[10] = (yesterday_list[10] or 0.)
            today_list[11] = 0.
            today_list[12] = (yesterday_list[12] or 0.)
            today_list[13] = today_list[12]*today_list[3]
            today_list[14] = ((today_list[13] or 0.) + (today_list[10] or 0.) - (today_list[9] or 0.))/(today_list[9] or 1.)
            today_list[15] = (yesterday_list[15] or 0.)

            csv_writer.writerow(today_list)
            yesterday_list = today_list
            yesterday_y_close = line["close"]
            continue;

        # 触发购买总市值 - 昨日持仓市值
        balance = today_list[6] - (yesterday_list[13] or 0)
        if balance>=0 :
            # 判断是否从累计现金中取
            today_list[7] = balance
            if balance-yesterday_list[10]>=0 :
                today_list[8] = balance-yesterday_list[10]
                today_list[9] = (yesterday_list[9] or 0.) + today_list[8]
                today_list[10] = 0.
            else:
                today_list[8] = 0.
                today_list[9] = (yesterday_list[9] or 0.)
                today_list[10] = yesterday_list[10] - balance

            today_list[11] = round(today_list[7]/today_list[3],2)
        else:
            # 今日卖出
            today_list[7] = 0.
            today_list[8] = 0.
            today_list[9] = (yesterday_list[9] or 0.)
            today_list[10] = (yesterday_list[10] or 0.) + abs(balance)
            today_list[11] = round(balance/today_list[3],2)

        today_list[12] = (yesterday_list[12] or 0.) + today_list[11]
        today_list[13] = today_list[12]*today_list[3]
        today_list[14] = ((today_list[13] or 0.) + (today_list[10] or 0.) - (today_list[9] or 0.))/(today_list[9] or 1.)
        today_list[15] = (yesterday_list[15] or 0.) + 1

        csv_writer.writerow(today_list)
        yesterday_list = today_list
        yesterday_y_close = line["close"]

def main():
    mongo_ip='127.0.0.1'
    mongo_port=27017
    y_code = 'hs300'

    out = open('./EqualValueLookBack.csv', 'w', newline='')
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
        以沪深300跌幅计算买入份数
    '''

    main()



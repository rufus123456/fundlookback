import sys
import LibMongo
import csv
from datetime import datetime

class FixLookBack():
    def __init__(self,ip,port):
        self.mongo = LibMongo.LibMongo(ip,port)
        self.mongo_client = self.mongo.connection()
        self.module = 0
        #print(self.mongo_client)
    def setDb(self,dbname):
        ''' 载入database '''
        self.db = self.mongo_client[dbname]
    def setCode(self,x_code):
        ''' 载入股票代码 '''
        self.x_code = x_code
        self.x_coll = self.db['day_'+x_code]
        self.lb_x_coll = self.db['lb_'+x_code]
        self.xb_coll= self.db['bonus_'+x_code]
    def setCVS(self,csv_writer):
        self.csv_writer = csv_writer
    def writeCVS(self,list1):
        self.csv_writer.writerow(list1)
    def setModule(self,modul):
        self.module = modul
    def getBonus(self,coll,start_day,end_day):
        xb_doc = coll.find({"_id":{'$gte':start_day,'$lte':end_day}})
        dict_line = {}
        for line in xb_doc :
            dict_line[line["_id"]] = line['bonus']
        #print(dict_line)
        return dict_line
    def isbuy(self,nowday,yesday):
        # 周买入，每周3 module = 1
        # 其他，月买入
        if self.module == 1 :
            week=datetime.strptime(nowday, "%Y%m%d").weekday()
            if week==2 :
                return True;
            else:
                return False;
        else:
            if nowday[0:6]!=yesday[0:6]:
                return True;
            else:
                return False;
    def process(self,start_day,end_day,copy_amt):
        ''' 遍历指定交易日start_day <= x <= end_day，计算每月初买入指定份数'''
        x = self.lb_x_coll.delete_many({})
        #print("请空文档",x.deleted_count, "个文档已删除")

        x_doc = self.x_coll.find({"_id":{'$gte':start_day,'$lte':end_day}})
        day_ss = "-1"
        close = -1

        fund_close=-1
        fund_copies = 0
        total_amt = 0
        count = 0

        dict_bonus = self.getBonus(self.xb_coll,start_day,end_day)

        for line in x_doc :
            dict_line = {}
            dict_line['_id'] = line["_id"]
            dict_line['close'] = line["close"]
            dict_line['amt'] = 0
            dict_line['fund_copies'] =  0
            total_add_fund_copies = 0
            if day_ss=="-1":
                dict_line['amt'] = copy_amt
                dict_line['fund_copies'] =  round(copy_amt/line["close"],2)
                total_add_fund_copies = dict_line['fund_copies']
                count = count + 1
            elif self.isbuy(line["_id"],day_ss) :
                #print("上月:", day_ss, '本月初:', line["_id"] )
                dict_line['amt'] = copy_amt
                dict_line['fund_copies'] =  round(copy_amt/line["close"],2)
                total_add_fund_copies = dict_line['fund_copies']
                count = count + 1

            if  line["_id"] in dict_bonus:
                #print('分红日:',line["_id"],'每份分红额:',dict_bonus[line["_id"]])
                b_total_amt = round(fund_copies,2)*dict_bonus[line["_id"]]
                new_fund_copies = round(b_total_amt/line["close"],2)
                #print('当前总份数:',fund_copies,'分红总额:',b_total_amt,'再投资新增份数:',new_fund_copies)
                #print('本日购买份数:',total_add_fund_copies,'本日总购买份数:',total_add_fund_copies+new_fund_copies)
                total_add_fund_copies = total_add_fund_copies + new_fund_copies
                dict_line['fund_copies'] = total_add_fund_copies
            #if dict_line['fund_copies']>0 :
            #    print(dict_line['_id'],dict_line['close'],dict_line['amt'],dict_line['fund_copies'])
            day_ss = line["_id"]
            close = line["close"]
            fund_copies = round(fund_copies + dict_line['fund_copies'],2)
            total_amt = total_amt + dict_line['amt']
            fund_close = dict_line['close']
        list = []
        list.append(self.x_code)
        list.append(start_day)
        list.append(end_day)
        list.append(fund_copies)
        list.append(count)
        list.append(total_amt)
        list.append(round(fund_copies,2)*fund_close)
        list.append(round((round(fund_copies,2)*fund_close-total_amt)/total_amt,4))
        self.writeCVS(list)

def main():
    '''
    start_day = sys.argv[1]
    end_day = sys.argv[2]
    copy_amt = int(sys.argv[3])
    '''
    mongo_ip='127.0.0.1'
    mongo_port=27017

    out = open('./FixLookBack.csv', 'w', newline='')
    csv_writer = csv.writer(out, dialect='excel')

    listss = []
    listss.append({'start':'20180101','end':'20200531'})
    listss.append({'start':'20180701','end':'20200531'})
    listss.append({'start':'20190101','end':'20200531'})

    fund_list=['hs300','110011','519732']
    for list in listss :
        for fund in fund_list :
            loolback = FixLookBack(mongo_ip,mongo_port)
            loolback.setDb("test")
            loolback.setCVS(csv_writer)
            loolback.setModule(1)
            loolback.setCode(fund)
            loolback.process(list['start'],list['end'],1000)

if __name__ == '__main__':
    ''' 
        app.py start_day end_day copy_amt
        以沪深300跌幅计算买入份数
     '''
    #if len(sys.argv)<4 :
    #    print('app.py start_day end_day copy_amt')
    #    sys.exit()
    main()

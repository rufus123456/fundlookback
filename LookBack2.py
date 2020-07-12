import sys
import LibMongo
from datetime import datetime
import csv

class LookBack():
    def __init__(self,ip,port):
        self.mongo = LibMongo.LibMongo(ip,port)
        self.mongo_client = self.mongo.connection()
        self.rise_num = 0
        self.total_copies_num = 0
        self.last_day = -1
        self.today = -1

    def setDb(self,dbname):
        ''' 载入database '''
        self.db = self.mongo_client[dbname]
    def setCVS(self,csv_writer):
        self.csv_writer = csv_writer
    def writeCVS(self,list1):
        self.csv_writer.writerow(list1)
    def setCode(self,y_code,x_code):
        ''' 载入2个比较的股票代码 '''
        self.x_code = x_code
        self.y_coll = self.db['day_'+y_code]
        self.yb_coll = self.db['bonus_'+y_code]
        self.x_coll = self.db['day_'+x_code]
        self.xb_coll= self.db['bonus_'+x_code]
        self.y_x_coll = self.db['lb_'+y_code+'_'+x_code]
    def getCopies1(self,rate):
        ''' 计算跌幅索取份数
            <=-0.005,>-0.015 = 1份
            <=-0.015,>-0.025 = 2份
            <=-0.025,>-0.035 = 3份
            <=-0.035,>-0.045 = 4份
            <=-0.045,>-0.055 = 5份
            <=-0.055,>-0.065 = 6份
            ……
        '''
        if rate<=0 :
            self.rate = abs(int(round(rate-0.0001,2)*100))
        else:
            self.rate = 0
        return self.rate
    def getCopies11(self,rate):
        ''' 计算跌幅索取份数
            <=-0.005,>-0.015 = 1份
            <=-0.015,>-0.025 = 2份
            <=-0.025,>-0.035 = 3份
            <=-0.035,>-0.045 = 4份
            <=-0.045,>-0.055 = 5份
            <=-0.055         = 6份
            ……
            连涨5天，第5天买一手
        '''
        if rate<=0 :
            self.rate = abs(int(round(rate-0.0001,2)*100))
            self.rise_num = 0
        else:
            self.rate = 0
            self.rise_num = self.rise_num + 1
            if self.rise_num/5 >=1 :
                #print("连涨",self.rise_num)
                self.rate = 1
        return self.rate
    def getCopies12(self,rate):
        ''' 计算跌幅索取份数
            <=-0.005,>-0.015 = 1份
            <=-0.015,>-0.025 = 2份
            <=-0.025,>-0.035 = 3份
            <=-0.035,>-0.045 = 4份
            <=-0.045,>-0.055 = 5份
            <=-0.055,>-0.065 = 6份
            ……
            连涨5天，从第6天开始 <=0.005, >-0.005 = 1份
        '''
        if rate<=0 :
            self.rate = abs(int(round(rate-0.0001,2)*100))
            if self.rise_num>=6 and rate>-0.005 and self.rate==0:
                #print("连涨",self.rise_num,rate)
                self.rate = 1
            self.rise_num = 0
        else:
            self.rate = 0
            if self.rise_num>=6 and rate<=0.005:
                #print("连涨",self.rise_num,rate)
                self.rate = 1
            self.rise_num = self.rise_num + 1
        return self.rate
    def getCopies13(self,rate):
        ''' 计算跌幅索取份数
            <=0.005, >-0.005 = 1份
            <=-0.005,>-0.015 = 1份
            <=-0.015,>-0.025 = 2份
            <=-0.025,>-0.035 = 3份
            <=-0.035,>-0.045 = 4份
            <=-0.045,>-0.055 = 5份
            <=-0.055,>-0.065 = 6份
            ……
        '''
        if rate<=-0.005 :
            self.rate = abs(int(round(rate-0.0001,2)*100))
        elif rate>-0.005 and rate<=0.005:
            self.rate = 1
        else:
            self.rate = 0 
        return self.rate
    def getCopies4(self,rate):
        ''' 计算跌幅索取份数
            <=-0.005,>-0.015 = 1份
            <=-0.015,>-0.025 = 3份
            <=-0.025,>-0.035 = 5份
            <=-0.035,>-0.045 = 7份
            <=-0.045,>-0.055 = 8份
            <=-0.055,>-0.065 = 10份
            ……
        '''
        if rate<=0 :
            rate = rate-0.0001
            if rate<=-0.005 and rate>-0.015 :
                return 1
            elif rate<=-0.015 and rate>-0.025 :
                return 3
            elif rate<=-0.025 and rate>-0.035 :
                return 5
            elif rate<=-0.035 and rate>-0.045 :
                return 7
            elif rate<=-0.045 and rate>-0.055 :
                return 8
            elif rate<=-0.055 :
                return 10
            else:
                return 0
        else:
            self.rate = 0
        return self.rate
    def setmodule(self,module,rate):
        if module==1:
            return self.getCopies1(rate)
        elif module==2:
            return self.getCopies11(rate)
        elif module==3:
            return self.getCopies12(rate)
        elif module==4:
            return self.getCopies13(rate)
        else:
            return self.getCopies4(rate)
        # 月度定投、周定投

    def getBonus(self,coll,start_day,end_day):
        xb_doc = coll.find({"_id":{'$gte':start_day,'$lte':end_day}})
        dict_line = {}
        for line in xb_doc :
            dict_line[line["_id"]] = line['bonus']
        #print(dict_line)
        return dict_line

    def process(self,start_day,end_day,copy_amt,module):
        ''' 遍历指定交易日start_day < x <= end_day，计算每日购买金额、基金份数、回溯后涨幅'''
        x = self.y_x_coll.delete_many({})
        #print("请空文档",x.deleted_count, "个文档已删除")
        y_doc = self.y_coll.find({"_id":{'$gte':start_day,'$lte':end_day}})
        day_ss = "-1"
        close = -1

        fund_close=-1
        fund_copies = 0
        total_amt = 0
        count = 0
        oneweek_count = 0
        last_copies_num = 0
        copies_num = 0

        dict_bonus = self.getBonus(self.xb_coll,start_day,end_day)
        dict_y_bonus = self.getBonus(self.yb_coll,start_day,end_day)

        for line in y_doc :
            if day_ss=="-1":
                day_ss = line["_id"]
                close = line["close"]
                continue;
            if  line["_id"] in dict_y_bonus:
                print('标存在分红日:',line["_id"],'涨跌剔除分红:',dict_y_bonus[line["_id"]])
                close = close - dict_y_bonus[line["_id"]]
            rate = (line["close"] - close)/close
            copies_num = self.setmodule(module,rate)
            if copies_num>0 :
                count = count +1
            x_doc_line = self.x_coll.find_one({"_id":line["_id"]})

            if x_doc_line==None:
                print('基金不开盘')
                day_ss = line["_id"]
                close = line["close"]
                last_copies_num = copies_num
                continue;

            dict_line = {}
            dict_line['_id'] = line["_id"]
            dict_line['300_close'] = line["close"]
            dict_line['close'] = x_doc_line["close"]
            dict_line['rate'] = rate
            dict_line['amt'] = copy_amt * copies_num

            total_add_fund_copies = round(dict_line['amt']/x_doc_line["close"],2)
            if  line["_id"] in dict_bonus:
                print('分红日:',line["_id"],'每份分红额:',dict_bonus[line["_id"]])
                b_total_amt = round(fund_copies,2)*dict_bonus[line["_id"]]
                new_fund_copies = round(b_total_amt/x_doc_line["close"],2)
                print('当前总份数:',fund_copies,'分红总额:',b_total_amt,'再投资新增份数:',new_fund_copies)
                print('本日购买份数:',total_add_fund_copies,'本日总购买份数:',total_add_fund_copies+new_fund_copies)
                total_add_fund_copies = total_add_fund_copies + new_fund_copies

            dict_line['fund_copies'] = total_add_fund_copies
            #print(self.y_x_coll.insert_one(dict_line))
            #print(dict_line['_id'],dict_line['300_close'],dict_line['rate'],dict_line['close'],dict_line['amt'],dict_line['fund_copies'])
            fund_copies = round(fund_copies + dict_line['fund_copies'],2)
            total_amt = total_amt + dict_line['amt']

            fund_close = dict_line['close']
            day_ss = line["_id"]
            close = line["close"]
            last_copies_num = copies_num
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
    y_code = 'hs300'

    out = open('./LookBack.csv', 'w', newline='')
    csv_writer = csv.writer(out, dialect='excel')

    listss = []
    listss.append({'start':'20180101','end':'20200711'})
    listss.append({'start':'20180701','end':'20200711'})
    listss.append({'start':'20190101','end':'20200711'})

    fund_list=['hs300','110011','519732']
    module_list=[1,5,3]
    for fund in fund_list :
        for list in listss :
            for module in module_list:
                loolback = LookBack(mongo_ip,mongo_port)
                loolback.setDb("test")
                loolback.setCVS(csv_writer)
                loolback.setCode(y_code,fund)
                loolback.process(list['start'],list['end'],1000,module)
        print(list['start'],list['end'])

if __name__ == '__main__':
    ''' 
        app.py start_day end_day copy_amt
        以沪深300跌幅计算买入份数
     '''
    #if len(sys.argv)<4 :
    #    print('app.py start_day end_day copy_amt')
    #    sys.exit()
    main()

import sys
import LibMongo
from datetime import datetime
import csv

class LookBack():
    def __init__(self,ip,port):
        self.mongo = LibMongo.LibMongo(ip,port)
        self.mongo_client = self.mongo.connection()

        # 连涨天数
        self.rise_num = 0
        # 上一个交易日
        self.last_day = -1
        self.today = -1
        self.get_copies_num = 0
        # 每份金额
        self.copy_amt = 0
        # 当天基金净值
        self.x_fund_close = 0

        # 购买总次数
        self.count = 0
        # 购买总份数
        self.total_copies_num = 0
        # 购买总金额
        self.total_amt = 0
        # 已购买基金总份数
        self.total_fund_copies = 0
        # 分红日增加基金份数
        self.bonus_add_fund_copies = 0



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
            self.get_copies_num = abs(int(round(rate-0.0001,2)*100))
        else:
            self.get_copies_num = 0
        return self.get_copies_num
    def getCopies2(self,rate):
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
                self.get_copies_num = 1
            elif rate<=-0.015 and rate>-0.025 :
                self.get_copies_num = 3
            elif rate<=-0.025 and rate>-0.035 :
                self.get_copies_num = 5
            elif rate<=-0.035 and rate>-0.045 :
                self.get_copies_num = 7
            elif rate<=-0.045 and rate>-0.055 :
                self.get_copies_num = 8
            elif rate<=-0.055 :
                self.get_copies_num = 10
            else:
                self.get_copies_num = 0
        else:
            self.get_copies_num = 0
        return self.get_copies_num
    def getCopies3(self,rate):
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
            self.get_copies_num = abs(int(round(rate-0.0001,2)*100))
            if self.rise_num>=6 and rate>-0.005 and self.get_copies_num==0:
                #print("连涨",self.rise_num,rate)
                self.get_copies_num = 1
            self.rise_num = 0
        else:
            self.get_copies_num = 0
            if self.rise_num>=6 and rate<=0.005:
                #print("连涨",self.rise_num,rate)
                self.get_copies_num = 1
            self.rise_num = self.rise_num + 1
        return self.get_copies_num
    def getCopies4(self,rate):
        ''' 每月定投一次 '''
        if self.last_day!=-1 and self.today!=-1 and self.last_day[0:6]!=self.today[0:6] :
            self.get_copies_num = 1
        else:
            self.get_copies_num = 0
        return self.get_copies_num
    def getCopies5(self,rate):
        ''' 每周定投一次 '''
        self.get_copies_num = 0
        if self.last_day!=-1 and self.today!=-1 :
            last_day_week = datetime.strptime(self.last_day, "%Y%m%d").weekday()
            today_week = datetime.strptime(self.today, "%Y%m%d").weekday()
            if last_day_week>today_week :
                self.get_copies_num = 1
        return self.get_copies_num
    def getCopies6(self,rate):
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
            self.get_copies_num = abs(int(round(rate-0.0001,2)*100))
        else:
            self.get_copies_num = 0

        # 最新购买总市值
        per_total_amt = round(self.total_amt + self.get_copies_num*self.copy_amt,5)
        # 当前基金总市值
        now_total_amt = round(self.x_fund_close*(self.total_fund_copies+self.bonus_add_fund_copies),5)
        if self.get_copies_num!=0 and now_total_amt>=per_total_amt:
            print("超价值，不买",per_total_amt,now_total_amt)
        if self.get_copies_num!=0 and now_total_amt<per_total_amt:
            amt = per_total_amt - now_total_amt
            self.get_copies_num = amt/(self.copy_amt)
        elif now_total_amt<per_total_amt:
            #print(self.today,self.get_copies_num,rate,per_total_amt,now_total_amt)
            self.get_copies_num = 0
        else:
            self.get_copies_num = 0
        return self.get_copies_num

    def setmodule(self,module,rate):
        self.get_copies_num = 0
        if module==1:
            self.getCopies1(rate)
        elif module==2:
            self.getCopies2(rate)
        elif module==3:
            self.getCopies3(rate)
        elif module==4:
            self.getCopies4(rate)
        elif module==5:
            self.getCopies5(rate)
        else:
            self.getCopies6(rate)

        if self.get_copies_num>0:
            self.count = self.count + 1
        return self.get_copies_num

    def getmodulename(self,module):
        if module==1:
            return "智能定投"
        elif module==2:
            return "佛系定投"
        elif module==3:
            return "智能优化6+1"
        elif module==4:
            return "月定投"
        elif module==5:
            return "周定投"
        else:
            return "等价值投";

    def getBonus(self,coll,start_day,end_day):
        xb_doc = coll.find({"_id":{'$gte':start_day,'$lte':end_day}})
        dict_line = {}
        for line in xb_doc :
            dict_line[line["_id"]] = line['bonus']
        #print(dict_line)
        return dict_line

    def process(self,start_day,end_day,copy_amt,module):
        ''' 遍历指定交易日start_day < x <= end_day，计算每日购买金额、基金份数、回溯后涨幅'''
        self.copy_amt = copy_amt
        x = self.y_x_coll.delete_many({})
        #print("请空文档",x.deleted_count, "个文档已删除")
        y_doc = self.y_coll.find({"_id":{'$gte':start_day,'$lte':end_day}})
        y_day_ss = "-1"
        y_close = -1

        fund_close=-1
        copies_num = 0

        dict_bonus = self.getBonus(self.xb_coll,start_day,end_day)
        dict_y_bonus = self.getBonus(self.yb_coll,start_day,end_day)

        for line in y_doc :
            self.today = line["_id"]
            if y_day_ss=="-1":
                #print('第一个计算日')
                y_day_ss = line["_id"]
                y_close = line["close"]
                self.last_day = self.today
                continue;
            if  line["_id"] in dict_y_bonus:
                #print('标存在分红日:',line["_id"],'涨跌剔除分红:',dict_y_bonus[line["_id"]])
                y_close = y_close - dict_y_bonus[line["_id"]]

            x_doc_line = self.x_coll.find_one({"_id":line["_id"]})
            if x_doc_line==None:
                #print('基金不开盘')
                y_day_ss = line["_id"]
                y_close = line["close"]
                self.last_day = self.today
                continue;

            self.x_fund_close = x_doc_line["close"]
            self.bonus_add_fund_copies = 0
            if  line["_id"] in dict_bonus:
                #print('分红日:',line["_id"],'每份分红额:',dict_bonus[line["_id"]])
                b_total_amt = round(self.total_fund_copies,2)*dict_bonus[line["_id"]]
                self.bonus_add_fund_copies = round(b_total_amt/x_doc_line["close"],2)
                #print('当前总份数:',self.total_fund_copies,'分红总额:',b_total_amt,'再投资新增份数:',self.bonus_add_fund_copies)

            rate = (line["close"] - y_close)/y_close
            self.setmodule(module,rate)

            dict_line = {}
            dict_line['_id'] = line["_id"]
            dict_line['300_close'] = line["close"]
            dict_line['close'] = x_doc_line["close"]
            dict_line['rate'] = rate
            dict_line['amt'] = round(self.copy_amt * self.get_copies_num,2)
            dict_line['fund_copies'] = round(dict_line['amt']/x_doc_line["close"],2) + self.bonus_add_fund_copies

            print(dict_line['_id'],dict_line['300_close'],dict_line['rate'],dict_line['close'],dict_line['amt'],
            dict_line['fund_copies'],self.get_copies_num)

            self.total_copies_num = self.total_copies_num + self.get_copies_num
            self.total_fund_copies = round(self.total_fund_copies + dict_line['fund_copies'],2)
            self.total_amt = self.total_amt + dict_line['amt']

            fund_close = dict_line['close']
            y_day_ss = line["_id"]
            y_close = line["close"]
            self.last_day = self.today
        list = []
        list.append(self.x_code)
        list.append(start_day)
        list.append(end_day)
        list.append(self.total_fund_copies)
        list.append(self.count)
        list.append(self.total_amt)
        list.append(round(self.total_fund_copies,2)*fund_close)
        list.append(round((round(self.total_fund_copies,2)*fund_close-self.total_amt)/self.total_amt,4))
        list.append(self.getmodulename(module))
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
    list = []
    list.append("投资标的")
    list.append("开始日期")
    list.append("结束日期")
    list.append("累计份额")
    list.append("累计次数")
    list.append("累计投入资金")
    list.append("截止日市值")
    list.append("持仓收益率")
    list.append("模式")
    csv_writer.writerow(list)

    #print("交易日","沪深手盘价","沪深涨跌幅","基金净值","今日购买份数","购买总份数","目标购买总市值","","","",dict_line['amt'],dict_line['fund_copies'],self.get_copies_num)
    listss = []
    #listss.append({'start':'20200421','end':'20200711'})
    listss.append({'start':'20180101','end':'20200711'})
    #listss.append({'start':'20180701','end':'20200711'})
    #listss.append({'start':'20190101','end':'20200711'})

    #fund_list=['hs300','110011','519732']
    fund_list=['110011']
    module_list=[1,2,3,4,5,6]
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

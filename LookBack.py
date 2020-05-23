import sys
import LibMongo

class LookBack():
    def __init__(self,ip,port):
        self.mongo = LibMongo.LibMongo(ip,port)
        self.mongo_client = self.mongo.connection()
        print(self.mongo_client)
    def setDb(self,dbname):
        ''' 载入database '''
        self.db = self.mongo_client[dbname]
    def setCode(self,y_code,x_code):
        ''' 载入2个比较的股票代码 '''
        self.y_coll = self.db['day_'+y_code]
        self.x_coll = self.db['day_'+x_code]
        self.xb_coll= self.db['bonus_'+x_code]
        self.y_x_coll = self.db['lb_'+y_code+'_'+x_code]
    def getCopies(self,rate):
        ''' 计算跌幅索取份数 '''
        if rate<0 :
            self.rate = abs(int(round(rate,2)*100))
        else:
            self.rate = 0
        return self.rate
    def getBonus(self,start_day,end_day):
        xb_doc = self.xb_coll.find({"_id":{'$gte':start_day,'$lte':end_day}})
        dict_line = {}
        for line in xb_doc :
            dict_line[line["_id"]] = line['bonus']
        print(dict_line)
        return dict_line

    def process(self,start_day,end_day,copy_amt):
        ''' 遍历指定交易日start_day < x <= end_day，计算每日购买金额、基金份数、回溯后涨幅'''
        x = self.y_x_coll.delete_many({})
        print("请空文档",x.deleted_count, "个文档已删除")
        y_doc = self.y_coll.find({"_id":{'$gte':start_day,'$lte':end_day}})
        day_ss = "-1"
        close = -1

        fund_close=-1
        fund_copies = 0
        total_amt = 0

        dict_bonus = self.getBonus(start_day,end_day)

        for line in y_doc :
            if day_ss=="-1":
                day_ss = line["_id"]
                close = line["close"]
                continue;
            rate = (line["close"] - close)/close
            copies_num = self.getCopies(rate)
            x_doc_line = self.x_coll.find_one({"_id":line["_id"]})

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
            print(dict_line)
            fund_copies = round(fund_copies + dict_line['fund_copies'],2)
            total_amt = total_amt + dict_line['amt']

            fund_close = dict_line['close']
            day_ss = line["_id"]
            close = line["close"]
        print('基金总份数:',fund_copies,'涨幅:',round((round(fund_copies,2)*fund_close-total_amt)/total_amt,4))

def main():
    mongo_ip='127.0.0.1'
    mongo_port=27017
    y_code = sys.argv[1]
    x_code = sys.argv[2]
    start_day = sys.argv[3]
    end_day = sys.argv[4]
    copy_amt = int(sys.argv[5])

    loolback = LookBack(mongo_ip,mongo_port)
    loolback.setDb("test")
    loolback.setCode(y_code,x_code)
    loolback.process(start_day,end_day,copy_amt)

if __name__ == '__main__':
    ''' app.py y_code x_code start_day end_day copy_amt '''
    main()

import sys
import LibMongo

class LookBack():
    def __init__(self,ip,port):
        self.mongo_client = LibMongo(ip,port).connection()
        print(self.mongo_client)
    def setDb(self,dbname):
        ''' 载入database '''
        self.db = self.mongo_client[dbname]
    def setCode(self,y_code,x_code):
        ''' 载入2个比较的股票代码 '''
        self.y_coll = self.db['day_'+y_code]
        self.x_coll = self.db['day_'+x_code]
        self.y_x_coll = self.db[y_code+'_'+x_code]
    def getCopies(self,rate):
        ''' 计算跌幅索取份数 '''
        if rate<0 :
            self.rate = round(rate,2)*100
        else:
            self.rate = 0
    def process(self,start_day,end_day,copy_amt):
        ''' 遍历指定交易日start_day < x <= end_day，计算每日购买金额、基金份数、回溯后涨幅'''
        y_doc = self.y_coll.find({"_id":{'$gte':start_day,'$lte':end_day}})
        day_ss = "-1"
        close = -1
        for line in mydoc :
            if day_ss=="-1":
                day_ss = line["_id"]
                close = line["close"]
                continue;
            rate = (line["close"] - close)/close
            copies_num = getCopies(rate)
            x_doc_line = self.x_coll.find_one({"_id":line["_id"]})

            dict_line = {}
            dict_line['_id'] = line["_id"]
            dict_line['300_close'] = line["close"]
            dict_line['close'] = x_doc_line["close"]
            dict_line['rate'] = rate
            dict_line['amt'] = copy_amt * copies_num
            dict_line['fund_copies'] = round(dict_line['amt']/x_doc_line["close"],2)
            print(self.y_x_coll.insert_one(dict_line))
            print(dict_line)
            
            day_ss = line["_id"]
            close = line["close"]

def main():
    mongo_ip='127.0.0.1'
    mongo_port=27017
    y_code = 'hs300'
    x_code = sys.argv[1]
    start_day = sys.argv[2]
    end_day = sys.argv[3]
    copy_amt = int(sys.argv[4])

if __name__ == '__main__':
    main()
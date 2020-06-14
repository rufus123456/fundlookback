import sys
import LibMongo

class LookBack():
    def __init__(self,ip,port):
        self.mongo = LibMongo.LibMongo(ip,port)
        self.mongo_client = self.mongo.connection()
        #print(self.mongo_client)
    def setDb(self,dbname):
        ''' 载入database '''
        self.db = self.mongo_client[dbname]
    def setCode(self,x_code):
        ''' 载入股票代码 '''
        self.x_code = x_code
        self.x_coll = self.db['day_'+x_code]

    def process(self,start_day,end_day,copy_amt):
        x_doc = list(self.x_coll.find({"_id":{'$gte':start_day,'$lte':end_day}}))
        fund_copies = round(copy_amt / x_doc[0]["close"],2)
        print(x_doc[0])
        print(x_doc[-1])
        print(self.x_code,start_day,end_day,fund_copies,1,copy_amt,round(fund_copies,2)*x_doc[-1]["close"],round((round(fund_copies,2)*x_doc[-1]["close"]-copy_amt)/copy_amt,4))

def main():
    mongo_ip='127.0.0.1'
    mongo_port=27017

    start_day = sys.argv[1]
    end_day = sys.argv[2]
    copy_amt = int(sys.argv[3])

    fund_list=['hs300','110011','519732']
    for fund in fund_list :
        loolback = LookBack(mongo_ip,mongo_port)
        loolback.setDb("test")
        loolback.setCode(fund)
        loolback.process(start_day,end_day,copy_amt)

if __name__ == '__main__':
    ''' 
        app.py start_day end_day copy_amt 
        每月定投基金
    '''
    if len(sys.argv)<4 :
        print('app.py start_day end_day copy_amt')
        sys.exit()
    main()

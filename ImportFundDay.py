import LibMongo
import sys
import time

class ImportFundDay():
    def filelines(file) :
        '''
            开头2行是说明，结束行是备注，中间均是内容行
        '''
        fo = open(file, "r")
        list_of_all_the_lines = fo.readlines()[2:-1]
        fo.close()
        return list_of_all_the_lines

    def datess(ss):
        ''' 将%Y/%m/%d日期转换成%Y%m%d '''
        aa = "%Y/%m/%d"
        return time.strftime("%Y%m%d", time.strptime(ss,aa))

if __name__=="__main__" :
    '''
    python ./app.py fund_name
    '''
    ip = '127.0.0.1'
    port = 27017
    fund_name = sys.argv[1]

    mongo = LibMongo.LibMongo(ip,port)
    mongo_client = mongo.connection()
    print(mongo_client)

    mongo_db = mongo_client["test"]
    mycol = mongo_db["day_"+fund_name]
    x = mycol.delete_many({})
    print("请空文档",x.deleted_count, "个文档已删除")

    funddayfile = "./data/33#"+fund_name+".txt"
    list_of_all_the_lines = ImportFundDay.filelines(funddayfile)
    many_lines=[]
    for line in list_of_all_the_lines :
        list_line = line.split()
        print(list_line)
        dict_line = {}
        dict_line['_id']=ImportFundDay.datess(list_line[0])
        dict_line['open']=float(list_line[1])
        dict_line['high']=float(list_line[2])
        dict_line['low']=float(list_line[3])
        dict_line['close']=float(list_line[4])
        dict_line['volume']=float(list_line[5])
        dict_line['amount']=float(list_line[6])
        print(dict_line)
        many_lines.append(dict_line)
    xx = mycol.insert_many(many_lines)
    print(xx.inserted_ids)


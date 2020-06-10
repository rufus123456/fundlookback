from pymongo import MongoClient
import sys
import time

def mygetlines(file) :
    fo = open(file, "r")
    list_of_all_the_lines = fo.readlines()[2:-1]
    fo.close()
    return list_of_all_the_lines

# %Y/%m/%d to %Y%m%d
def datess(ss):
    aa = "%Y/%m/%d"
    return time.strftime("%Y%m%d", time.strptime(ss,aa))

if __name__=="__main__" :
    '''
    python ./app.py file name
    '''
    client = MongoClient('127.0.0.1', 27017)
    db = client.test

    print(sys.argv[1])
    list_of_all_the_lines = mygetlines(sys.argv[1])
    many_lines=[]
    for line in list_of_all_the_lines :
        list_line = line.split()
        print(list_line)
        dict_line = {}
        dict_line['_id']=datess(list_line[0])
        dict_line['open']=float(list_line[1])
        dict_line['high']=float(list_line[2])
        dict_line['low']=float(list_line[3])
        dict_line['close']=float(list_line[4])
        dict_line['volume']=float(list_line[5])
        dict_line['amount']=float(list_line[6])
        print(dict_line)
        many_lines.append(dict_line)
    collection = db["day_"+sys.argv[2]]
    print(collection)
    print(collection.insert_many(many_lines))





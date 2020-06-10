from pymongo import MongoClient
import sys

def mygetlines(file) :
    fo = open(file, "r")
    list_of_all_the_lines = fo.readlines()[2:-1]
    fo.close()
    return list_of_all_the_lines


if __name__=="__main__" :
    '''
    python ./app.py file
    '''
    client = MongoClient('127.0.0.1', 27017)
    db = client.test
    print(sys.argv[1])
    list_of_all_the_lines = mygetlines(sys.argv[1])
    for line in list_of_all_the_lines :
        print(line.split())
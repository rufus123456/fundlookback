import pymongo

class LibMongo():
    def __init__(self,ip,port):
        self.myip = ip
        self.myport = port
        self.myclient = None
        self.dbname = None
        self.colname = None
        self.mydb = None
        self.mycoll = None
    def connection(self):
        if (self.myclient!=None) or (not self.myclient) :
            self.myclient = pymongo.MongoClient(self.myip,int(self.myport))
        return self.myclient

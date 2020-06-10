#!/usr/bin/python3
 
import pymongo
import sys

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["test"]
print(mydb)
mycol = mydb["day_hs300"]
print(mycol)
myquery = { "_id": { "$gt": "20190101" } }

mydoc = mycol.find(myquery)

date_ss = "-1"
close1 = -1

amt = 1000

for x in mydoc:
	#print(x)
	#print(x["_id"],x["close"])

	if date_ss=="-1" :
		date_ss = x["_id"]
		close1 = x["close"]
		continue;
	num = 0
	# 判断涨跌,买入份数
	raise_or_up = (x["close"] - close1)/close1
	if raise_or_up<0 :
		#print(raise_or_up)
		if raise_or_up>-0.005 :
			num = 1
		elif raise_or_up>-0.01 :
			num = 2
		elif raise_or_up>-0.015 :
			num = 3
		elif raise_or_up>-0.02 :
			num = 4
		else :
			num = 6
	mycol2 = mydb["day_110011"]
	mydoc2 = mycol2.find_one({"_id":x["_id"]})

	dict_line = {}
	dict_line['_id'] = x["_id"]
	dict_line['300_close'] = x["close"]
	dict_line['close'] = mydoc2["close"]
	dict_line['raise_or_up'] = raise_or_up
	dict_line['amt'] = num*amt
	dict_line['num_copies'] = int(num*amt/(mydoc2["close"]))
	dict_line['get_amt'] = dict_line['num_copies'] * mydoc2["close"]

	collection = mydb["hs300_110011"]
	print(collection.insert_one(dict_line))
	

	print(dict_line)

	date_ss = x["_id"]
	close1 = x["close"]

__author__ = 'Gao'

from pymongo import MongoClient
import time

client = MongoClient()
client = MongoClient('localhost', 27017)
client = MongoClient('mongodb://localhost:27017/')
db = client.TWEETS
tweets=db.tweets


pipeline=[
    {"$sort":{"timestamp":1}},
    {"$match":{"timestamp":{"$ne": None }}},
]



agg = tweets.aggregate(pipeline, allowDiskUse=True)

total=0;
average=0;

matrix=[];
for doc in agg:
    matrix.append(doc["timestamp"]);


for i in range (len(matrix)-1):
    timeArray1 = time.strptime(matrix[i], "%Y-%m-%d %H:%M:%S");timeArray2 = time.strptime(matrix[i+1], "%Y-%m-%d %H:%M:%S");
    year1=timeArray1[0];month1=timeArray1[1];day1=timeArray1[2];hour1=timeArray1[3];mins1=timeArray1[4];secs1=timeArray1[5];
    year2=timeArray2[0];month2=timeArray2[1];day2=timeArray2[2];hour2=timeArray2[3];mins2=timeArray2[4];secs2=timeArray2[5];
    seconds=abs(60*(24*60*(month1-month2)+24*60*(day1-day2)+60*(hour1-hour2)+(mins1-mins2)+(secs1-secs2)/60))
    total=total+seconds

average=total/(len(matrix)-1)
print("Mean time delta in seconds: ",average)


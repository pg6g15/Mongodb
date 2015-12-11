__author__ = 'Gao'

from pymongo import MongoClient
from bson.code import Code

client = MongoClient()
client = MongoClient('localhost', 27017)
client = MongoClient('mongodb://localhost:27017/')
db = client.TWEETS
tweets=db.tweets


pipeline1=[
    {"$sort":{"timestamp":-1}},
    {"$limit":1 }
]


pipeline2=[
    {"$sort":{"timestamp":1}},
    {"$match":{"timestamp":{"$ne": None }}},
    {"$limit":1 }
]


late=db.tweets.aggregate(pipeline1,allowDiskUse=True)
early=db.tweets.aggregate(pipeline2,allowDiskUse=True)


for late_time in late:
    print("Late time: ",late_time["timestamp"])

for early_time in early:
    print("Early time",early_time["timestamp"])

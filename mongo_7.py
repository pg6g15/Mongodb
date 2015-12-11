__author__ = 'Gao'

from pymongo import MongoClient
from bson.code import Code

client = MongoClient()
client = MongoClient('localhost', 27017)
client = MongoClient('mongodb://localhost:27017/')
db = client.TWEETS
tweets=db.tweets

mapper = Code("""
              function () {
              var count = 0;

              var string=this.text;

              if (typeof string === 'string' || string instanceof String)
                string=string;
              else
                string=" ";


              var res = (string.match(/#/g) || []);

              count=res.length;

              emit(this._id,count);
              }
               """)


reducer = Code("""
                function (key, values) {
                return Array.sum(values);
                }
               """)

try :
    result = db.tweets.map_reduce(mapper, reducer, "hastag")
except Exception as ex:
    print(ex)



pipeline=[
    {"$match":{"value":{"$ne": None }}},

]
agg = result.aggregate(pipeline, allowDiskUse=True)

total=0
for doc in agg:
    total=total+doc["value"];


result=float(total)/float(db.tweets.count())
print("The average hashtags in a message",result)





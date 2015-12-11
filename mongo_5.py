__author__ = 'Gao'

from pymongo import MongoClient
from bson.code import Code

client = MongoClient()
client = MongoClient('localhost', 27017)
client = MongoClient('mongodb://localhost:27017/')
db = client.TWEETS
tweets=db.tweets



mapper = Code("""
              function () {emit(this._id,this.text.length);}
               """)


reducer = Code("""
                function (key, values) {return values}
               """)

try :
    result = db.tweets.map_reduce(mapper, reducer, "mean_length")
except Exception as ex:
    print(ex)



pipeline=[
    {"$match":{"value":{"$ne": None }}},
]

agg = result.aggregate(pipeline, allowDiskUse=True)

total=0
average=0

for doc in agg:
    total=total+doc["value"]

print("The mean lenght of a message",total/db.tweets.count())



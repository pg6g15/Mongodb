__author__ = 'Gao'

from pymongo import MongoClient
from bson.code import Code

client = MongoClient()
client = MongoClient('localhost', 27017)
client = MongoClient('mongodb://localhost:27017/')
db = client.TWEETS
tweets=db.tweets

mapper = Code("""
              function () {emit(this.id_member, 1);}
               """)


reducer = Code("""
                function (key, values) {
                    return Array.sum(values);
               }
               """)

try :
    result = db.tweets.map_reduce(mapper, reducer, "top10_tweets")
except Exception as ex:
    print(ex)
pipeline=[
    {"$sort":{"value":-1}},
    {"$limit":10 }
]

agg = db.myResults.aggregate(pipeline )
total = db.tweets.count()
print("Total tweets: ",total)



total_10=0;
for doc in agg:
    total_10=total_10+doc['value'];

print("Top10 tweets: ",total_10)

print("The tweet that top 10 users publish in percentage: ",(total_10/total)*100)






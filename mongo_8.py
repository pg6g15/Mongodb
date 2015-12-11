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
              var lat=this.geo_lat.toFixed(2);
              var lng=this.geo_lng.toFixed(2);
              var result=[lat,lng]
              emit(this._id,result);
              }
               """)
reducer = Code("""
                function (key, values) {
                    return Array.sum(values);
                }
               """)

try :
    result = db.tweets.map_reduce(mapper, reducer, "lat_lng")
except Exception as ex:
    print(ex)


pipeline=[
    {"$group":{"_id":"$value","count":{"$sum":1}}}
]

agg = db.lat_lng.aggregate(pipeline, allowDiskUse=True)

total_number=[]
total_location=[]
for doc in agg:
    total_number.append(doc["count"])
    total_location.append(doc["_id"])

result=max(total_number)
location=total_number.index(result)

print("How many person tweets: ",result)
print("The location is: ",total_location[location])


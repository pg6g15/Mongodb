__author__ = 'Gao'

from pymongo import MongoClient

conn = MongoClient('localhost',27017)
client = MongoClient()
client = MongoClient('localhost', 27017)
client = MongoClient('mongodb://localhost:27017/')


db = client.TWEETS
tweets=db.tweets


number=len(tweets.distinct("id_member"))
print("The unique user: ",number)
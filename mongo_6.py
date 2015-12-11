__author__ = 'Gao'

from pymongo import MongoClient
from bson.code import Code

client = MongoClient()
client = MongoClient('localhost', 27017)
client = MongoClient('mongodb://localhost:27017/')
db = client.TWEETS
tweets=db.tweets


pipeline=[
    {"$match":{"text":{"$type": 2 }}},
    {"$limit":10}
]

agg = tweets.aggregate(pipeline, allowDiskUse=True)

total_word=[]
total_number=[]
number=[]
flag=0
word=[]
for doc in agg:
    matrix=doc["text"].split()
    for i in range(len(matrix)):
        for j in range(len(total_word)):
            if total_word[j] == matrix[i]:
                total_number[j]=total_number[j]+1
                flag=1
        if flag is 0:
            total_word.append(matrix[i])
            total_number.append(1)
        elif flag is 1:
            flag=0



total_number_order=sorted(total_number,reverse=True)
print(total_number_order)
print(total_number_order)

for i in range(10):
    number.append(total_number_order[i])


last_index=0
for i in range(10):
    if i is 0:
        location=total_number.index(number[i])
        word.append(total_word[location])
        last_index=location
    else:
        if number[i] is number[i-1]:
            location=total_number.index(number[i], last_index+1, len(total_number))
            word.append(total_word[location])
            last_index=location
        else:
            location=total_number.index(number[i]);
            word.append(total_word[location])
            last_index=location


print(word)
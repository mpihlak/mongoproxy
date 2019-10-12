import pymongo

con = pymongo.MongoClient("mongodb://localhost:27111")
bigcollection = con['test']['bigcollection']
for i in range(1000):
    bigcollection.insert_one({ "a": "bbbbbbbbbbbbbbbbbbbb", "b": "CCCCCCCCCCCCCCCCCC"})

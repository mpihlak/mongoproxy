import pymongo

con = pymongo.MongoClient("mongodb://localhost:27111")
bigcollection = con['test']['bigcollection']
for _ in range(1000):
    payload = dict()
    for i in range(100):
        payload['element_%d' % i] = 'x' * 100
    bigcollection.insert_one(payload)

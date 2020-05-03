import time
import pymongo
import sys, os

if len(sys.argv) != 2:
    mongo_uri = "mongodb://localhost:27111"
else:
    mongo_uri = sys.argv[1]

count = 0
while 1:
    count += 1
    print(f"#{count} connecting to {mongo_uri}")
    con = pymongo.MongoClient(mongo_uri)

    bigcollection = con['test']['bigcollection']

    start = time.time()
    for i in range(1000):
        for res in bigcollection.find({}).limit(10):
            pass
    elapsed = time.time() - start
    print(f"{i} queries in {elapsed:.5f} seconds, avg {i / elapsed:.5f} / sec")

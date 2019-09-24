import sys
import time
import os
import pymongo

con = pymongo.MongoClient("mongodb://localhost:27111")
bigcollection = con['test']['bigcollection']

print "Inserting some data"
for i in range(1000):
    bigcollection.insert_one({ "a": "bbbbbbbbbbbbbbbbbbbb", "b": "CCCCCCCCCCCCCCCCCC"})

while True:
    print "Now fetching it. ^C to exit"
    count = 1000

    start = time.time()
    for i in range(count):
        for res in bigcollection.find({}).limit(1000):
            pass

    elapsed = time.time() - start
    print "%d fetches done in %.2f seconds, %.3f / find" % (count, elapsed, elapsed/count)

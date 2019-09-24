import sys
import time
import os
import pymongo

con = pymongo.MongoClient("mongodb://localhost:27111")
bigcollection = con['test']['bigcollection']

print "Inserting some data"
for i in range(1000):
    bigcollection.insert_one({ "a": "bbbbbbbbbbbbbbbbbbbb", "b": "CCCCCCCCCCCCCCCCCC"})

start = time.time()
coll = con['EchoGlobal']['botcollections']

print "Now fetching it"
count = 1000
for i in range(count):
    for res in coll.find({}).limit(1000):
        pass

elapsed = time.time() - start
print "%d fetches done in %.2f seconds, %.3f / find" % (count, elapsed, elapsed/count)

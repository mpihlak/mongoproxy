import sys
import time
import os
import pymongo

if len(sys.argv) > 1:
    mongo_uri = sys.argv[1]
else:
    mongo_uri = "mongodb://localhost:27111"

print(f"Connecting to {mongo_uri}")
con = pymongo.MongoClient(mongo_uri)
bigcollection = con['test']['bigcollection']

print("Cleaning up")
bigcollection.drop()

print("Inserting some data")
for i in range(1000):
    bigcollection.insert_one({ "a": "bbbbbbbbbbbbbbbbbbbb", "b": "CCCCCCCCCCCCCCCCCC"})

print("Running the fetch loop until ^C")
try:
    while True:
        count = 1000

        start = time.time()
        for i in range(count):
            for res in bigcollection.find({}).limit(1000):
                pass

        elapsed = time.time() - start
        print(f"{count} fetches done in {elapsed:.2f} seconds, {elapsed/count:.3f} / find")
except KeyboardInterrupt:
    pass

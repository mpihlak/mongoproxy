#!/usr/bin/env python3

import time
import pymongo
import sys
import os

import numpy as np

ITEM_COUNT = 1000
RUN_DURATION = 3


if len(sys.argv) != 2:
    mongo_uri = "mongodb://localhost:27111"
else:
    mongo_uri = sys.argv[1]

print(f"Connecting to {mongo_uri}")
con = pymongo.MongoClient(mongo_uri)

for item_size in [128, 1024, 8192, 16384, 65536, 131072, 262144, 524288, 1048576]:
    coll = con['test']['benchmarkdata']
    coll.drop()
    coll.create_index('i', unique=True)
    for i in range(ITEM_COUNT):
        coll.insert_one(dict(i=i, a='x' * item_size))

    counter = 0
    t = end = start = time.perf_counter()
    latencies = []
    while end - start < RUN_DURATION:
        for res in coll.find({}).limit(1):
            pass
        end = time.perf_counter()
        latencies.append((end - t) * 1000)
        t = end
        if not res:
            print("dude, where's the data?")
            break
        counter = (counter + 1) % ITEM_COUNT

    elapsed = end - start
    p99 = np.percentile(latencies, 99)
    p50 = np.percentile(latencies, 50)
    print(f"item_size={item_size}: {counter} queries in {elapsed:.2f} seconds, avg {counter/elapsed:.3f}/sec, p99={p99:.5f}, p50={p50:.5f}")

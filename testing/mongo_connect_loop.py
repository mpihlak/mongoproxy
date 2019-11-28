import pymongo

count = 0
while 1:
    count += 1
    print "connection", count
    con = pymongo.MongoClient("mongodb://localhost:27111")

    bigcollection = con['test']['bigcollection']
    for res in bigcollection.find({}).limit(1000):
        pass

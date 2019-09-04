import pymongo

con = pymongo.MongoClient("mongodb://localhost:27111")
bigcollection = con['test']['bigcollection']
while True:
    print "Inserting"
    for i in range(1000):
        bigcollection.insert_one({ "a": "bbbbbbbbbbbbbbbbbbbb", "b": "CCCCCCCCCCCCCCCCCC"})
    print "Removing"
    res = bigcollection.delete_many({})
    print "deleted:", res.deleted_count

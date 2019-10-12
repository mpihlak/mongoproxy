import pymongo

count = 0
while 1:
    count += 1
    print "connection", count
    con = pymongo.MongoClient("mongodb://localhost:27111")

    bigcollection = con['test']['bigcollection']
    for res in bigcollection.find({}).limit(1000):
        pass

    if 0:
        bigcollection = con['test']['bigcollection']

        print "Inserting"
        for i in range(1000):
            bigcollection.insert_one({ "a": "bbbbbbbbbbbbbbbbbbbb", "b": "CCCCCCCCCCCCCCCCCC"})
        print "Removing"
        res = bigcollection.delete_many({})
        print "deleted:", res.deleted_count

    con.close()

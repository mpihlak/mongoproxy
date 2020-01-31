package main

import (
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"log"
)

func main() {
	session, err := mgo.Dial("mongodb://127.0.0.1:27113/test?appName=fookittens")
	if err != nil {
		log.Fatalf("cannot connect: %v", err)
	}

	log.Println("connected.")
	coll := session.DB("").C("kittens")

	log.Println("Querying one")
	var kittens []bson.M
	if err := coll.Find(nil).All(&kittens); err != nil {
		log.Fatalf("error querying: %v", err)
	}

	log.Printf("%v", kittens)

	log.Println("Inserting")
	if err := coll.Insert(bson.M{"name": "Fux", "number": 123}); err != nil {
		log.Fatalf("Insert failed: %v", err)
	}

	log.Println("Updating")
	if err := coll.Update(bson.M{"name": "Fux"}, bson.M{"name": "Foxy", "number": 124}); err != nil {
		log.Fatalf("Update failed: %v", err)
	}

	log.Println("Deleting")
	if err := coll.Remove(bson.M{"name": "Foxy"}); err != nil {
		log.Fatalf("Delete failed: %v", err)
	}

	log.Println("Iterating")
	iter := coll.Find(nil).Iter()
	var kitten bson.M
	for iter.Next(&kitten) {
		log.Println(kitten)
	}

	log.Println("Done")
}

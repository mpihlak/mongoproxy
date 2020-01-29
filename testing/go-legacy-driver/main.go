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
	c := session.DB("").C("kittens")

	var kittens []bson.M
	if err := c.Find(nil).All(&kittens); err != nil {
		log.Fatalf("error querying: %v", err)
	}

	for _, v := range kittens {
		log.Printf("%v %v", v["name"], v["number"])
	}

}

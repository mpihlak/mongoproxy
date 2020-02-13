package main

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Kitten struct {
	Name   string
	Number float64
}

func check_ok(msg string, err interface{}) {
	if err != nil {
		log.Fatalf("%v: %v", msg, err)
	}
}

func main() {
	uri := "mongodb://127.0.0.1:27113/test?appName=fookittens"
	clientOptions := options.Client().ApplyURI(uri)
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	client, err := mongo.Connect(ctx, clientOptions)
	check_ok("connect", err)

	log.Println("Connected.")
	db := client.Database("test")
	coll := db.Collection("kittens")

	log.Println("Inserting one")
	kitten := Kitten{Name: "Fux", Number: 42}
	insertResult, err := coll.InsertOne(ctx, kitten)
	check_ok("insert_one", err)
	log.Printf("Inserted: %+v", insertResult)

	log.Println("Updating one")
	updateResult, err := coll.UpdateOne(ctx,
		bson.M{"name": "Fux", "$comment": "DoUpdate"},
		bson.D{{"$inc", bson.D{{"number", 1}}}})
	check_ok("update", err)
	log.Printf("Updated: %+v", updateResult)

	log.Println("Finding one")
	err = coll.FindOne(ctx, bson.M{"name": "Fux", "$comment": "DoFindOne"}).Decode(&kitten)
	check_ok("find_one", err)
	log.Printf("found: %+v", kitten)

	log.Println("Deleting one")
	deleteResult, err := coll.DeleteOne(ctx,
		bson.M{"_id": insertResult.InsertedID, "$comment": "DoDeleteOne"})
	check_ok("delete_one", err)
	log.Printf("deleted: %+v", deleteResult)

	log.Println("Done")
}

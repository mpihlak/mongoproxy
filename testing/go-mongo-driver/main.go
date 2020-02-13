package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-lib/metrics"

	"github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	jaegerlog "github.com/uber/jaeger-client-go/log"
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

func spanText(span opentracing.Span) string {
	return fmt.Sprintf("uber-trace-id:%s", span)
}

func main() {
	cfg := jaegercfg.Configuration{
		Sampler:  &jaegercfg.SamplerConfig{Type: jaeger.SamplerTypeConst, Param: 1},
		Reporter: &jaegercfg.ReporterConfig{LogSpans: true},
	}

	jLogger := jaegerlog.StdLogger
	jMetricsFactory := metrics.NullFactory

	// Initialize tracer with a logger and a metrics factory
	closer, err := cfg.InitGlobalTracer(
		"go-tracing-test",
		jaegercfg.Logger(jLogger),
		jaegercfg.Metrics(jMetricsFactory),
	)
	check_ok("init tracing", err)
	defer closer.Close()

	uri := "mongodb://127.0.0.1:27113/test?appName=fookittens"
	clientOptions := options.Client().ApplyURI(uri)
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	client, err := mongo.Connect(ctx, clientOptions)
	check_ok("connect", err)

	log.Println("Connected.")
	db := client.Database("test")
	coll := db.Collection("kittens")

	rootSpan, ctx := opentracing.StartSpanFromContext(ctx, "root_trace")
	defer rootSpan.Finish()

	insertSpan := opentracing.StartSpan("insert_one", opentracing.ChildOf(rootSpan.Context()))
	log.Printf("span=%s", insertSpan)
	log.Println("Inserting one")
	kitten := Kitten{Name: "Fux", Number: 42}
	insertResult, err := coll.InsertOne(ctx, kitten)
	insertSpan.Finish()
	check_ok("insert_one", err)
	log.Printf("Inserted: %+v", insertResult)

	log.Println("Updating one")
	updateSpan := opentracing.StartSpan("update_one", opentracing.ChildOf(rootSpan.Context()))
	updateResult, err := coll.UpdateOne(ctx,
		bson.M{"name": "Fux", "$comment": spanText(updateSpan)},
		bson.D{{"$inc", bson.D{{"number", 1}}}})
	updateSpan.Finish()
	check_ok("update", err)
	log.Printf("Updated: %+v", updateResult)

	log.Println("Finding one")
	findSpan := opentracing.StartSpan("find_one", opentracing.ChildOf(rootSpan.Context()))
	err = coll.FindOne(ctx,
		bson.M{"name": "Fux"},
		options.FindOne().SetComment(spanText(findSpan))).Decode(&kitten)
	findSpan.Finish()
	check_ok("find_one", err)
	log.Printf("found: %+v", kitten)

	log.Println("Deleting one")
	deleteSpan := opentracing.StartSpan("delete_one", opentracing.ChildOf(rootSpan.Context()))
	deleteResult, err := coll.DeleteOne(ctx,
		bson.M{"_id": insertResult.InsertedID, "$comment": spanText(deleteSpan)})
	deleteSpan.Finish()
	check_ok("delete_one", err)
	log.Printf("deleted: %+v", deleteResult)

	log.Println("Find all")
	findAllSpan := opentracing.StartSpan("find_all", opentracing.ChildOf(rootSpan.Context()))
	cur, err := coll.Find(ctx, bson.M{"$comment": spanText(findAllSpan)}, options.Find()) // .Comment(spanText(findAllSpan))
	check_ok("find_all", err)
	for cur.Next(ctx) {
		var cat Kitten
		err := cur.Decode(&cat)
		check_ok("fetch", err)
		log.Printf("fetched: %+v", cat)
	}
	findAllSpan.Finish()
	check_ok("cursor", cur.Err())
	cur.Close(ctx)

	log.Println("Done")
}

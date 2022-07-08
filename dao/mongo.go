package dao

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo/options"

	"go.mongodb.org/mongo-driver/mongo"
)

type MongoDao struct {
	db     *mongo.Database
	ABTest *mongo.Collection
}

var once sync.Once
var singleInstance *MongoDao

func GetInstance() *MongoDao {
	once.Do(func() {
		uri := os.Getenv("mongo_uri")

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		clientOptions := options.Client().ApplyURI(uri).SetMaxPoolSize(50)
		client, err := mongo.Connect(ctx, clientOptions)

		if err != nil {
			fmt.Printf("connect mongo err: %v\n", err.Error())
		}

		err = client.Ping(context.TODO(), nil)

		if err != nil {
			fmt.Printf("ping mongo err: %v\n", err.Error())
		}
		fmt.Printf("connected to mongo %s success\n", uri)

		singleInstance = &MongoDao{}
		singleInstance.db = client.Database("ABTestDB")
		singleInstance.ABTest = singleInstance.db.Collection("ABTestCollection")
	})
	return singleInstance
}

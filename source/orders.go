package main

import "go.mongodb.org/mongo-driver/v2/bson"

type MongoDBOrders struct {
	ID    bson.ObjectID `json:"id,omitempty" bson:"_id,omitempty"`
	OldID uint64        `json:"old_id" bson:"old_id"`
}

type MySQLOrders struct {
}

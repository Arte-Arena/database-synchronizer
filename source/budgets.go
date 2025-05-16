package main

import "go.mongodb.org/mongo-driver/v2/bson"

type MongoDBProduct struct {
	ProductId  bson.ObjectID `json:"product_id" bson:"product_id"`
	Properties any           `json:"properties" bson:"properties"`
}

type MongoDBBudgets struct {
	ID            bson.ObjectID    `json:"id,omitempty" bson:"_id,omitempty"`
	OldID         uint64           `json:"old_id" bson:"old_id"`
	Seller        bson.ObjectID    `json:"seller" bson:"seller"`
	RelatedLead   bson.ObjectID    `json:"related_lead" bson:"related_lead"`
	RelatedClient bson.ObjectID    `json:"related_client" bson:"related_client"`
	ProductsList  []MongoDBProduct `json:"products_list" bson:"products_list"`
	Cep           string           `json:"cep" bson:"cep"`
}

type MySQLBudgets struct {
}

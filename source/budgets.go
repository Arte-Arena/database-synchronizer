package main

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

type MongoDBProduct struct {
	ProductId  bson.ObjectID `json:"product_id" bson:"product_id"`
	Properties any           `json:"properties" bson:"properties"`
}

type MongoDBDelivery struct {
	Option   string  `json:"option" bson:"option"`
	Deadline uint    `json:"deadline" bson:"deadline"`
	Price    float64 `json:"price" bson:"price"`
}

type MongoDBEarlyMode struct {
	Date time.Time `json:"date" bson:"date"`
	Tax  float64   `json:"tax" bson:"tax"`
}

type MongoDBDiscount struct {
	Type       string  `json:"type" bson:"type"`
	Value      float64 `json:"value" bson:"value"`
	Percentage float64 `json:"percentage" bson:"percentage"`
}

type MongoDBBudgets struct {
	ID                 bson.ObjectID    `json:"id,omitempty" bson:"_id,omitempty"`
	OldID              uint64           `json:"old_id" bson:"old_id"`
	Seller             bson.ObjectID    `json:"seller" bson:"seller"`
	RelatedLead        bson.ObjectID    `json:"related_lead" bson:"related_lead"`
	RelatedClient      bson.ObjectID    `json:"related_client" bson:"related_client"`
	ProductsList       []MongoDBProduct `json:"products_list" bson:"products_list"`
	Cep                string           `json:"cep" bson:"cep"`
	Delivery           MongoDBDelivery  `json:"delivery" bson:"delivery"`
	EarlyMode          MongoDBEarlyMode `json:"early_mode" bson:"early_mode"`
	Discount           MongoDBDiscount  `json:"discount" bson:"discount"`
	Gifts              []MongoDBProduct `json:"gifts" bson:"gifts"`
	ProductionDeadline uint             `json:"production_deadline" bson:"production_deadline"`
	DeliveryForecast   time.Time        `json:"delivery_forecast" bson:"delivery_forecast"`
	TotalValue         float64          `json:"total_value" bson:"total_value"`
	CreatedAt          time.Time        `json:"created_at" bson:"created_at,omitempty"`
	UpdatedAt          time.Time        `json:"updated_at" bson:"updated_at,omitempty"`
}

type MySQLBudgets struct {
}

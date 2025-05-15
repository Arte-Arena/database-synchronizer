package main

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

type MongoDBLeads struct {
	ID             bson.ObjectID   `json:"id,omitempty" bson:"_id,omitempty"`
	Name           string          `json:"name,omitempty" bson:"name,omitempty"`
	Nickname       string          `json:"nickname,omitempty" bson:"nickname,omitempty"`
	Phone          string          `json:"phone,omitempty" bson:"phone,omitempty"`
	Type           string          `json:"type,omitempty" bson:"type,omitempty"`
	Segment        string          `json:"segment,omitempty" bson:"segment,omitempty"`
	CreatedAt      time.Time       `json:"created_at" bson:"created_at,omitempty"`
	UpdatedAt      time.Time       `json:"updated_at" bson:"updated_at,omitempty"`
	Status         string          `json:"status,omitempty" bson:"status,omitempty"`
	Source         string          `json:"source,omitempty" bson:"source,omitempty"`
	UniqueID       string          `json:"unique_id,omitempty" bson:"unique_id,omitempty"`
	RelatedBudgets []bson.ObjectID `json:"related_budgets,omitempty" bson:"related_budgets,omitempty"`
	RelatedOrders  []bson.ObjectID `json:"related_orders,omitempty" bson:"related_orders,omitempty"`
	RelatedClient  bson.ObjectID   `json:"related_client,omitempty" bson:"related_client,omitempty"`
	Rating         string          `json:"classification,omitempty" bson:"classification,omitempty"`
	Notes          string          `json:"notes,omitempty" bson:"notes,omitempty"`
	Responsible    bson.ObjectID   `json:"responsible,omitempty" bson:"responsible,omitempty"`
}

type MySQLLeads struct {
}

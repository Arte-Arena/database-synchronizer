package main

import (
	"context"
	"database_sync/source/database"
	"database_sync/source/utils"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	SUPER_ADMIN = iota + 1
	IT
	ADMIN
	LEADER
	COLLABORATOR
	DESIGNER
	DESIGNER_COORDINATOR
	PRODUCTION
	COMMERCIAL
)

type MongoDBUsers struct {
	ID    bson.ObjectID `json:"id,omitempty" bson:"_id,omitempty"`
	OldID uint          `json:"old_id" bson:"old_id"`
	Name  string        `json:"name" bson:"name"`
	Email string        `json:"email" bson:"email"`
	Role  string        `json:"role" bson:"role"`
}

type MYSQLUsers struct {
	ID              uint64    `db:"id"`
	Name            string    `db:"name"`
	Email           string    `db:"email"`
	EmailVerifiedAt time.Time `db:"email_verified_at"`
	Password        string    `db:"password"`
	RememberToken   string    `db:"remember_token"`
	CreatedAt       time.Time `db:"created_at"`
	UpdatedAt       time.Time `db:"updated_at"`
}

func SyncUsers() {
	ctx, cancel := context.WithTimeout(context.Background(), database.MONGODB_TIMEOUT)
	defer cancel()

	mongoURI := os.Getenv(utils.MONGODB_URI)
	opts := options.Client().ApplyURI(mongoURI)
	mongoClient, err := mongo.Connect(opts)
	if err != nil {
		// LOG HERE
	}
	defer mongoClient.Disconnect(ctx)

	collection := mongoClient.Database(database.GetDB()).Collection(database.COLLECTION_USERS)
}

package main

import (
	"context"
	"database_sync/source/database"
	"database_sync/source/utils"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MongoDBUsers struct {
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
		// PANIC HERE
	}
	defer mongoClient.Disconnect(ctx)

	collection := mongoClient.Database(database.GetDB()).Collection(database.COLLECTION_USERS)
}

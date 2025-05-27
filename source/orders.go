package main

import (
	"context"
	"database/sql"
	"database_sync/database"
	"database_sync/utils"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MongoDBOrders struct {
	ID    bson.ObjectID `json:"id,omitempty" bson:"_id,omitempty"`
	OldID uint64        `json:"old_id" bson:"old_id"`
}

type MySQLOrders struct {
	ID string `db:"id"`
}

func SyncOrders() error {
	mysqlURI := os.Getenv("MYSQL_URI")

	mysqlDB, err := sql.Open("mysql", mysqlURI)
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	defer mysqlDB.Close()

	mysqlDB.SetConnMaxLifetime(database.MYSQL_CONN_MAX_LIFETIME)
	mysqlDB.SetMaxOpenConns(database.MYSQL_MAX_OPEN_CONNS)
	mysqlDB.SetMaxIdleConns(database.MYSQL_MAX_IDLE_CONNS)

	if err := mysqlDB.Ping(); err != nil {
		return fmt.Errorf("failed to ping MySQL: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), database.MONGODB_TIMEOUT)
	defer cancel()

	mongoURI := os.Getenv(utils.MONGODB_URI)
	opts := options.Client().ApplyURI(mongoURI)
	mongoClient, err := mongo.Connect(opts)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}
	defer mongoClient.Disconnect(ctx)

	ordersCollection := mongoClient.Database(database.GetDB()).Collection(database.COLLECTION_ORDERS)

	allOrdersMap := make(map[uint64]*MySQLOrders)

	dataRows, err := mysqlDB.Query("SELECT id FROM pedidos_arte_final WHERE id IS NOT NULL")
	if err != nil {
		return fmt.Errorf("failed to query MySQL orders data: %w", err)
	}

	for dataRows.Next() {
		var id sql.NullInt64
		if err := dataRows.Scan(&id); err != nil {
			dataRows.Close()
			return fmt.Errorf("failed to scan MySQL order data: %w", err)
		}

		if !id.Valid || id.Int64 <= 0 {
			continue
		}

		order := &MySQLOrders{
			ID: fmt.Sprintf("%d", id.Int64),
		}
		allOrdersMap[uint64(id.Int64)] = order
	}
	dataRows.Close()

	if err = dataRows.Err(); err != nil {
		return fmt.Errorf("error iterating MySQL data rows: %w", err)
	}

	if len(allOrdersMap) == 0 {
		return nil
	}

	mysqlIDs := make(map[uint64]bool, len(allOrdersMap))
	for id := range allOrdersMap {
		mysqlIDs[id] = true
	}

	mongoIDs := make(map[uint64]bool)
	mongoOrdersData := make(map[uint64]MongoDBOrders)

	cursor, err := ordersCollection.Find(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("failed to query MongoDB orders: %w", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var order MongoDBOrders
		if err := cursor.Decode(&order); err != nil {
			return fmt.Errorf("failed to decode MongoDB order: %w", err)
		}
		if order.OldID > 0 {
			mongoIDs[order.OldID] = true
			mongoOrdersData[order.OldID] = order
		}
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("error iterating MongoDB cursor: %w", err)
	}

	idsToDelete := []uint64{}
	for mongoID := range mongoIDs {
		if !mysqlIDs[mongoID] {
			idsToDelete = append(idsToDelete, mongoID)
		}
	}

	if len(idsToDelete) > 0 {
		deleteFilter := bson.D{{Key: "old_id", Value: bson.D{{Key: "$in", Value: idsToDelete}}}}
		_, err := ordersCollection.DeleteMany(ctx, deleteFilter)
		if err != nil {
			return fmt.Errorf("failed to delete non-existing orders from MongoDB: %w", err)
		}
	}

	idsToUpsert := []uint64{}
	for id := range allOrdersMap {
		_, exists := mongoOrdersData[id]
		if !exists {
			idsToUpsert = append(idsToUpsert, id)
		}
	}

	if len(idsToUpsert) == 0 {
		return nil
	}

	batchSize := 50
	if len(idsToUpsert) > 1000 {
		batchSize = 200
	} else if len(idsToUpsert) > 5000 {
		batchSize = 500
	}

	bulkOperations := []mongo.WriteModel{}

	for _, id := range idsToUpsert {
		mongoOrder := MongoDBOrders{
			OldID: id,
		}

		filter := bson.D{{Key: "old_id", Value: mongoOrder.OldID}}
		update := bson.D{{Key: "$set", Value: mongoOrder}}

		upsertModel := mongo.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(update).
			SetUpsert(true)

		bulkOperations = append(bulkOperations, upsertModel)

		if len(bulkOperations) >= batchSize {
			if _, err := ordersCollection.BulkWrite(ctx, bulkOperations); err != nil {
				return fmt.Errorf("failed to execute bulk write: %w", err)
			}
			bulkOperations = []mongo.WriteModel{}
		}
	}

	if len(bulkOperations) > 0 {
		if _, err := ordersCollection.BulkWrite(ctx, bulkOperations); err != nil {
			return fmt.Errorf("failed to execute final bulk write: %w", err)
		}
	}

	return nil
}

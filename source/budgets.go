package main

import (
	"context"
	"database/sql"
	"database_sync/source/database"
	"database_sync/source/utils"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// type MongoDBProduct struct {
// 	ProductId  bson.ObjectID `json:"product_id" bson:"product_id"`
// 	Properties any           `json:"properties" bson:"properties"`
// }

// type MongoDBDelivery struct {
// 	Option   string  `json:"option" bson:"option"`
// 	Deadline uint    `json:"deadline" bson:"deadline"`
// 	Price    float64 `json:"price" bson:"price"`
// }

// type MongoDBEarlyMode struct {
// 	Date time.Time `json:"date" bson:"date"`
// 	Tax  float64   `json:"tax" bson:"tax"`
// }

// type MongoDBDiscount struct {
// 	Type       string  `json:"type" bson:"type"`
// 	Value      float64 `json:"value" bson:"value"`
// 	Percentage float64 `json:"percentage" bson:"percentage"`
// }

// type MongoDBInstallments struct {
// 	Date  time.Time `json:"date" bson:"date"`
// 	Value float64   `json:"value" bson:"value"`
// }

// type MongoDBBilling struct {
// 	Type         string                `json:"type" bson:"type"`
// 	Installments []MongoDBInstallments `json:"installments" bson:"installments"`
// }

type MongoDBBudgets struct {
	ID    bson.ObjectID `json:"id,omitempty" bson:"_id,omitempty"`
	OldID uint64        `json:"old_id" bson:"old_id"`
	//Approver           bson.ObjectID    `json:"approver" bson:"approver"`
	//Seller             bson.ObjectID    `json:"seller" bson:"seller"`
	//RelatedLead        bson.ObjectID    `json:"related_lead" bson:"related_lead"`
	//RelatedClient      bson.ObjectID    `json:"related_client" bson:"related_client"`
	//ProductsList       []MongoDBProduct `json:"products_list" bson:"products_list"`
	//Cep                string           `json:"cep" bson:"cep"`
	//Delivery           MongoDBDelivery  `json:"delivery" bson:"delivery"`
	//EarlyMode          MongoDBEarlyMode `json:"early_mode" bson:"early_mode"`
	//Discount           MongoDBDiscount  `json:"discount" bson:"discount"`
	//Gifts              []MongoDBProduct `json:"gifts" bson:"gifts"`
	//ProductionDeadline uint             `json:"production_deadline" bson:"production_deadline"`
	//Status             string           `json:"status" bson:"status"`
	//PaymentMethod      string           `json:"payment_method" bson:"payment_method"`
	//Billing            MongoDBBilling   `json:"billing" bson:"billing"`
	//Trello_uri         string           `json:"trello_uri" bson:"trello_uri"`
	//Notes              string           `json:"notes" bson:"notes"`
	//DeliveryForecast   time.Time        `json:"delivery_forecast" bson:"delivery_forecast"`
	CreatedAt time.Time `json:"created_at" bson:"created_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at" bson:"updated_at,omitempty"`
}

type MySQLBudgets struct {
	ID        string    `db:"id"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

func SyncBudgets() error {
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

	allBudgetsMap := make(map[uint64]*MySQLBudgets)

	dataRows, err := mysqlDB.Query("SELECT id, created_at, updated_at FROM orcamentos WHERE id IS NOT NULL")
	if err != nil {
		return fmt.Errorf("failed to query MySQL orcamentos data: %w", err)
	}

	for dataRows.Next() {
		budget := &MySQLBudgets{}
		var createdAtStr, updatedAtStr []byte
		var id sql.NullInt64

		err := dataRows.Scan(
			&id,
			&createdAtStr,
			&updatedAtStr,
		)
		if err != nil {
			dataRows.Close()
			return fmt.Errorf("failed to scan MySQL budget data: %w", err)
		}

		if !id.Valid || id.Int64 <= 0 {
			continue
		}

		budget.ID = fmt.Sprintf("%d", id.Int64)
		budget.CreatedAt, err = time.Parse("2006-01-02 15:04:05", string(createdAtStr))
		if err != nil {
			dataRows.Close()
			return fmt.Errorf("failed to parse created_at datetime: %w", err)
		}

		budget.UpdatedAt, err = time.Parse("2006-01-02 15:04:05", string(updatedAtStr))
		if err != nil {
			dataRows.Close()
			return fmt.Errorf("failed to parse updated_at datetime: %w", err)
		}

		allBudgetsMap[uint64(id.Int64)] = budget
	}
	dataRows.Close()

	if err = dataRows.Err(); err != nil {
		return fmt.Errorf("error iterating MySQL data rows: %w", err)
	}

	if len(allBudgetsMap) == 0 {
		fmt.Printf("[SYNC_BUDGETS] No records found in MySQL to synchronize: %s\n",
			time.Now().Format("2006-01-02 15:04:05"))
		return nil
	}

	mysqlIDs := make(map[uint64]bool, len(allBudgetsMap))
	for id := range allBudgetsMap {
		mysqlIDs[id] = true
	}

	budgetsCollection := mongoClient.Database(database.GetDB()).Collection(database.COLLECTION_BUDGETS)
	mongoIDs := make(map[uint64]bool)
	mongoBudgetsData := make(map[uint64]MongoDBBudgets)

	cursor, err := budgetsCollection.Find(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("failed to query MongoDB budgets: %w", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var budget MongoDBBudgets
		if err := cursor.Decode(&budget); err != nil {
			return fmt.Errorf("failed to decode MongoDB budget: %w", err)
		}
		if budget.OldID > 0 {
			mongoIDs[budget.OldID] = true
			mongoBudgetsData[budget.OldID] = budget
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
		_, err := budgetsCollection.DeleteMany(ctx, deleteFilter)
		if err != nil {
			return fmt.Errorf("failed to delete non-existing budgets from MongoDB: %w", err)
		}
	}

	recordsToUpsert := make([]uint64, 0)

	for id, mysqlBudget := range allBudgetsMap {
		mongoBudget, exists := mongoBudgetsData[id]

		if !exists {
			recordsToUpsert = append(recordsToUpsert, id)
			continue
		}

		if !mysqlBudget.UpdatedAt.Equal(mongoBudget.UpdatedAt) {
			recordsToUpsert = append(recordsToUpsert, id)
		}
	}

	if len(recordsToUpsert) == 0 {
		return nil
	}

	totalRecords := len(recordsToUpsert)
	batchSize := 50
	if totalRecords > 1000 {
		batchSize = 200
	} else if totalRecords > 5000 {
		batchSize = 500
	}

	bulkOperations := []mongo.WriteModel{}
	processedCount := 0
	bulkWriteCount := 0

	for _, id := range recordsToUpsert {
		budget := allBudgetsMap[id]

		mongoBudget := MongoDBBudgets{
			OldID:     id,
			CreatedAt: budget.CreatedAt,
			UpdatedAt: budget.UpdatedAt,
		}

		filter := bson.D{{Key: "old_id", Value: mongoBudget.OldID}}
		update := bson.D{{Key: "$set", Value: mongoBudget}}

		upsertModel := mongo.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(update).
			SetUpsert(true)

		bulkOperations = append(bulkOperations, upsertModel)
		processedCount++

		if len(bulkOperations) >= batchSize {
			_, err := budgetsCollection.BulkWrite(ctx, bulkOperations)
			if err != nil {
				return fmt.Errorf("failed to bulk write to MongoDB: %w", err)
			}

			bulkOperations = []mongo.WriteModel{}
			bulkWriteCount++
		}
	}

	if len(bulkOperations) > 0 {
		_, err := budgetsCollection.BulkWrite(ctx, bulkOperations)
		if err != nil {
			return fmt.Errorf("failed to bulk write remaining documents to MongoDB: %w", err)
		}
		bulkWriteCount++
	}

	return nil
}

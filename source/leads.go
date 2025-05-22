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
	PlatformId     string          `json:"platform_id,omitempty" bson:"platform_id,omitempty"`
	RelatedBudgets []bson.ObjectID `json:"related_budgets,omitempty" bson:"related_budgets,omitempty"`
	RelatedOrders  []bson.ObjectID `json:"related_orders,omitempty" bson:"related_orders,omitempty"`
	RelatedClient  bson.ObjectID   `json:"related_client,omitempty" bson:"related_client,omitempty"`
	Rating         string          `json:"classification,omitempty" bson:"classification,omitempty"`
	Notes          string          `json:"notes,omitempty" bson:"notes,omitempty"`
	Responsible    bson.ObjectID   `json:"responsible,omitempty" bson:"responsible,omitempty"`
}

type MySQLLeads struct {
	ID        string    `db:"id"`
	Name      *string   `db:"nome"`
	Email     *string   `db:"email"`
	Phone     *string   `db:"telefone"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

func SyncLeads() error {
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

	allLeadsMap := make(map[string]*MySQLLeads, 20000)

	dataRows, err := mysqlDB.Query("SELECT id, nome, email, telefone, created_at, updated_at FROM octa_webhook WHERE id IS NOT NULL")
	if err != nil {
		return fmt.Errorf("failed to query MySQL octa_webhook data: %w", err)
	}

	for dataRows.Next() {
		lead := &MySQLLeads{}
		var createdAtStr, updatedAtStr []byte
		var id sql.NullString

		err := dataRows.Scan(
			&id,
			&lead.Name,
			&lead.Email,
			&lead.Phone,
			&createdAtStr,
			&updatedAtStr,
		)
		if err != nil {
			dataRows.Close()
			return fmt.Errorf("failed to scan MySQL lead data: %w", err)
		}

		if !id.Valid || id.String == "" {
			continue
		}

		lead.ID = id.String
		lead.CreatedAt, err = time.Parse("2006-01-02 15:04:05", string(createdAtStr))
		if err != nil {
			dataRows.Close()
			return fmt.Errorf("failed to parse created_at datetime: %w", err)
		}

		lead.UpdatedAt, err = time.Parse("2006-01-02 15:04:05", string(updatedAtStr))
		if err != nil {
			dataRows.Close()
			return fmt.Errorf("failed to parse updated_at datetime: %w", err)
		}

		allLeadsMap[lead.ID] = lead
	}
	dataRows.Close()

	if err = dataRows.Err(); err != nil {
		return fmt.Errorf("error iterating MySQL data rows: %w", err)
	}

	mysqlIDs := make(map[string]bool, len(allLeadsMap))
	for id := range allLeadsMap {
		mysqlIDs[id] = true
	}

	if len(mysqlIDs) == 0 {
		fmt.Printf("[SYNC_LEADS] Nenhum registro encontrado no MySQL para sincronizar: %s\n",
			time.Now().Format("2006-01-02 15:04:05"))
		return nil
	}

	leadsCollection := mongoClient.Database(database.GetDB()).Collection(database.COLLECTION_LEADS)
	mongoIDs := make(map[string]bool, 20000)
	mongoLeadsData := make(map[string]MongoDBLeads)

	cursor, err := leadsCollection.Find(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("failed to query MongoDB leads: %w", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var lead MongoDBLeads
		if err := cursor.Decode(&lead); err != nil {
			return fmt.Errorf("failed to decode MongoDB lead: %w", err)
		}
		if lead.PlatformId != "" {
			mongoIDs[lead.PlatformId] = true
			mongoLeadsData[lead.PlatformId] = lead
		}
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("error iterating MongoDB cursor: %w", err)
	}

	idsToDelete := []string{}
	for mongoID := range mongoIDs {
		if !mysqlIDs[mongoID] {
			idsToDelete = append(idsToDelete, mongoID)
		}
	}

	if len(idsToDelete) > 0 {
		deleteFilter := bson.D{{Key: "old_id", Value: bson.D{{Key: "$in", Value: idsToDelete}}}}
		_, err := leadsCollection.DeleteMany(ctx, deleteFilter)
		if err != nil {
			return fmt.Errorf("failed to delete non-existing leads from MongoDB: %w", err)
		}
	}

	recordsToUpsert := make([]string, 0)

	for id, mysqlLead := range allLeadsMap {
		mongoLead, exists := mongoLeadsData[id]

		if !exists {
			recordsToUpsert = append(recordsToUpsert, id)
			continue
		}

		var mysqlName, mysqlPhone string
		if mysqlLead.Name != nil {
			mysqlName = *mysqlLead.Name
		}
		if mysqlLead.Phone != nil {
			mysqlPhone = *mysqlLead.Phone
		}

		if mysqlName != mongoLead.Name ||
			mysqlPhone != mongoLead.Phone ||
			!mysqlLead.UpdatedAt.Equal(mongoLead.UpdatedAt) {
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
		lead := allLeadsMap[id]
		var name, phone string
		if lead.Name != nil {
			name = *lead.Name
		}
		if lead.Phone != nil {
			phone = *lead.Phone
		}

		mongoLead := MongoDBLeads{
			PlatformId: lead.ID,
			Name:       name,
			Phone:      phone,
			Source:     "Octa",
			CreatedAt:  lead.CreatedAt,
			UpdatedAt:  lead.UpdatedAt,
		}

		filter := bson.D{{Key: "platform_id", Value: mongoLead.PlatformId}}
		update := bson.D{{Key: "$set", Value: mongoLead}}

		upsertModel := mongo.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(update).
			SetUpsert(true)

		bulkOperations = append(bulkOperations, upsertModel)
		processedCount++

		if len(bulkOperations) >= batchSize {
			_, err := leadsCollection.BulkWrite(ctx, bulkOperations)
			if err != nil {
				return fmt.Errorf("failed to execute bulk write: %w", err)
			}
			bulkWriteCount++
			bulkOperations = []mongo.WriteModel{}
		}
	}

	if len(bulkOperations) > 0 {
		_, err := leadsCollection.BulkWrite(ctx, bulkOperations)
		if err != nil {
			return fmt.Errorf("failed to execute final bulk write: %w", err)
		}
		bulkWriteCount++
	}

	return nil
}

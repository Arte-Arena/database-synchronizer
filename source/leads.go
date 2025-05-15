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
	UniqueID       string          `json:"unique_id,omitempty" bson:"unique_id,omitempty"`
	RelatedBudgets []bson.ObjectID `json:"related_budgets,omitempty" bson:"related_budgets,omitempty"`
	RelatedOrders  []bson.ObjectID `json:"related_orders,omitempty" bson:"related_orders,omitempty"`
	RelatedClient  bson.ObjectID   `json:"related_client,omitempty" bson:"related_client,omitempty"`
	Rating         string          `json:"classification,omitempty" bson:"classification,omitempty"`
	Notes          string          `json:"notes,omitempty" bson:"notes,omitempty"`
	Responsible    bson.ObjectID   `json:"responsible,omitempty" bson:"responsible,omitempty"`
}

type MySQLLeads struct {
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

	tempCollName := database.COLLECTION_LEADS + "_temp"
	tempCollection := mongoClient.Database(database.GetDB()).Collection(tempCollName)

	if _, err := tempCollection.DeleteMany(ctx, bson.D{}); err != nil {
		return fmt.Errorf("failed to clear temporary collection: %w", err)
	}

	rows, err := mysqlDB.Query("SELECT nome, email, telefone, created_at, updated_at FROM octa_webhook")
	if err != nil {
		return fmt.Errorf("failed to query MySQL octa_webhook: %w", err)
	}
	defer rows.Close()

	bulkOperations := []mongo.WriteModel{}

	for rows.Next() {
		lead := MySQLLeads{}

		var createdAtStr, updatedAtStr []byte

		err := rows.Scan(
			&lead.Name,
			&lead.Email,
			&lead.Phone,
			&createdAtStr,
			&updatedAtStr,
		)
		if err != nil {
			return fmt.Errorf("failed to scan MySQL octa_webhook: %w", err)
		}

		lead.CreatedAt, err = time.Parse("2006-01-02 15:04:05", string(createdAtStr))
		if err != nil {
			return fmt.Errorf("failed to parse created_at datetime: %w", err)
		}

		lead.UpdatedAt, err = time.Parse("2006-01-02 15:04:05", string(updatedAtStr))
		if err != nil {
			return fmt.Errorf("failed to parse updated_at datetime: %w", err)
		}

		var name, phone string
		if lead.Name != nil {
			name = *lead.Name
		}
		if lead.Phone != nil {
			phone = *lead.Phone
		}

		mongoLead := MongoDBLeads{
			Name:      name,
			Phone:     phone,
			Source:    "Octa",
			CreatedAt: lead.CreatedAt,
			UpdatedAt: lead.UpdatedAt,
		}

		insertModel := mongo.NewInsertOneModel().SetDocument(mongoLead)
		bulkOperations = append(bulkOperations, insertModel)

		if len(bulkOperations) >= 500 {
			if _, err := tempCollection.BulkWrite(ctx, bulkOperations); err != nil {
				return fmt.Errorf("failed to execute bulk write: %w", err)
			}
			bulkOperations = []mongo.WriteModel{}
		}
	}

	if len(bulkOperations) > 0 {
		if _, err := tempCollection.BulkWrite(ctx, bulkOperations); err != nil {
			return fmt.Errorf("failed to execute final bulk write: %w", err)
		}
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("error iterating MySQL rows: %w", err)
	}

	db := mongoClient.Database(database.GetDB())
	err = db.Collection(database.COLLECTION_LEADS).Drop(ctx)
	if err != nil {
		return fmt.Errorf("failed to drop original collection: %w", err)
	}

	renameCmd := bson.D{
		{Key: "renameCollection", Value: database.GetDB() + "." + tempCollName},
		{Key: "to", Value: database.GetDB() + "." + database.COLLECTION_LEADS},
	}
	err = mongoClient.Database("admin").RunCommand(ctx, renameCmd).Err()
	if err != nil {
		return fmt.Errorf("failed to rename temp collection: %w", err)
	}

	return nil
}

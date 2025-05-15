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
	ID        bson.ObjectID `json:"id,omitempty" bson:"_id,omitempty"`
	OldID     uint          `json:"old_id" bson:"old_id"`
	Name      string        `json:"name" bson:"name"`
	Email     string        `json:"email" bson:"email"`
	Role      []string      `json:"role" bson:"role"`
	CreatedAt time.Time     `json:"created_at" bson:"created_at"`
	UpdatedAt time.Time     `json:"updated_at" bson:"updated_at"`
}

type MySQLUsers struct {
	ID        uint64    `db:"id"`
	Name      string    `db:"name"`
	Email     string    `db:"email"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

type MySQLRoleUser struct {
	UserID uint64 `db:"user_id"`
	RoleID uint   `db:"role_id"`
}

func GetRoleString(roleID uint) string {
	switch roleID {
	case SUPER_ADMIN:
		return "super_admin"
	case IT:
		return "it"
	case ADMIN:
		return "admin"
	case LEADER:
		return "leader"
	case COLLABORATOR:
		return "collaborator"
	case DESIGNER:
		return "designer"
	case DESIGNER_COORDINATOR:
		return "designer_coordinator"
	case PRODUCTION:
		return "production"
	case COMMERCIAL:
		return "commercial"
	default:
		return "unknown"
	}
}

func SyncUsers() error {
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

	tempCollName := database.COLLECTION_USERS + "_temp"
	tempCollection := mongoClient.Database(database.GetDB()).Collection(tempCollName)

	if _, err := tempCollection.DeleteMany(ctx, bson.D{}); err != nil {
		return fmt.Errorf("failed to clear temporary collection: %w", err)
	}

	roleUserMap := make(map[uint64][]uint)
	roleUserRows, err := mysqlDB.Query("SELECT user_id, role_id FROM role_user")
	if err != nil {
		return fmt.Errorf("failed to query MySQL role_user table: %w", err)
	}
	defer roleUserRows.Close()

	for roleUserRows.Next() {
		var userID uint64
		var roleID uint
		if err := roleUserRows.Scan(&userID, &roleID); err != nil {
			return fmt.Errorf("failed to scan MySQL role_user row: %w", err)
		}
		roleUserMap[userID] = append(roleUserMap[userID], roleID)
	}

	if err = roleUserRows.Err(); err != nil {
		return fmt.Errorf("error iterating MySQL role_user rows: %w", err)
	}

	rows, err := mysqlDB.Query("SELECT id, name, email, created_at, updated_at FROM users")
	if err != nil {
		return fmt.Errorf("failed to query MySQL users: %w", err)
	}
	defer rows.Close()

	bulkOperations := []mongo.WriteModel{}

	for rows.Next() {
		user := MySQLUsers{}

		var createdAtStr, updatedAtStr []byte

		err := rows.Scan(
			&user.ID,
			&user.Name,
			&user.Email,
			&createdAtStr,
			&updatedAtStr,
		)
		if err != nil {
			return fmt.Errorf("failed to scan MySQL user: %w", err)
		}

		user.CreatedAt, err = time.Parse("2006-01-02 15:04:05", string(createdAtStr))
		if err != nil {
			return fmt.Errorf("failed to parse created_at datetime: %w", err)
		}

		user.UpdatedAt, err = time.Parse("2006-01-02 15:04:05", string(updatedAtStr))
		if err != nil {
			return fmt.Errorf("failed to parse updated_at datetime: %w", err)
		}

		roleIDs, exists := roleUserMap[user.ID]
		var userRoles []string
		if exists {
			for _, roleID := range roleIDs {
				userRoles = append(userRoles, GetRoleString(roleID))
			}
		}

		if len(userRoles) == 0 {
			userRoles = append(userRoles, GetRoleString(COLLABORATOR))
		}

		mongoUser := MongoDBUsers{
			OldID:     uint(user.ID),
			Name:      user.Name,
			Email:     user.Email,
			Role:      userRoles,
			CreatedAt: user.CreatedAt,
			UpdatedAt: user.UpdatedAt,
		}

		filter := bson.D{{Key: "old_id", Value: mongoUser.OldID}}
		update := bson.D{{Key: "$set", Value: mongoUser}}

		upsertModel := mongo.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(update).
			SetUpsert(true)

		bulkOperations = append(bulkOperations, upsertModel)

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
	err = db.Collection(database.COLLECTION_USERS).Drop(ctx)
	if err != nil {
		return fmt.Errorf("failed to drop original collection: %w", err)
	}

	renameCmd := bson.D{
		{Key: "renameCollection", Value: database.GetDB() + "." + tempCollName},
		{Key: "to", Value: database.GetDB() + "." + database.COLLECTION_USERS},
	}
	err = mongoClient.Database("admin").RunCommand(ctx, renameCmd).Err()
	if err != nil {
		return fmt.Errorf("failed to rename temp collection: %w", err)
	}

	return nil
}

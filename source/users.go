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
	OldID     uint64        `json:"old_id" bson:"old_id"`
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

	usersCollection := mongoClient.Database(database.GetDB()).Collection(database.COLLECTION_USERS)

	allUsersMap := make(map[uint64]*MySQLUsers)

	dataRows, err := mysqlDB.Query("SELECT id, name, email, created_at, updated_at FROM users WHERE id IS NOT NULL")
	if err != nil {
		return fmt.Errorf("failed to query MySQL users data: %w", err)
	}

	for dataRows.Next() {
		user := &MySQLUsers{}
		var createdAtStr, updatedAtStr []byte
		var id sql.NullInt64

		err := dataRows.Scan(
			&id,
			&user.Name,
			&user.Email,
			&createdAtStr,
			&updatedAtStr,
		)
		if err != nil {
			dataRows.Close()
			return fmt.Errorf("failed to scan MySQL user data: %w", err)
		}

		if !id.Valid || id.Int64 <= 0 {
			continue
		}

		user.ID = uint64(id.Int64)
		user.CreatedAt, err = time.Parse("2006-01-02 15:04:05", string(createdAtStr))
		if err != nil {
			dataRows.Close()
			return fmt.Errorf("failed to parse created_at datetime: %w", err)
		}

		user.UpdatedAt, err = time.Parse("2006-01-02 15:04:05", string(updatedAtStr))
		if err != nil {
			dataRows.Close()
			return fmt.Errorf("failed to parse updated_at datetime: %w", err)
		}

		allUsersMap[user.ID] = user
	}
	dataRows.Close()

	if err = dataRows.Err(); err != nil {
		return fmt.Errorf("error iterating MySQL data rows: %w", err)
	}

	if len(allUsersMap) == 0 {
		return nil
	}

	mysqlIDs := make(map[uint64]bool, len(allUsersMap))
	for id := range allUsersMap {
		mysqlIDs[id] = true
	}

	roleUserMap := make(map[uint64][]uint)
	roleUserRows, err := mysqlDB.Query("SELECT user_id, role_id FROM role_user WHERE user_id IS NOT NULL")
	if err != nil {
		return fmt.Errorf("failed to query MySQL role_user table: %w", err)
	}

	for roleUserRows.Next() {
		var userID sql.NullInt64
		var roleID uint
		if err := roleUserRows.Scan(&userID, &roleID); err != nil {
			roleUserRows.Close()
			return fmt.Errorf("failed to scan MySQL role_user row: %w", err)
		}
		if userID.Valid && userID.Int64 > 0 {
			roleUserMap[uint64(userID.Int64)] = append(roleUserMap[uint64(userID.Int64)], roleID)
		}
	}
	roleUserRows.Close()

	if err = roleUserRows.Err(); err != nil {
		return fmt.Errorf("error iterating MySQL role_user rows: %w", err)
	}

	mongoIDs := make(map[uint64]bool)
	mongoUsersData := make(map[uint64]MongoDBUsers)

	cursor, err := usersCollection.Find(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("failed to query MongoDB users: %w", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var user MongoDBUsers
		if err := cursor.Decode(&user); err != nil {
			return fmt.Errorf("failed to decode MongoDB user: %w", err)
		}
		if user.OldID > 0 {
			mongoIDs[user.OldID] = true
			mongoUsersData[user.OldID] = user
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
		_, err := usersCollection.DeleteMany(ctx, deleteFilter)
		if err != nil {
			return fmt.Errorf("failed to delete non-existing users from MongoDB: %w", err)
		}
	}

	idsToUpsert := []uint64{}

	for id, mysqlUser := range allUsersMap {
		mongoUser, exists := mongoUsersData[id]

		roles := roleUserMap[mysqlUser.ID]
		userRoles := []string{}

		if len(roles) > 0 {
			for _, roleID := range roles {
				userRoles = append(userRoles, GetRoleString(roleID))
			}
		} else {
			userRoles = append(userRoles, GetRoleString(COLLABORATOR))
		}

		if !exists {
			idsToUpsert = append(idsToUpsert, id)
			continue
		}

		rolesChanged := false
		if len(userRoles) != len(mongoUser.Role) {
			rolesChanged = true
		} else {
			roleMap := make(map[string]bool)
			for _, role := range mongoUser.Role {
				roleMap[role] = true
			}

			for _, role := range userRoles {
				if !roleMap[role] {
					rolesChanged = true
					break
				}
			}
		}

		if mysqlUser.Name != mongoUser.Name ||
			mysqlUser.Email != mongoUser.Email ||
			!mysqlUser.UpdatedAt.Equal(mongoUser.UpdatedAt) ||
			rolesChanged {
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
		user := allUsersMap[id]

		roles := roleUserMap[user.ID]
		userRoles := []string{}

		if len(roles) > 0 {
			for _, roleID := range roles {
				userRoles = append(userRoles, GetRoleString(roleID))
			}
		} else {
			userRoles = append(userRoles, GetRoleString(COLLABORATOR))
		}

		mongoUser := MongoDBUsers{
			OldID:     user.ID,
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

		if len(bulkOperations) >= batchSize {
			if _, err := usersCollection.BulkWrite(ctx, bulkOperations); err != nil {
				return fmt.Errorf("failed to execute bulk write: %w", err)
			}
			bulkOperations = []mongo.WriteModel{}
		}
	}

	if len(bulkOperations) > 0 {
		if _, err := usersCollection.BulkWrite(ctx, bulkOperations); err != nil {
			return fmt.Errorf("failed to execute final bulk write: %w", err)
		}
	}

	return nil
}

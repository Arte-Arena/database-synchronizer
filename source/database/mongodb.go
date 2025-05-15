package database

import (
	"database_sync/source/utils"
	"os"
	"time"
)

func GetDB() string {
	environment := os.Getenv(utils.ENV)

	if environment == utils.ENV_RELEASE {
		return utils.ENV_RELEASE
	}

	if environment == utils.ENV_HOMOLOG {
		return utils.ENV_HOMOLOG
	}

	if environment == utils.ENV_DEVELOPMENT {
		return utils.ENV_DEVELOPMENT
	}

	panic("[MongoDB] Invalid DB name")
}

const (
	MONGODB_TIMEOUT = 20 * time.Second
)

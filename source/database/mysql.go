package database

import "time"

const (
	MYSQL_CONN_MAX_LIFETIME = 5 * time.Minute
	MYSQL_MAX_OPEN_CONNS    = 10
	MYSQL_MAX_IDLE_CONNS    = 10
)

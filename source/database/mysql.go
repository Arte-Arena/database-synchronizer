package database

import "time"

const (
	MYSQL_CONN_MAX_LIFETIME = 20 * time.Second
	MYSQL_MAX_OPEN_CONNS    = 10
	MYSQL_MAX_IDLE_CONNS    = 10
)

package db

import (
	_ "github.com/go-sql-driver/mysql" // blank import for mysql driver
	"github.com/jmoiron/sqlx"
)

// NewConnector creates new db instance with given db
func NewConnector(driverName string, connString string) (*sqlx.DB, error) {
	return sqlx.Connect(driverName, connString)
}

package dads

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx" // User MySQL driver
)

// ConnectAffiliationsDB - connect to affilaitions DB
func ConnectAffiliationsDB(ctx *Ctx) {
	if !ctx.AffsDBConfigured() {
		Fatalf("requested connection to affiliations DB while connection parameters are not set")
	}
	connStr := ctx.DBConn
	if connStr == "" {
		if ctx.DBName == "" {
			Fatalf("requested connection to affiliations DB while DB name was not specified")
		}
		if ctx.DBUser == "" {
			Fatalf("requested connection to affiliations DB while DB user was not specified")
		}
		hostPort := ctx.DBHost
		if hostPort == "" {
			hostPort = "127.0.0.1"
		}
		if ctx.DBPort != "" {
			hostPort += ":" + ctx.DBPort
		}
		userPass := ctx.DBUser
		if ctx.DBPass != "" {
			userPass += ":" + ctx.DBPass
		}
		opts := ctx.DBOpts
		if opts == "" {
			opts = "charset=utf8&parseTime=true"
		}
		// user:pwd@tcp(127.0.0.1:3306)/db?charset=utf8&parseTime=true
		connStr = fmt.Sprintf("%s@tcp(%s)/%s?%s", userPass, hostPort, ctx.DBName, opts)
	}
	if ctx.Debug > 0 {
		Printf("affiliations DB connect string: %s\n", connStr)
	}
	d, err := sqlx.Connect("mysql", connStr)
	FatalOnError(err)
	ctx.DB = d
}

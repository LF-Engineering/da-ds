package dads

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"

	_ "github.com/go-sql-driver/mysql" // User MySQL driver
	"github.com/jmoiron/sqlx"
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
	d.SetMaxOpenConns(3)
	ctx.DB = d
	FatalOnError(SetDBSessionOrigin(ctx))
}

// SetDBSessionOrigin - Set Session DB variable @origin to 'dads' so we will know which tool performed the DB operation
func SetDBSessionOrigin(ctx *Ctx) (err error) {
	_, err = ExecSQL(ctx, nil, "set @origin = ?", DADSOrigin)
	return err
}

// QueryOut - display DB query
func QueryOut(ctx *Ctx, in bool, err error, query string, args ...interface{}) {
	pref := "<<< "
	if in {
		pref = ">>> "
	}
	q := pref + query + "\n"
	if (err != nil || ctx.DebugSQL > 1) && len(args) > 0 {
		s := ""
		for vi, vv := range args {
			switch v := vv.(type) {
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, complex64, complex128, string, bool, time.Time:
				s += fmt.Sprintf("%d:%+v ", vi+1, v)
			case *int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64, *float32, *float64, *complex64, *complex128, *string, *bool, *time.Time:
				s += fmt.Sprintf("%d:%+v ", vi+1, v)
			case nil:
				s += fmt.Sprintf("%d:(null) ", vi+1)
			default:
				s += fmt.Sprintf("%d:%+v ", vi+1, reflect.ValueOf(vv))
			}
		}
		q += "[" + s + "]\n"
	}
	if err != nil || ctx.DebugSQL > 0 {
		Printf("%s", q)
		if err != nil {
			Printf("Error: %+v\n", err)
		}
	}
}

// ExecDB - execute DB query without transaction
func ExecDB(ctx *Ctx, query string, args ...interface{}) (res sql.Result, err error) {
	if err != nil || ctx.DebugSQL > 0 {
		QueryOut(ctx, true, err, query, args...)
	}
	res, err = ctx.DB.Exec(query, args...)
	if err != nil || ctx.DebugSQL > 0 {
		QueryOut(ctx, false, err, query, args...)
	}
	return
}

// ExecTX - execute DB query with transaction
func ExecTX(ctx *Ctx, tx *sql.Tx, query string, args ...interface{}) (res sql.Result, err error) {
	if err != nil || ctx.DebugSQL > 0 {
		QueryOut(ctx, true, err, query, args...)
	}
	res, err = tx.Exec(query, args...)
	if err != nil || ctx.DebugSQL > 0 {
		QueryOut(ctx, false, err, query, args...)
	}
	return
}

// ExecSQL - execute db query with transaction if provided
func ExecSQL(ctx *Ctx, tx *sql.Tx, query string, args ...interface{}) (sql.Result, error) {
	if tx == nil {
		return ExecDB(ctx, query, args...)
	}
	return ExecTX(ctx, tx, query, args...)
}

// QueryDB - query database without transaction
func QueryDB(ctx *Ctx, query string, args ...interface{}) (rows *sql.Rows, err error) {
	if err != nil || ctx.DebugSQL > 0 {
		QueryOut(ctx, true, err, query, args...)
	}
	rows, err = ctx.DB.Query(query, args...)
	if err != nil || ctx.DebugSQL > 0 {
		QueryOut(ctx, false, err, query, args...)
	}
	return
}

// QueryTX - query database with transaction
func QueryTX(ctx *Ctx, tx *sql.Tx, query string, args ...interface{}) (rows *sql.Rows, err error) {
	if err != nil || ctx.DebugSQL > 0 {
		QueryOut(ctx, true, err, query, args...)
	}
	rows, err = tx.Query(query, args...)
	if err != nil || ctx.DebugSQL > 0 {
		QueryOut(ctx, false, err, query, args...)
	}
	return
}

// QuerySQL - query DB using transaction if provided
func QuerySQL(ctx *Ctx, tx *sql.Tx, query string, args ...interface{}) (*sql.Rows, error) {
	if tx == nil {
		return QueryDB(ctx, query, args...)
	}
	return QueryTX(ctx, tx, query, args...)
}

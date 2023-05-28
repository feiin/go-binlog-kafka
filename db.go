package main

import (
	"github.com/feiin/ploto"
	_ "github.com/go-sql-driver/mysql"
)

var db *ploto.Dialect

func initManageDb(dbHost string, dbPort int, dbUser, dbPass string, dbDatabase string, srcHost, srcUser, srcPass string, srcPort int) (err error) {

	config := ploto.DialectConfig{

		Clients: map[string]interface{}{
			"binlog_center": map[string]interface{}{
				"host":     dbHost,
				"port":     float64(dbPort),
				"user":     dbUser,
				"password": dbPass,
				"database": dbDatabase,
			},
			"sync_src_db": map[string]interface{}{
				"host":     srcHost,
				"port":     float64(srcPort),
				"user":     srcUser,
				"password": srcPass,
				"database": "information_schema",
			},
		},
		Default: map[string]interface{}{
			"port":    float64(3306),
			"dialect": "mysql",
			"pool": map[string]interface{}{
				"maxIdleConns": float64(2),
				"maxLeftTime":  float64(60000),
				"maxOpenConns": float64(5),
			},
			"dialectOptions": map[string]interface{}{
				"parseTime":       true,
				"multiStatements": true,
				"writeTimeout":    "3000ms",
				"readTimeout":     "3000ms",
				"timeout":         "3000ms",
				"loc":             "Local",
			},
		},
	}

	db, err = ploto.Open(config, &ploto.DefaultLogger{})
	return err

}

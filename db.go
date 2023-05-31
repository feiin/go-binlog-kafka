package main

import (
	"github.com/feiin/ploto"
	_ "github.com/go-sql-driver/mysql"
)

var metaDataMap = make(map[string]map[string][]string)

type MysqlTableMeta struct {
	TableSchema string `db:"table_schema" json:"table_schema"`
	TableName   string `db:"table_name" json:"table_name"`
	Column_name string `db:"column_name" json:"column_name"`
}

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

func getMysqlMeta(schema string, table string) ([]MysqlTableMeta, error) {
	var mysqlTableMeta []MysqlTableMeta
	err := db.Use("sync_src_db").Query("select table_schema,table_name,column_name from columns where table_schema=? and table_name=?", schema, table).Scan(&mysqlTableMeta)
	return mysqlTableMeta, err
}

func getMysqlTableColumns(schema string, table string) ([]string, error) {

	schemaMap, ok := metaDataMap[schema]
	if !ok {
		metaDataMap[schema] = make(map[string][]string)
	}

	_, ok = schemaMap[table]
	if !ok {
		tableColumns, err := getMysqlMeta(schema, table)
		if err != nil {
			return nil, err
		}
		for _, v := range tableColumns {
			metaDataMap[schema][table] = append(metaDataMap[schema][table], v.Column_name)
		}
	}

	var columns []string
	for _, v := range metaDataMap[schema][table] {
		columns = append(columns, v)
	}
	return columns, nil
}

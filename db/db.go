package db

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

func InitManageDb(dbHost string, dbPort int, dbUser, dbPass string, dbDatabase string, srcHost, srcUser, srcPass string, srcPort int, metaStoreType string, srcGTID string) (err error) {

	binlogInfoStoreType = metaStoreType

	config := ploto.DialectConfig{

		Clients: map[string]interface{}{
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

	if binlogInfoStoreType != "file" {
		config.Clients["binlog_center"] = map[string]interface{}{
			"host":     dbHost,
			"port":     float64(dbPort),
			"user":     dbUser,
			"password": dbPass,
			"database": dbDatabase,
		}
	}

	db, err = ploto.Open(config, &ploto.DefaultLogger{})
	return err

}

func GetMysqlMeta(schema string, table string) ([]MysqlTableMeta, error) {
	var mysqlTableMeta []MysqlTableMeta
	err := db.Use("sync_src_db").Query("select table_schema as table_schema,table_name as table_name,column_name as column_name from columns where table_schema=? and table_name=? order by ORDINAL_POSITION asc", schema, table).Scan(&mysqlTableMeta)
	return mysqlTableMeta, err
}

func GetMysqlTableColumns(schema string, table string) (columns []string, metaFromMaster bool, err error) {

	schemaMap, ok := metaDataMap[schema]
	if !ok {
		metaDataMap[schema] = make(map[string][]string)
	}

	_, ok = schemaMap[table]
	if !ok {
		tableColumns, err := GetMysqlMeta(schema, table)
		if err != nil {
			return nil, false, err
		}
		metaFromMaster = true
		for _, v := range tableColumns {
			metaDataMap[schema][table] = append(metaDataMap[schema][table], v.Column_name)
		}
	}

	for _, v := range metaDataMap[schema][table] {
		columns = append(columns, v)
	}
	return columns, metaFromMaster, nil
}

func UpdateMysqlTableColumns(schema, table string) error {
	tableColumns, err := GetMysqlMeta(schema, table)
	if err != nil {
		return err
	}

	_, ok := metaDataMap[schema]
	if !ok {
		metaDataMap[schema] = make(map[string][]string)
	}

	metaDataMap[schema][table] = nil
	for _, v := range tableColumns {
		metaDataMap[schema][table] = append(metaDataMap[schema][table], v.Column_name)
	}
	return nil
}

func InitBinLogCenter(binlogCenter *BinLogCenterInfo) error {
	if binlogCenter.MetaData != nil {
		metaDataMap = binlogCenter.MetaData
	}
	return nil
}

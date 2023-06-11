package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/feiin/go-binlog-kafka/db"
	"github.com/feiin/go-binlog-kafka/logger"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tidb/parser"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

func main() {
	dbInstanceName := flag.String("db_instance_name", "", "Database instance name")
	kafkaTopicName := flag.String("kafka_topic_name", "", "Kafka topic name")
	kafkaAddr := flag.String("kafka_addr", "", "Kafka address")
	adminHost := flag.String("admin_host", "", "binlog meta manage db host")
	adminPort := flag.Int("admin_port", 3306, "binlog meta manage db port")
	adminUser := flag.String("admin_user", "root", "binlog meta manage db user")
	adminPass := flag.String("admin_pass", "", "binlog meta manage db password")
	adminDatabase := flag.String("admin_database", "binlog_center", "binlog meta manage db name")
	metaStoreType := flag.String("meta_store_type", "file", "sync source db type: file/mysql")
	srcDbUser := flag.String("src_db_user", "root", "sync source db user")
	srcDbPass := flag.String("src_db_pass", "", "sync source db password")
	srcDbHost := flag.String("src_db_host", "", "sync source db host")
	srcDbPort := flag.Int("src_db_port", 3306, "sync source db port")
	srcDbGtid := flag.String("src_db_gtid", "", "sync source db gtid")
	replicationId := flag.Int("replication_id", 212388888, "replication id")
	binlogTimeout := flag.Int64("binlog_timeout", 0, "binlog max read timeout")
	isDebug := flag.Bool("debug", false, "is debug mode")
	flag.Parse()

	kafkaAddress := strings.Split(*kafkaAddr, ",")

	err := InitKafka(kafkaAddress)
	if err != nil {
		logger.ErrorWith(context.Background(), err).Msg("InitKafka error")
		panic(err)
	}

	err = db.InitManageDb(*adminHost, *adminPort, *adminUser, *adminPass, *adminDatabase, *srcDbHost, *srcDbUser, *srcDbPass, *srcDbPort, *metaStoreType, *srcDbGtid)
	if err != nil {
		logger.ErrorWith(context.Background(), err).Msg("initManageDb error")
		panic(err)
	}

	binlogCenterPos, err := db.GetReplicationPos(context.Background(), *dbInstanceName)
	if err != nil {
		logger.ErrorWith(context.Background(), err).Msg("GetReplicationPos error")
		panic(err)
	}

	if binlogCenterPos != nil {
		*srcDbGtid = binlogCenterPos.BinlogGtid
		db.InitBinLogCenter(binlogCenterPos)
	}

	cfg := replication.BinlogSyncerConfig{
		ServerID: uint32(*replicationId),
		Flavor:   "mysql",
		Host:     *srcDbHost,
		Port:     uint16(*srcDbPort),
		User:     *srcDbUser,
		Password: *srcDbPass,
	}

	syncer := replication.NewBinlogSyncer(cfg)
	gtid, _ := mysql.ParseGTIDSet("mysql", *srcDbGtid)
	streamer, _ := syncer.StartSyncGTID(gtid)

	var rowData RowData

	var eventRowList []RowData

	ctx := context.Background()

	ddlParser := parser.New()

	var forceSavePos bool

	for {

		// batch_count 100
		if len(eventRowList) >= 100 {
			forceSavePos = true
		}

		if forceSavePos && len(eventRowList) > 0 {
			// 1. push to kafka
			pushJsonData, err := json.Marshal(eventRowList)
			if err != nil {
				logger.ErrorWith(ctx, err).Msg("json.Marshal error")
				panic(err)
			}
			if *isDebug {
				logger.Info(ctx).Interface("pushJsonData", string(pushJsonData)).Msg("push to kafka")
			}
			err = sendKafkaMsg(ctx, pushJsonData, *kafkaTopicName)
			if err != nil {
				logger.ErrorWith(ctx, err).Msg("sendKafkaMsg error")
				panic(err)
			}
			logger.Info(ctx).Interface("rowCount", len(eventRowList)).Msg("success to push to kafka")

			// 2. update position
			lastRow := eventRowList[len(eventRowList)-1]
			err = db.SaveReplicationPos(ctx, *dbInstanceName, lastRow.LogPos, lastRow.Gtid, lastRow.BinLogFile)
			if err != nil {
				logger.ErrorWith(ctx, err).Msg("SaveReplicationPos error")
				panic(err)
			}

			eventRowList = eventRowList[:0]
		}

		rowData.AfterValues = nil
		rowData.Values = nil
		rowData.BeforeValues = nil
		forceSavePos = false

		var ev *replication.BinlogEvent

		var err error
		if *binlogTimeout > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*binlogTimeout)*time.Millisecond)
			ev, err = streamer.GetEvent(ctx)
			cancel()
			if err == context.DeadlineExceeded {
				// logger.Info(ctx).Msg("GetEventTimeout timeout")
				forceSavePos = true
				continue
			}
			if err != nil {
				logger.ErrorWith(ctx, err).Msg("GetEventTimeout error")
				break
			}

		} else {
			ev, err = streamer.GetEvent(context.Background())
			if err != nil {
				logger.ErrorWith(ctx, err).Msg("GetEvent error")
				break
			}
		}

		event := ev.Header.EventType

		switch event {
		case replication.WRITE_ROWS_EVENTv2, replication.WRITE_ROWS_EVENTv1:
			rowsEvent := ev.Event.(*replication.RowsEvent)
			columns, updateMeta, err := db.GetMysqlTableColumns(rowData.Schema, rowData.Table)
			if err != nil {
				logger.ErrorWith(ctx, err).Msg("getMysqlTableColumns error")
				panic(err)
			}
			forceSavePos = updateMeta

			rowData.Action = "insert"
			for _, row := range rowsEvent.Rows {

				values := map[string]interface{}{}
				for i, column := range columns {

					if i+1 > len(row) {
						continue
					}

					if _, ok := row[i].([]byte); ok {
						values[column] = fmt.Sprintf("%s", row[i])
					} else {
						values[column] = row[i]
					}
				}
				rowData.Values = values
				eventRowList = append(eventRowList, rowData)

				rowData.Table = string(rowsEvent.Table.Table)
			}
		case replication.UPDATE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv1:
			rowsEvent := ev.Event.(*replication.RowsEvent)
			columns, updateMeta, err := db.GetMysqlTableColumns(rowData.Schema, rowData.Table)
			if err != nil {
				logger.ErrorWith(ctx, err).Msg("getMysqlTableColumns error")
				panic(err)
			}
			forceSavePos = updateMeta

			rowData.Action = "update"
			for j, row := range rowsEvent.Rows {

				beforeValues := map[string]interface{}{}
				afterValues := map[string]interface{}{}
				if j%2 == 0 {
					for i, column := range columns {

						if i+1 > len(row) {
							continue
						}

						if _, ok := row[i].([]byte); ok {
							beforeValues[column] = fmt.Sprintf("%s", row[i])
						} else {
							beforeValues[column] = row[i]
						}
					}
					rowData.BeforeValues = beforeValues

				} else {

					for i, column := range columns {
						if i+1 > len(row) {
							continue
						}

						if _, ok := row[i].([]byte); ok {
							afterValues[column] = fmt.Sprintf("%s", row[i])
						} else {
							afterValues[column] = row[i]
						}
					}
					rowData.AfterValues = afterValues
					eventRowList = append(eventRowList, rowData)
				}
			}

		case replication.DELETE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv1:
			rowsEvent := ev.Event.(*replication.RowsEvent)
			columns, updateMeta, err := db.GetMysqlTableColumns(rowData.Schema, rowData.Table)
			if err != nil {
				logger.ErrorWith(ctx, err).Msg("getMysqlTableColumns error")
				panic(err)
			}
			forceSavePos = updateMeta

			rowData.Action = "delete"
			if len(columns) == 0 {
				break
			}

			for _, row := range rowsEvent.Rows {

				values := map[string]interface{}{}
				for i, column := range columns {
					if i+1 > len(row) {
						continue
					}

					if _, ok := row[i].([]byte); ok {
						values[column] = fmt.Sprintf("%s", row[i])
					} else {
						values[column] = row[i]
					}
				}
				rowData.Values = values
				eventRowList = append(eventRowList, rowData)
			}

		case replication.QUERY_EVENT:
			rowData.Gtid = ev.Event.(*replication.QueryEvent).GSet.String()
			queryEvent := ev.Event.(*replication.QueryEvent)
			stmts, _, err := ddlParser.Parse(string(queryEvent.Query), "", "")
			if err != nil {
				logger.ErrorWith(ctx, err).Interface("queryEvent", queryEvent).Msg("Parse query error,will skip this query event")
				continue
			}

			var allDDLEvents []*db.DDLEvent
			for _, stmt := range stmts {
				ddlEvents := db.ParseDDLStmt(stmt)
				for _, ddlEvent := range ddlEvents {
					if ddlEvent.Schema == "" {
						ddlEvent.Schema = string(queryEvent.Schema)
					}
					allDDLEvents = append(allDDLEvents, ddlEvent)
					// update mysql table columns
					err := db.UpdateMysqlTableColumns(ddlEvent.Schema, ddlEvent.Table)
					if err != nil {
						logger.ErrorWith(ctx, err).Msg("updateMysqlTableColumns error")
						panic(err)
					}
				}
			}

			if len(allDDLEvents) > 0 {
				rowData.Action = "DDL"
				rowData.Values = map[string]interface{}{
					"ddl_events": allDDLEvents,
					"ddl_sql":    string(queryEvent.Query),
				}
				eventRowList = append(eventRowList, rowData)

				forceSavePos = true
			}

		case replication.GTID_EVENT:
			rowData.LogPos = ev.Header.LogPos

		case replication.TABLE_MAP_EVENT:
			rowData.Schema = string(ev.Event.(*replication.TableMapEvent).Schema)
			rowData.Table = string(ev.Event.(*replication.TableMapEvent).Table)

		case replication.ROTATE_EVENT:
			rowData.BinLogFile = string(ev.Event.(*replication.RotateEvent).NextLogName)
		default:
		}

	}

}

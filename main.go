package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/feiin/go-binlog-kafka/logger"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tidb/parser"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

func main() {
	// dbInstanceName := flag.String("db_instance_name", "", "Database instance name")
	// kafkaTopicName := flag.String("kafka_topic_name", "", "Kafka topic name")
	// kafkaAddrr := flag.String("kafka_addr", "", "Kafka address")
	adminHost := flag.String("admin_host", "", "admin manage db host")
	adminPort := flag.Int("admin_port", 3306, "admin manage db port")
	adminUser := flag.String("admin_user", "root", "admin manage db user")
	adminPass := flag.String("admin_pass", "", "admin manage db password")
	srcDbUser := flag.String("src_db_user", "root", "sync source db user")
	srcDbPass := flag.String("src_db_pass", "", "sync source db password")
	srcDbHost := flag.String("src_db_host", "", "sync source db host")
	srcDbPort := flag.Int("src_db_port", 3306, "sync source db port")
	replicationId := flag.Int("replication_id", 212388888, "replication id")
	binlogTimeout := flag.Int64("binlog_timeout", 0, "binlog max read timeout")
	flag.Parse()

	err := initManageDb(*adminHost, *adminPort, *adminUser, *adminPass, "binlog_center", *srcDbHost, *srcDbUser, *srcDbPass, *srcDbPort)
	if err != nil {
		fmt.Printf("initManageDb error:%v", err)
		return
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
	gtid, _ := mysql.ParseGTIDSet("mysql", "68414ab6-fd2a-11ed-9e2d-0242ac110002:1")
	streamer, _ := syncer.StartSyncGTID(gtid)

	var rowData RowData

	var eventRowList []RowData

	ctx := context.Background()

	ddlParser := parser.New()

	var forceSavePos bool

	for {

		if forceSavePos == true && len(eventRowList) > 0 {
			// push to kafka
			logger.Info(ctx).Interface("eventRowList", eventRowList).Msg("eventRowList")
			eventRowList = eventRowList[:0]
			logger.Info(ctx).Interface("eventRowList", eventRowList).Msg("eventRowList clear")
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
			columns, updateMeta, err := getMysqlTableColumns(rowData.Schema, rowData.Table)
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
			columns, updateMeta, err := getMysqlTableColumns(rowData.Schema, rowData.Table)
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
			columns, updateMeta, err := getMysqlTableColumns(rowData.Schema, rowData.Table)
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

			var allDDLEvents []*DDLEvent
			for _, stmt := range stmts {
				ddlEvents := parseDDLStmt(stmt)
				for _, ddlEvent := range ddlEvents {
					if ddlEvent.Schema == "" {
						ddlEvent.Schema = string(queryEvent.Schema)
					}
					allDDLEvents = append(allDDLEvents, ddlEvent)
					// update mysql table columns
					err := updateMysqlTableColumns(ddlEvent.Schema, ddlEvent.Table)
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

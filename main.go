package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
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

	for {

		var ev *replication.BinlogEvent
		var err error
		if *binlogTimeout > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*binlogTimeout)*time.Millisecond)
			ev, err = streamer.GetEvent(ctx)
			cancel()
			if err == context.DeadlineExceeded {
				fmt.Printf("GetEventTimeout timeout")
				continue
			}
			if err != nil {
				fmt.Printf("GetEvent error:%v", err)
				break
			}

		} else {
			ev, err = streamer.GetEvent(context.Background())
			if err != nil {
				fmt.Printf("GetEvent error:%v", err)
				break
			}
		}

		event := ev.Header.EventType

		switch event {
		case replication.WRITE_ROWS_EVENTv2, replication.WRITE_ROWS_EVENTv1:
			rowsEvent := ev.Event.(*replication.RowsEvent)
			for _, row := range rowsEvent.Rows {
				fmt.Printf("insert row %v\n", row)
			}
		case replication.UPDATE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv1:
			rowsEvent := ev.Event.(*replication.RowsEvent)
			for _, row := range rowsEvent.Rows {
				fmt.Printf("update row %v\n", row)
			}

		case replication.DELETE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv1:
			rowsEvent := ev.Event.(*replication.RowsEvent)
			for _, row := range rowsEvent.Rows {
				fmt.Printf("delete row %v\n", row)
			}

		case replication.QUERY_EVENT:
			queryEvent := ev.Event.(*replication.QueryEvent)
			fmt.Printf("query event %v\n", queryEvent)
		case replication.GTID_EVENT:
			gtidEvent := ev.Event.(*replication.GTIDEvent)
			fmt.Printf("gtid event %v\n", gtidEvent)

		case replication.TABLE_MAP_EVENT:
			tableMapEvent := ev.Event.(*replication.TableMapEvent)
			fmt.Printf("table map event %v\n", tableMapEvent)

		case replication.ROTATE_EVENT:
			rotateEvent := ev.Event.(*replication.RotateEvent)
			fmt.Printf("rotate event %v\n", rotateEvent)
		default:
			fmt.Printf("no action with event %v\n", event)
		}
	}

}

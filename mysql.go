package main

import (
	"github.com/pingcap/tidb/parser/ast"
)

type DDLEvent struct {
	Schema string
	Table  string
}

func parseDDLStmt(stmt ast.StmtNode) (es []*DDLEvent) {
	switch t := stmt.(type) {
	case *ast.RenameTableStmt:
		for _, tableInfo := range t.TableToTables {
			e := &DDLEvent{
				Schema: tableInfo.OldTable.Schema.String(),
				Table:  tableInfo.OldTable.Name.String(),
			}
			es = append(es, e)
		}
	case *ast.AlterTableStmt:
		e := &DDLEvent{
			Schema: t.Table.Schema.String(),
			Table:  t.Table.Name.String(),
		}
		es = []*DDLEvent{e}
	case *ast.DropTableStmt:
		for _, table := range t.Tables {
			e := &DDLEvent{
				Schema: table.Schema.String(),
				Table:  table.Name.String(),
			}
			es = append(es, e)
		}
	case *ast.CreateTableStmt:
		e := &DDLEvent{
			Schema: t.Table.Schema.String(),
			Table:  t.Table.Name.String(),
		}
		es = []*DDLEvent{e}
	}
	return es
}

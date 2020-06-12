package flexmy

import (
	"database/sql"
	"fmt"
	"strings"

	"git.kanosolution.net/kano/dbflex"
	"git.kanosolution.net/kano/dbflex/drivers/rdbms"
	"github.com/eaciit/toolkit"
)

// Query implementaion of dbflex.IQuery
type Query struct {
	rdbms.Query
	db         *sql.DB
	tx         *sql.Tx
	sqlcommand string
}

// Cursor produces a cursor from query
func (q *Query) Cursor(in toolkit.M) dbflex.ICursor {
	cursor := new(Cursor)
	cursor.SetThis(cursor)

	ct := q.Config(dbflex.ConfigKeyCommandType, dbflex.QuerySelect).(string)
	if ct != dbflex.QuerySelect && ct != dbflex.QuerySQL {
		cursor.SetError(toolkit.Errorf("cursor is used for only select command"))
		return cursor
	}

	cmdtxt := q.Config(dbflex.ConfigKeyCommand, "").(string)
	if cmdtxt == "" {
		cursor.SetError(toolkit.Errorf("no command"))
		return cursor
	}

	tablename := q.Config(dbflex.ConfigKeyTableName, "").(string)
	cq := dbflex.From(tablename).Select("count(*) as Count")
	if filter := q.Config(dbflex.ConfigKeyFilter, nil); filter != nil {
		cq.Where(filter.(*dbflex.Filter))
	}
	cursor.SetCountCommand(cq)

	var rows *sql.Rows
	var err error

	if q.tx == nil {
		rows, err = q.db.Query(cmdtxt)
	} else {
		rows, err = q.tx.Query(cmdtxt)
	}
	if rows == nil {
		cursor.SetError(toolkit.Errorf("%s. SQL Command: %s", err.Error(), cmdtxt))
	} else {
		cursor.SetFetcher(rows)
	}
	return cursor
}

// Execute will executes non-select command of a query
func (q *Query) Execute(in toolkit.M) (interface{}, error) {
	cmdtype, ok := q.Config(dbflex.ConfigKeyCommandType, dbflex.QuerySelect).(string)
	if !ok {
		return nil, toolkit.Errorf("Operation is unknown. current operation is %s", cmdtype)
	}
	cmdtxt := q.Config(dbflex.ConfigKeyCommand, "").(string)
	if cmdtxt == "" && cmdtype != dbflex.QuerySave {
		return nil, toolkit.Errorf("No command")
	}

	var (
		sqlfieldnames []string
		sqlvalues     []string
	)

	data, hasData := in["data"]
	if !hasData && !(cmdtype == dbflex.QueryDelete || cmdtype == dbflex.QuerySelect) {
		return nil, toolkit.Error("non select and delete command should has data")
	}

	if hasData {
		sqlfieldnames, _, _, sqlvalues = rdbms.ParseSQLMetadata(q, data)
		affectedfields := q.Config("fields", []string{}).([]string)
		if len(affectedfields) > 0 {
			newfieldnames := []string{}
			newvalues := []string{}
			for idx, field := range sqlfieldnames {
				for _, find := range affectedfields {
					if strings.ToLower(field) == strings.ToLower(find) {
						newfieldnames = append(newfieldnames, find)
						newvalues = append(newvalues, sqlvalues[idx])
					}
				}
			}
			sqlfieldnames = newfieldnames
			sqlvalues = newvalues
		}
	}

	switch cmdtype {
	case dbflex.QuerySave:
		tableName := q.Config(dbflex.ConfigKeyTableName, "").(string)
		filter := q.Config(dbflex.ConfigKeyFilter, nil)
		if filter == nil {
			return nil, fmt.Errorf("save operations should have filter")
		}

		cmdGets := dbflex.From(tableName).Where(filter.(*dbflex.Filter)).Select()
		cursor := q.Connection().Cursor(cmdGets, nil)
		if err := cursor.Error(); err != nil {
			return nil, fmt.Errorf("unable to get data for checking. %s", err.Error())
		}

		//fmt.Println("Filter:", toolkit.JsonString(filter))
		var saveCmd dbflex.ICommand
		if cursor.Count() == 0 {
			saveCmd = dbflex.From(tableName).Where(filter.(*dbflex.Filter)).Insert()
		} else {
			saveCmd = dbflex.From(tableName).Where(filter.(*dbflex.Filter)).Update()
		}
		cursor.Close()

		return q.Connection().Execute(saveCmd, in)

	case dbflex.QueryInsert:
		cmdtxt = strings.Replace(cmdtxt, "{{.FIELDS}}", strings.Join(sqlfieldnames, ","), -1)
		cmdtxt = strings.Replace(cmdtxt, "{{.VALUES}}", strings.Join(sqlvalues, ","), -1)
		//toolkit.Printfn("\nCmd: %s", cmdtxt)

	case dbflex.QueryUpdate:
		//fmt.Println("fieldnames:", sqlfieldnames)
		updatedfields := []string{}
		for idx, fieldname := range sqlfieldnames {
			updatedfields = append(updatedfields, fieldname+"="+sqlvalues[idx])
		}
		cmdtxt = strings.Replace(cmdtxt, "{{.FIELDVALUES}}", strings.Join(updatedfields, ","), -1)
	}

	//fmt.Println("Cmd: ", cmdtxt)
	var r sql.Result
	var err error
	if q.tx == nil {
		r, err = q.db.Exec(cmdtxt)
	} else {
		r, err = q.tx.Exec(cmdtxt)
	}

	if err != nil {
		return nil, toolkit.Errorf("%s. SQL Command: %s", err.Error(), cmdtxt)
	}
	return r, nil
}

// ExecType to identify type of exec
type ExecType int

const (
	ExecQuery ExecType = iota
	ExecNonQuery
	ExecQueryRow
)

/*
func (q *Query) SQL(string cmd, exec) dbflex.IQuery {
	swicth()
}
*/

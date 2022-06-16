package flexmy

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"git.kanosolution.net/kano/dbflex"
	"git.kanosolution.net/kano/dbflex/drivers/rdbms"
	"github.com/sebarcode/codekit"
)

// Query implementaion of dbflex.IQuery
type Query struct {
	rdbms.Query
	db         *sql.DB
	tx         *sql.Tx
	sqlcommand string
}

// Cursor produces a cursor from query
func (q *Query) Cursor(in codekit.M) dbflex.ICursor {
	cursor := new(Cursor)
	cursor.SetThis(cursor)

	ct := q.Config(dbflex.ConfigKeyCommandType, dbflex.QuerySelect).(string)
	if ct != dbflex.QuerySelect && ct != dbflex.QuerySQL {
		cursor.SetError(fmt.Errorf("cursor is used for only select command"))
		return cursor
	}

	cmdtxt := q.Config(dbflex.ConfigKeyCommand, "").(string)
	if cmdtxt == "" {
		cursor.SetError(fmt.Errorf("no command"))
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
		cursor.SetError(fmt.Errorf("%s. SQL Command: %s", err.Error(), cmdtxt))
	} else {
		cursor.SetFetcher(rows)
	}
	return cursor
}

// Execute will executes non-select command of a query
func (q *Query) Execute(in codekit.M) (interface{}, error) {
	cmdtype, ok := q.Config(dbflex.ConfigKeyCommandType, dbflex.QuerySelect).(string)
	if !ok {
		return nil, fmt.Errorf("Operation is unknown. current operation is %s", cmdtype)
	}
	cmdtxt := q.Config(dbflex.ConfigKeyCommand, "").(string)
	if cmdtxt == "" && cmdtype != dbflex.QuerySave {
		return nil, fmt.Errorf("No command")
	}

	var (
		sqlfieldnames []string
		sqlvalues     []string
	)

	data, hasData := in["data"]
	if !hasData && !(cmdtype == dbflex.QueryDelete || cmdtype == dbflex.QuerySelect) {
		return nil, errors.New("non select and delete command should has data")
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

		//fmt.Println("Filter:", codekit.JsonString(filter))
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
		//fmt.Printfn("\nCmd: %s", cmdtxt)

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
		return nil, fmt.Errorf("%s. SQL Command: %s", err.Error(), cmdtxt)
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

func (q *Query) ValueToSQlValue(v interface{}) string {
	switch v.(type) {
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%f", v)
	case time.Time:
		return codekit.Date2String(v.(time.Time), "'yyyy-MM-dd HH:mm:ss'")
	case *time.Time:
		dt := v.(*time.Time)
		return codekit.Date2String(*dt, "'yyyy-MM-dd HH:mm:ss'")
	case bool:
		if v.(bool) {
			return "true"
		}
		return "false"
	case string:
		return fmt.Sprintf("'%s'", v.(string))
	default:
		return fmt.Sprintf("'%s'", CleanupSQL(fmt.Sprintf("%v", codekit.JsonString(v))))
	}
}

func CleanupSQL(s string) string {
	return strings.Replace(s, "'", "''", -1)
}

package flexmy

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"git.kanosolution.net/kano/dbflex"
	"github.com/eaciit/toolkit"

	"git.kanosolution.net/kano/dbflex/drivers/rdbms"
	_ "github.com/go-sql-driver/mysql"
)

// Connection implementation of dbflex.IConnection
type Connection struct {
	rdbms.Connection
	db *sql.DB
	tx *sql.Tx
}

func init() {
	dbflex.RegisterDriver("mysql", func(si *dbflex.ServerInfo) dbflex.IConnection {
		c := new(Connection)
		c.SetThis(c)
		c.ServerInfo = *si
		return c
	})
}

// Connect to database instance
func (c *Connection) Connect() error {
	sqlconnstring := toolkit.Sprintf("tcp(%s)/%s", c.Host, c.Database)
	if c.User != "" {
		sqlconnstring = toolkit.Sprintf("%s:%s@%s", c.User, c.Password, sqlconnstring)
	}
	configs := strings.Join(func() []string {
		var out []string
		for k, v := range c.Config {
			out = append(out, toolkit.Sprintf("%s=%s", k, v))
		}
		return out
	}(), "&")
	if configs != "" {
		sqlconnstring = sqlconnstring + "?" + configs
	}
	db, err := sql.Open("mysql", sqlconnstring)
	c.db = db
	return err
}

func (c *Connection) State() string {
	if c.db != nil {
		return dbflex.StateConnected
	}
	return dbflex.StateUnknown
}

// Close database connection
func (c *Connection) Close() {
	if c.db != nil {
		c.db.Close()
	}
}

// NewQuery generates new query object to perform query action
func (c *Connection) NewQuery() dbflex.IQuery {
	q := new(Query)
	q.SetThis(q)
	q.db = c.db
	q.tx = c.tx
	return q
}

// DropTable - delete table
func (c *Connection) DropTable(name string) error {
	_, err := c.db.Exec("drop table if exists " + name)
	return err
}

// EnsureTable ensure existence and structures of the table
func (c *Connection) EnsureTable(name string, keys []string, obj interface{}) error {
	cmd := fmt.Sprintf("select table_name from information_schema.TABLES t where table_type='BASE TABLE' and table_name='%s'", strings.ToLower(name))
	rs, err := c.db.Query(cmd)
	if err != nil {
		return fmt.Errorf("unable to check table existence. %s", err.Error())
	}
	defer rs.Close()

	tableExists := false
	for rs.Next() {
		tbname := ""
		rs.Scan(&tbname)
		tableExists = strings.ToLower(tbname) == strings.ToLower(name)
	}

	if !tableExists {
		cmd := createCommandForCreate(name, keys, obj)
		_, err = c.db.Exec(cmd)
		if err != nil {
			return fmt.Errorf("unable to created table %s. %s", name, err.Error())
		}
	} else {
		sql := createCommandForUpdate(name, keys, obj, c)
		if sql != "" {
			_, err = c.db.Exec(sql)
			if err != nil {
				return fmt.Errorf("unable to alter table %s. %s", name, err.Error())
			}
		}
	}
	return nil
}

func createCommandForCreate(name string, keys []string, obj interface{}) string {
	v := reflect.Indirect(reflect.ValueOf(obj))
	t := v.Type()

	cmd := "CREATE TABLE %s (\n%s\n)"
	fieldNum := v.NumField()
	fields := []string{}
	for idx := 0; idx < fieldNum; idx++ {
		ft := t.Field(idx)
		alias := ft.Tag.Get(toolkit.TagName())
		if alias == "-" {
			continue
		}
		fieldName := ft.Name
		if alias != "" {
			fieldName = alias
		}
		ftName := strings.ToLower(ft.Name)
		dataType := getDataType(ftName)
		ftxt := fmt.Sprintf("%s %s", fieldName, dataType)
		if toolkit.HasMember(keys, fieldName) {
			ftxt = ftxt + " NOT NULL PRIMARY KEY"
		}
		fields = append(fields, ftxt)
	}
	cmd = fmt.Sprintf(cmd, name, strings.Join(fields, ",\n"))
	return cmd
}

func createCommandForUpdate(name string, keys []string, obj interface{}, c *Connection) string {
	// get all fields from existing
	type fieldMeta struct {
		Field   string
		Type    string
		Null    string
		Key     string
		Default string
		Extra   string
	}

	fields := map[string]fieldMeta{}
	describe := "describe " + name
	rows, _ := c.db.Query(describe)
	for rows.Next() {
		f := fieldMeta{}
		rows.Scan(&(f.Field), &(f.Type), &(f.Null), &(f.Key), &(f.Default), &(f.Extra))
		fields[f.Field] = f
	}
	//fmt.Println(fields)

	v := reflect.Indirect(reflect.ValueOf(obj))
	t := v.Type()

	cmds := []string{}

	fieldNum := t.NumField()
	for idx := 0; idx < fieldNum; idx++ {
		ft := t.Field(idx)
		fieldName := ft.Name
		alias := ft.Tag.Get(toolkit.TagName())
		if alias == "-" {
			continue
		}
		if alias != "" {
			fieldName = alias
		}

		dataType := getDataType(ft.Type.Name())
		columnDef := fmt.Sprintf("%s", dataType)
		meta, hasField := fields[fieldName]
		if !hasField {
			cmd := fmt.Sprintf("add %s %s", fieldName, columnDef)
			cmds = append(cmds, cmd)
		} else if dataType != meta.Type {
			cmd := fmt.Sprintf("modify %s %s", fieldName, columnDef)
			cmds = append(cmds, cmd)
		}
	}

	if len(cmds) > 0 {
		sql := fmt.Sprintf("alter table %s ", name)
		sql += strings.Join(cmds, ", ")
		return sql
		//fmt.Println(sql)
	}

	return ""
}

func getDataType(ftName string) string {
	dataType := "varchar(200)"
	if strings.HasPrefix(ftName, "int") {
		dataType = "int"
	} else if strings.HasPrefix(ftName, "float") {
		dataType = "real"
	} else if strings.HasPrefix(ftName, "time") {
		dataType = "datetime"
	} else if strings.HasPrefix(ftName, "bool") {
		dataType = "tinyint(1)"
	}
	return dataType
}

func (c *Connection) BeginTx() error {
	if c.IsTx() {
		return errors.New("already in transaction mode. Please commit or rollback first")
	}
	tx, e := c.db.Begin()
	if e != nil {
		return e
	}
	c.tx = tx
	return nil
}

func (c *Connection) Commit() error {
	if !c.IsTx() {
		return fmt.Errorf("not is transaction mode")
	}
	if e := c.tx.Commit(); e != nil {
		return e
	}
	c.tx = nil
	return nil
}

func (c *Connection) RollBack() error {
	if !c.IsTx() {
		return fmt.Errorf("not is transaction mode")
	}
	if e := c.tx.Rollback(); e != nil {
		return e
	}
	c.tx = nil
	return nil
}

func (c *Connection) SupportTx() bool {
	return true
}

func (c *Connection) IsTx() bool {
	return c.tx != nil
}

func (c *Connection) Tx() *sql.Tx {
	return c.tx
}

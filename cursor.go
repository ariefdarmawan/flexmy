package flexmy

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"git.kanosolution.net/kano/dbflex"
	"git.kanosolution.net/kano/dbflex/drivers/rdbms"
	"github.com/eaciit/toolkit"
)

// Cursor represent cursor object. Inherits Cursor object of rdbms drivers and implementation of dbflex.ICursor
type Cursor struct {
	rdbms.Cursor
}

func serializeValue(v string, typeName string) (interface{}, error) {
	var d interface{}
	var err error

	func() {
		if r := recover(); r != nil {
			err = errors.New(r.(string))
		}

		if typeName == "" {
			if v == "0" {
				d = int(0)
			} else if v == "" {
				d = ""
			} else {
				if f := toolkit.ToFloat64(v, 4, toolkit.RoundingAuto); f != 0 {
					d = f
				} else if dt, err := time.Parse(time.RFC3339, v); err == nil {
					d = dt
				} else if dt = toolkit.String2Date(v, "yyyy-MM-dd HH:mm:ss"); dt.Year() > 1900 {
					d = dt
				} else {
					d = v
				}
			}
		} else {
			typeName := strings.ToLower(typeName)
			if strings.HasPrefix(typeName, "float32") {
				d = toolkit.ToFloat32(d, 4, toolkit.RoundingAuto)
			} else if strings.HasPrefix(typeName, "float64") {
				d = toolkit.ToFloat64(d, 4, toolkit.RoundingAuto)
			} else if strings.HasPrefix(typeName, "int") && !strings.HasPrefix(typeName, "interface") {
				d = toolkit.ToInt(d, toolkit.RoundingAuto)
			} else if strings.HasPrefix(typeName, "time") {
				if dt, err := time.Parse(time.RFC3339, v); err == nil {
					d = dt
				} else if dt = toolkit.String2Date(v, "yyyy-MM-dd HH:mm:ss"); dt.Year() > 0 {
					d = dt
				}
			} else if strings.HasPrefix(typeName, "bool") {
				d = v == "1"
			} else {
				d = v
			}
		}
	}()

	return d, err
}

func (c *Cursor) Serialize(dest interface{}) error {
	destV := reflect.ValueOf(dest)
	if destV.Kind() != reflect.Ptr {
		return errors.New("serialization output should be a pointer of struct or pointer of map")
	}
	destElem := destV.Elem()
	if destElem.Kind() != reflect.Map && destElem.Kind() != reflect.Struct {
		return errors.New("serialization output should be a pointer of struct or pointer of map")
	}
	isMap := destElem.Kind() == reflect.Map

	//var err error
	columnNames := c.ColumnNames()

	destT := destElem.Type()

	for idx, value := range c.Values() {
		name := columnNames[idx]
		v := ""
		func() {
			defer func() {
				if r := recover(); r != nil {
					v = ""
				}
			}()
			v = string(value.([]byte))
		}()

		if !isMap {
			fieldNum := destElem.NumField()
			for i := 0; i < fieldNum; i++ {
				destFieldName := destT.Field(i).Name
				alias := destT.Field(i).Tag.Get(toolkit.TagName())
				if alias != "" {
					destFieldName = alias
				}
				if strings.ToLower(destFieldName) == strings.ToLower(name) {
					f := destElem.Field(i)
					d, e := serializeValue(v, f.Type().Name())
					if e != nil {
						return fmt.Errorf("fail to serialize %s. %s", name, e.Error())
					}
					f.Set(reflect.ValueOf(d))
				}
			}
		} else {
			d, e := serializeValue(v, "")
			if e != nil {
				return fmt.Errorf("fail to serialize %s. %s", name, e.Error())
			}
			destElem.SetMapIndex(reflect.ValueOf(name), reflect.ValueOf(d))
		}
	}

	return nil
}

func (c *Cursor) Fetchs(obj interface{}, n int) dbflex.ICursor {
	if r := recover(); r != nil {
		c.SetError(errors.New(r.(string)))
	}

	rvPtr := reflect.ValueOf(obj)
	if rvPtr.Kind() != reflect.Pointer {
		return c.SetError(errors.New("output should be a pointer of slice"))
	}

	rvSlice := rvPtr.Elem()
	if rvSlice.Kind() != reflect.Slice {
		return c.SetError(errors.New("output should be a pointer of slice"))
	}

	rvElemType := rvSlice.Type().Elem()
	rvElemTypeString := rvElemType.Kind().String()
	if rvElemTypeString != "struct" && rvElemTypeString != "map" {
		return c.SetError(errors.New("output element should be a struct or a map, currently it is a " + rvElemTypeString))
	}

	isPtr := rvElemTypeString == "ptr"
	isMap := rvElemTypeString == "map"

	if isPtr {
		rvElemTypeString = rvElemType.Elem().Kind().String()
		if rvElemTypeString != "struct" {
			return c.SetError(errors.New("output element should be a struct, pointer of struct or a map, currently it is a " + rvElemTypeString))
		}
	}

	var err error
	i := 0

	// set initial to 1000
	destSlice := reflect.MakeSlice(rvSlice.Type(), 1000, 1000)

scanLoop:
	for {
		err = c.Scan()
		if err != nil {
			if err == dbflex.EOF {
				break scanLoop
			} else {
				return c.SetError(errors.New("error while iteraring data. " + err.Error()))
			}
		}

		var newEl reflect.Value
		if isPtr {
			newEl = reflect.New(rvElemType.Elem())
		} else if !isMap {
			newEl = reflect.New(rvElemType)
		} else {
			newEl = reflect.New(rvElemType)
			newEl.Elem().Set(reflect.MakeMap(rvElemType))
		}

		if i%1000 == 0 {
			newLen := destSlice.Len() + 1000
			biggerSlice := reflect.MakeSlice(rvSlice.Type(), newLen, newLen)
			reflect.Copy(biggerSlice, destSlice)
			destSlice = biggerSlice
		}

		elObj := newEl.Interface()
		err = c.Serialize(elObj)

		if isPtr {
			destSlice.Index(i).Set(reflect.ValueOf(elObj))
		} else {
			destSlice.Index(i).Set(reflect.ValueOf(elObj).Elem())
		}
		i++
	}

	lesserSlice := reflect.MakeSlice(rvSlice.Type(), i, i)
	reflect.Copy(lesserSlice, destSlice)
	rvPtr.Elem().Set(lesserSlice)

	return c
}

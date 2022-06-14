package flexmy

import (
	"errors"
	"strings"
	"time"

	"git.kanosolution.net/kano/dbflex/drivers/rdbms"
	"github.com/eaciit/toolkit"
)

// Cursor represent cursor object. Inherits Cursor object of rdbms drivers and implementation of dbflex.ICursor
type Cursor struct {
	rdbms.Cursor
}

func (c *Cursor) CastValue(value interface{}, typeName string) (interface{}, error) {
	var d interface{}
	var err error

	v := ""
	func() {
		defer func() {
			if r := recover(); r != nil {
				v = ""
			}
		}()
		v = string(value.([]byte))
	}()

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
				d = toolkit.ToFloat32(v, 4, toolkit.RoundingAuto)
			} else if strings.HasPrefix(typeName, "float64") {
				d = toolkit.ToFloat64(v, 4, toolkit.RoundingAuto)
			} else if strings.HasPrefix(typeName, "int") && !strings.HasPrefix(typeName, "interface") {
				d = toolkit.ToInt(v, toolkit.RoundingAuto)
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

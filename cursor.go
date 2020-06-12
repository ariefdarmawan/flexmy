package flexmy

import (
	"time"

	"git.kanosolution.net/kano/dbflex/drivers/rdbms"
	"github.com/eaciit/toolkit"
)

// Cursor represent cursor object. Inherits Cursor object of rdbms drivers and implementation of dbflex.ICursor
type Cursor struct {
	rdbms.Cursor
}

func (c *Cursor) Serialize(dest interface{}) error {
	var err error
	m := toolkit.M{}
	toolkit.Serde(dest, &m, "")

	columnNames := c.ColumnNames()
	//sqlTypes := c.ColumnTypes()
	//fmt.Println("\n[debug] values:", toolkit.JsonString(c.values))
	//fmt.Println("\n[debug] values Ptr:", toolkit.JsonString(c.valuesPtr))
	for idx, value := range c.Values() {
		name := columnNames[idx]
		//ft := sqlTypes[idx]

		v := string(value.([]byte))

		if v == "0" {
			m.Set(name, 0)
		} else if v == "" {
			m.Set(name, "")
		} else {
			if f := toolkit.ToFloat64(v, 4, toolkit.RoundingAuto); f != 0 {
				m.Set(name, f)
			} else if dt, err := time.Parse(time.RFC3339, v); err == nil {
				m.Set(name, dt)
			} else if dt = toolkit.String2Date(v, rdbms.TimeFormat()); dt.Year() > 1900 {
				m.Set(name, dt)
			} else {
				m.Set(name, v)
			}
		}
		//dbflex.Logger().Debugf("%s (%s) = %s\n", name, ft, v)
		/*
			} else {
				m.Set(name, value)
				dbflex.Logger().Debugf("%s [%s] = %v\n", name, ft, value)
			}
		*/
	}

	err = toolkit.Serde(m, dest, "")
	if err != nil {
		return toolkit.Error(err.Error() + toolkit.Sprintf(" object: %s", toolkit.JsonString(m)))
	}
	return nil
}

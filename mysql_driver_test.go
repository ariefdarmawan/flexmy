package flexmy_test

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sebarcode/codekit"
	cv "github.com/smartystreets/goconvey/convey"
)

var (
	connTxt = "root:Database.1@/testdb"
)

func clasicConnect() (*sql.DB, error) {
	return sql.Open("mysql", connTxt)
}

func TestClassicMysql(t *testing.T) {
	cv.Convey("connect", t, func() {
		db, err := clasicConnect()
		cv.So(err, cv.ShouldBeNil)
		defer db.Close()

		cv.Convey("querying data", func() {
			cmd := "select * from testmodel"
			rows, err := db.Query(cmd)
			cv.So(err, cv.ShouldBeNil)
			defer rows.Close()

			cv.Convey("get the metadata", func() {
				columnNames, errColumnName := rows.Columns()
				columnTypes, errColumnType := rows.ColumnTypes()

				cv.So(errColumnName, cv.ShouldBeNil)
				cv.So(len(columnNames), cv.ShouldBeGreaterThan, 0)
				cv.So(errColumnType, cv.ShouldBeNil)
				cv.So(len(columnTypes), cv.ShouldBeGreaterThan, 0)

				sqlTypes := []string{}
				values := [][]byte{}
				valuePtrs := []interface{}{}
				for _, ct := range columnTypes {
					name := strings.ToLower(ct.DatabaseTypeName())
					//fmt.Println(columnNames[idx], " |", name)
					if strings.HasPrefix(name, "int") {
						sqlTypes = append(sqlTypes, "int")
						//values = append(values, int(0))
					} else if strings.HasPrefix(name, "dec") || strings.HasPrefix(name, "float") {
						sqlTypes = append(sqlTypes, "float64")
						//values = append(values, float64(0))
					} else if strings.HasPrefix(name, "datetime") {
						sqlTypes = append(sqlTypes, "time.Time")
						//values = append(values, time.Time{})
					} else {
						sqlTypes = append(sqlTypes, "string")
						//values = append(values, "")
					}
					values = append(values, []byte{})
				}

				for idx, _ := range values {
					valuePtrs = append(valuePtrs, &values[idx])
				}

				fmt.Println("\ncolumns: ", codekit.JsonString(columnNames),
					"\ntypes:", codekit.JsonString(sqlTypes))

				cv.Convey("validating data", func() {
					for {
						if rows.Next() {
							scanErr := rows.Scan(valuePtrs...)
							if scanErr != nil {
								cv.So(scanErr, cv.ShouldBeNil)
								break
							}
							//fmt.Println("values:", codekit.JsonString(values))
							m := codekit.M{}
							for idx, v := range values {
								name := columnNames[idx]
								ft := sqlTypes[idx]
								switch ft {
								case "int":
									m.Set(name, codekit.ToInt(string(v), codekit.RoundingAuto))

								case "float64":
									m.Set(name, codekit.ToFloat64(string(v), 4, codekit.RoundingAuto))

								case "time.Time":
									if dt, err := time.Parse(time.RFC3339, string(v)); err == nil {
										m.Set(name, dt)
									} else {
										dt = codekit.String2Date(string(v), "yyyy-MM-dd hh:mm:ss")
										m.Set(name, dt)
									}

								default:
									m.Set(name, string(v))
								}
							}
							//codekit.Println("data:", codekit.JsonString(m))
						} else {
							break
						}
					}
				})
			})
		})
	})
}

func TestClassicTrx(t *testing.T) {
	db, _ := clasicConnect()
	defer db.Close()
	_, err := db.Exec("delete from testmodel where datagroup='QT'")
	if err != nil {
		t.Fatalf("unable to clear data. %s", err.Error())
	}

	cv.Convey("insert committed data", t, func() {
		tx, err := db.Begin()
		cv.So(err, cv.ShouldBeNil)
		cmd := "INSERT INTO testmodel (ID,Title,DataDec,DataGroup,Created) " +
			"VALUES ('tx_1','Title for tx_0',48.050000,'QT','2019-11-15 07:48:34')"
		_, err = tx.Exec(cmd)
		cv.So(err, cv.ShouldBeNil)
		tx.Commit()
	})

	cv.Convey("insert rolledbacl data", t, func() {
		tx, err := db.Begin()
		cv.So(err, cv.ShouldBeNil)
		cmd := "INSERT INTO testmodel (ID,Title,DataDec,DataGroup,Created) " +
			"VALUES ('tx_2','Title for tx_0',48.050000,'QT','2019-11-15 07:48:34')"
		_, err = tx.Exec(cmd)
		cv.So(err, cv.ShouldBeNil)

		cv.Convey("validate before rollback", func() {
			var count int
			rows, _ := tx.Query("select count(*) from testmodel where datagroup='QT'")
			defer rows.Close()
			rows.Next()
			rows.Scan(&count)

			cv.So(count, cv.ShouldEqual, 2)

			cv.Convey("validate after rollback", func() {
				tx.Rollback()
				var count int
				rows, _ := db.Query("select count(*) from testmodel where datagroup='QT'")
				defer rows.Close()
				rows.Next()
				rows.Scan(&count)

				cv.So(count, cv.ShouldEqual, 1)
			})
		})

	})
}

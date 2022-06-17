package flexmy_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"git.kanosolution.net/kano/dbflex"
	"github.com/sebarcode/codekit"
	cv "github.com/smartystreets/goconvey/convey"
)

var (
	connString = "mysql://root:Database.1@/golang"
	tableName  = "testmodel"
)

func init() {
	//connString = fmt.Sprintf(connString, os.Getenv("MYSQL_USER"), os.Getenv("MYSQL_PASSWORD"))
}

type dataObject struct {
	ID        string
	Title     string
	DataDec   float64
	DataGroup string
	Created   time.Time
}

func newDataObject(id string, group string) *dataObject {
	do := new(dataObject)
	do.ID = id
	do.Title = "Title for " + id
	do.DataGroup = group
	do.DataDec = float64(codekit.RandFloat(1000, 2))
	do.Created = time.Now()
	return do
}

func connect() (dbflex.IConnection, error) {
	conn, err := dbflex.NewConnectionFromURI(connString, nil)
	if err != nil {
		return nil, errors.New("unable to connect. " + err.Error())
	}
	err = conn.Connect()
	if err != nil {
		return nil, errors.New("unable to connect. " + err.Error())
	}
	return conn, nil
}

func TestQueryDropCreateTable(t *testing.T) {
	cv.Convey("connecting", t, func() {
		conn, err := connect()
		cv.So(err, cv.ShouldBeNil)
		defer conn.Close()

		cv.Convey("drop table", func() {
			conn.DropTable(tableName)
			cv.So(err, cv.ShouldBeNil)

			cv.Convey("ensure table", func() {
				err = conn.EnsureTable(tableName, []string{"ID"}, newDataObject("", ""))
				cv.So(err, cv.ShouldBeNil)
			})
		})
	})
}

func TestQueryM(t *testing.T) {
	cv.Convey("connecting", t, func() {
		conn, err := connect()
		cv.So(err, cv.ShouldBeNil)
		defer conn.Close()

		cv.Convey("saving data", func() {
			cmd := dbflex.From(tableName).Where(dbflex.Eq("id", "e1")).Save()
			_, err := conn.Execute(cmd, codekit.M{}.Set("data", &dataObject{"E1", "Emp01", 20.37, "", time.Now()}))
			cv.So(err, cv.ShouldBeNil)

			cv.Convey("querying", func() {
				cmd := dbflex.From(tableName).Select()
				cur := conn.Cursor(cmd, nil)
				defer cur.Close()
				cv.So(cur.Error(), cv.ShouldBeNil)

				cv.Convey("get results", func() {
					ms := []codekit.M{}
					err := cur.Fetchs(&ms, 0)
					cv.So(err, cv.ShouldBeNil)
					cv.So(len(ms), cv.ShouldBeGreaterThan, 0)
				})
			})
		})

	})
}

func TestQueryObj(t *testing.T) {
	cv.Convey("connecting", t, func() {
		conn, err := connect()
		cv.So(err, cv.ShouldBeNil)
		defer conn.Close()

		cv.Convey("querying", func() {
			cmd := dbflex.From(tableName).Select()
			cur := conn.Cursor(cmd, nil)
			defer cur.Close()
			cv.So(cur.Error(), cv.ShouldBeNil)

			cv.Convey("get results", func() {
				ms := []dataObject{}
				err := cur.Fetchs(&ms, 2)
				cv.So(err, cv.ShouldBeNil)
				cv.So(len(ms), cv.ShouldBeGreaterThan, 0)

				cv.Printf("\nResults:\n%s\n", codekit.JsonString(ms))
			})
		})
	})
}

func TestQueryDelete(t *testing.T) {
	cv.Convey("connecting", t, func() {
		conn, err := connect()
		cv.So(err, cv.ShouldBeNil)
		defer conn.Close()

		cv.Convey("insert data 100x ", func() {
			for i := 0; i < 100; i++ {
				cmd := dbflex.From(tableName).Insert()
				_, err := conn.Execute(cmd, codekit.M{}.Set("data", newDataObject(codekit.RandomString(10), "QD")))
				if err != nil {
					cv.Println("error saving.", err.Error())
				}
			}
			cursor := conn.Cursor(dbflex.From(tableName).Where(dbflex.Eq("datagroup", "QD")).Select(), nil)
			cv.So(cursor.Error(), cv.ShouldEqual, nil)
			count := cursor.Count()
			cv.So(count, cv.ShouldBeGreaterThan, 99)

			cv.Convey("delete fews data", func() {
				dos := make([]dataObject, 5)
				cursor.Fetchs(&dos, 5)
				cursor.Close()

				for _, do := range dos {
					//fmt.Println("deleting", do.ID)
					cmdDel := dbflex.From(tableName).Delete().Where(dbflex.Eq("id", do.ID))
					_, err := conn.Execute(cmdDel, nil)
					if err != nil {
						cv.Println("unable to delete", do.ID, " error:", err.Error())
					}
				}

				conn1, _ := connect()
				defer conn1.Close()
				cursor1 := conn1.Cursor(dbflex.From(tableName).Select().Where(dbflex.Eq("datagroup", "QD")), nil)
				defer cursor1.Close()
				countAfterDel := cursor1.Count()
				cv.So(countAfterDel, cv.ShouldEqual, count-5)
			})
		})
	})
}

func TestUpdate(t *testing.T) {
	conn, _ := connect()
	defer conn.Close()

	cv.Convey("get data to be updated", t, func() {
		cmd := dbflex.From(tableName).Select().Where(dbflex.Eq("datagroup", "QD"))
		cur := conn.Cursor(cmd, nil)
		cv.So(cur.Error(), cv.ShouldBeNil)
		originalCount := cur.Count()
		ms := []dataObject{}
		cur.Fetchs(&ms, 5)
		cv.So(len(ms), cv.ShouldEqual, 5)

		cv.Convey("update", func() {
			var err error
			for _, m := range ms {
				cmd := dbflex.From(tableName).Update("datagroup").Where(dbflex.Eq("id", m.ID))
				m.DataGroup = "QA"
				_, err = conn.Execute(cmd, codekit.M{}.Set("data", m))
				if err != nil {
					break
				}
			}
			cv.So(err, cv.ShouldBeNil)

			cv.Convey("validate", func() {
				cmd := dbflex.From(tableName).Select().Where(dbflex.Eq("datagroup", "QD"))
				cur := conn.Cursor(cmd, nil)
				newCount := cur.Count()
				cv.So(newCount, cv.ShouldEqual, originalCount-5)
			})
		})
	})
}

func TestTrx(t *testing.T) {
	conn, _ := connect()
	defer conn.Close()
	groupcode := "QT"

	conn.Execute(dbflex.From(tableName).Delete().Where(dbflex.Eq("datagroup", groupcode)), nil)
	cv.Convey("insert data", t, func() {
		conn.BeginTx()
		var err error
		cmd := dbflex.From(tableName).Insert()
		for i := 0; i < 10 && err == nil; i++ {
			data := newDataObject(fmt.Sprintf("tx_%d", i), groupcode)
			_, err = conn.Execute(cmd, codekit.M{}.Set("data", data))
		}
		cv.So(err, cv.ShouldBeNil)

		cv.Convey("validate", func() {
			cmd := dbflex.From(tableName).Select().Where(dbflex.Eq("datagroup", groupcode))
			cur := conn.Cursor(cmd, nil)
			cv.So(cur.Error(), cv.ShouldBeNil)

			ms := []dataObject{}
			cur.Fetchs(&ms, 0)
			cv.So(len(ms), cv.ShouldEqual, 10)
			cur.Close()

			cv.Convey("rollback", func() {
				conn.RollBack()
				cur = conn.Cursor(cmd, nil)
				cv.So(cur.Error(), cv.ShouldBeNil)
				cv.So(cur.Error(), cv.ShouldBeNil)
				cur.Fetchs(&ms, 0)
				cv.So(len(ms), cv.ShouldEqual, 0)
			})
		})
	})
}

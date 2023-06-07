package dbmgr

import (
	"database/sql"
	"fmt"
	"github.com/mattn/go-sqlite3"
	"os"
	"testing"
)

func TestNewDB(t *testing.T) {

	fnCreate := func(id string, opts DBConnOptions) error {
		fname := "/tmp/" + id + ".db"
		connstr := "file:" + fname + opts.ConnstrOpts("rwc")
		fmt.Println("creating test db " + fname + " with connstr " + connstr)
		db, err := sql.Open("sqlite3", connstr)
		if err != nil {
			fmt.Println("error creating new file: " + err.Error())
			return err
		}

		// Need to ping the DB in order to serialize it to disk, evidently.
		// (That is, omitting this results in the file not being created)
		if db.Ping() != nil {
			return db.Ping()
		}

		db.Close()
		return nil
	}

	fnGet := func(id string) (string, error) {
		return "/tmp/" + id + ".db", nil
	}

	fname, _ := fnGet("test")
	os.Remove(fname)

	driver := &sqlite3.SQLiteDriver{}
	conn, err := OpenOrCreateDBConn(driver, "test", fnGet, fnCreate, DBConnOptions{})
	if err != nil {
		fmt.Println("Error opening/creating db: " + err.Error())
		t.Error(err.Error())
		return
	}
	fmt.Println("Connection opened")

	_, err = conn.Exec("create table test(name string);")
	if err != nil {
		fmt.Println("Error creating table in db: " + err.Error())
		t.Error(err.Error())
		return
	}
	ires, err := conn.Exec("insert into test(name) values('test');")
	if err != nil {
		t.Error(err.Error())
		return
	}
	i, err := ires.LastInsertId()
	if err != nil {
		fmt.Println("Error inserting into db: " + err.Error())
		t.Error(err.Error())
		return
	}
	fmt.Printf("Inserted: %d\n", i)

	rows, err := conn.Query("select * from test;")
	if err != nil {
		fmt.Println("Error selecting from db: " + err.Error())
		t.Error(err.Error())
		return
	}
	for rows.Next() {
		var s string
		err = rows.Scan(&s)
		if err != nil {
			fmt.Println("Error reading returned row from db: " + err.Error())
			t.Error(err.Error())
			return
		}
		fmt.Printf("%s\n", s)
	}
	isAuth := conn.AuthEnabled()
	fmt.Printf("%t\n", isAuth)
}

package tests

import (
	"database/sql"
	"fmt"
	"github.com/highgrav/rhizome"
	"github.com/highgrav/rhizome/internal/dbmgr"
	"github.com/mattn/go-sqlite3"
	"os"
	"testing"
	"time"
)

func TestNewDB(t *testing.T) {
	rhizome.Init(rhizome.RhizomeConfig{})
	fnCreate := func(id string, opts dbmgr.DBConnOptions) error {
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

	dbm := dbmgr.NewDBManager(dbmgr.DBManagerConfig{
		BaseDir:     "",
		MaxDBsOpen:  500,
		MaxIdleTime: 10 * time.Minute,
		SweepEach:   60 * time.Second,
		FnGetDB:     fnGet,
		FnNewDB:     fnCreate,
	}, dbmgr.DBConnOptions{
		UseJModeWAL:           true,
		CacheShared:           false,
		SecureDeleteFast:      true,
		AutoVacuumIncremental: true,
		CaseSensitiveLike:     false,
		ForeignKeys:           false,
	})

	fid := "testdb"
	fname, _ := fnGet(fid)
	os.Remove(fname)

	driver := &sqlite3.SQLiteDriver{}
	conn, err := dbmgr.OpenOrCreateDBConn(dbm, driver, fid, fnGet, fnCreate, dbmgr.DBConnOptions{})
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

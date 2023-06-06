package dbmgr

import (
	"database/sql"
	"fmt"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestCreateDBMgr(t *testing.T) {
	fnCreate := func(id string) error {
		fname := "/tmp/" + id + ".db"
		connstr := "file:" + fname + "?cache=shared&mode=rwc"
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
		return "/tmp/dbs/" + id + ".db", nil
	}

	dbm := NewDBManager(DBManagerConfig{
		BaseDir:     "",
		MaxDBsOpen:  500,
		MaxIdleTime: 10 * time.Minute,
		SweepEach:   60 * time.Second,
	}, fnGet, fnCreate)

	for i := 0; i < dbm.Cfg.MaxDBsOpen; i++ {
		id := strconv.FormatInt(int64(i), 10)
		err := dbm.OpenOrCreate(id, DBConnOptions{})
		if err != nil {
			t.Error(err.Error())
			return
		}
		err = dbm.DBs[id].Ping()
		if err != nil {
			t.Error(err.Error())
			return
		}
		conn, err := dbm.GetOrCreate(id)
		if err != nil {
			t.Error(err.Error())
			return
		}
		q := "create table test(name string);"
		conn.Exec(q)
		q = "insert into test(name)values('hello, world " + id + "');"
		conn.Exec(q)
		rows, err := conn.Query("select * from test;")
		if err != nil {
			t.Error(err.Error())
			return
		}
		for rows.Next() {
			var s string
			err = rows.Scan(&s)
			if err != nil {
				t.Error(err.Error())
				return
			}
			fmt.Printf("%s\n", s)
		}
	}
	bToMb := func(b uint64) uint64 {
		return b / 1024 / 1024
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)

}

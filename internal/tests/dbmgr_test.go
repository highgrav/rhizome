package tests

import (
	"database/sql"
	"fmt"
	"github.com/highgrav/rhizome"
	"github.com/highgrav/rhizome/internal/dbmgr"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestCreateDBMgr(t *testing.T) {
	rhizome.Init(rhizome.RhizomeConfig{})
	fnCreate := func(id string, opts dbmgr.DBConnOptions) error {
		fname := "/tmp/dbs/" + id + ".db"
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
		return "/tmp/dbs/" + id + ".db", nil
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

	for i := 0; i < dbm.Cfg.MaxDBsOpen; i++ {
		id := strconv.FormatInt(int64(i), 10)
		err := dbm.OpenOrCreate(id, dbmgr.DBConnOptions{})
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

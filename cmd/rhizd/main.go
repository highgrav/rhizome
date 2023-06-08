package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/highgrav/rhizome"
	"github.com/highgrav/rhizome/internal/dbmgr"
	"github.com/highgrav/rhizome/internal/pgif"
	"log"
	"net"
	"time"
)

func main() {

	fnGet := func(id string) (string, error) {
		return "/tmp/" + id + ".db", nil
	}

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

	cfg := dbmgr.DBManagerConfig{
		BaseDir:        "/tmp/",
		MaxDBsOpen:     500,
		MaxIdleTime:    10 * time.Second,
		SweepEach:      10 * time.Second,
		CheckpointEach: 5 * time.Minute,
		FnGetDB:        fnGet,
		FnNewDB:        fnCreate,
		LogDbOpenClose: true,
	}
	mgr := rhizome.NewDBManager(cfg, dbmgr.DBConnOptions{
		UseJModeWAL:           true,
		CacheShared:           false,
		SecureDeleteFast:      true,
		AutoVacuumIncremental: true,
		CaseSensitiveLike:     false,
		ForeignKeys:           false,
	})
	ln, err := net.Listen("tcp", ":5432")
	fmt.Println("listening...")
	if err != nil {
		panic(err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}
		b := rhizome.NewRhizomeBackend(context.Background(), conn, mgr, pgif.BackendConfig{
			LogLevel:      pgif.LogLevelDebug,
			ServerVersion: "9",
		})
		go func() {
			err := b.Run()
			if err != nil {
				log.Println(err.Error())
			}
			log.Println("Closed connection from", conn.RemoteAddr())
		}()
	}
}

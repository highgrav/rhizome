package main

import (
	"context"
	"database/sql"
	"fmt"
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

	cfg := dbmgr.DBManagerConfig{
		BaseDir:        "/tmp/",
		MaxDBsOpen:     500,
		MaxIdleTime:    10 * time.Minute,
		SweepEach:      30 * time.Second,
		CheckpointEach: 5 * time.Minute,
		UseWAL:         false,
	}
	mgr := dbmgr.NewDBManager(cfg, fnGet, fnCreate)
	ln, err := net.Listen("tcp", ":9898")
	fmt.Println("listening...")
	if err != nil {
		panic(err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}
		b := pgif.NewRhizomeBackend(context.Background(), conn, mgr, true)
		go func() {
			err := b.Run()
			if err != nil {
				log.Println(err.Error())
			}
			log.Println("Closed connection from", conn.RemoteAddr())
		}()
	}
}

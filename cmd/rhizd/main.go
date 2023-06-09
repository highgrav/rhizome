package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/highgrav/rhizome"
	"github.com/highgrav/rhizome/internal/constants"
	"github.com/highgrav/rhizome/internal/dbmgr"
	"github.com/highgrav/rhizome/internal/pgif"
	"github.com/mattn/go-sqlite3"
	"log"
	"net"
	"time"
)

func main() {
	logFn := func(txt string) int {
		fmt.Println(txt)
		return len(txt)
	}

	authFn := func(actionCode int, arg1, arg2, arg3 string) int {
		fmt.Printf("Action_code %d, %q %q %q\n", actionCode, arg1, arg2, arg3)
		return sqlite3.SQLITE_OK
	}

	pgFnVersion := func() string {
		return "sqlite3"
	}

	rhizome.Init(rhizome.RhizomeConfig{
		Authorizer: authFn,
		CustomFns: []rhizome.CustomFunction{
			{
				Name:   "log",
				Fn:     logFn,
				IsPure: true,
			},
			{
				Name:   "version",
				Fn:     pgFnVersion,
				IsPure: true,
			},
		},
	})

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

	fnAuthorize := func(username, pwd, db string) (bool, error) {
		if username == "test" && pwd == "test" {
			return true, nil
		}
		return false, nil
	}

	cfg := dbmgr.DBManagerConfig{
		BaseDir:         "/tmp/",
		MaxDBsOpen:      500,
		MaxIdleTime:     10 * time.Second,
		SweepEach:       10 * time.Second,
		CheckpointEach:  5 * time.Minute,
		FnGetDB:         fnGet,
		FnNewDB:         fnCreate,
		FnCheckDBAccess: fnAuthorize,
		LogDbOpenClose:  true,
		LogLevel:        constants.LogLevelDebug,
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
			LogLevel:      constants.LogLevelDebug,
			ServerVersion: "9",
			UseTLS:        true,
			TLSCertDir:    "/tmp/certs",
			TLSCertName:   "server.crt",
			TLSKeyName:    "server.key",
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

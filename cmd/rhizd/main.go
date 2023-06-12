package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"github.com/google/deck"
	"github.com/highgrav/rhizome"
	"github.com/highgrav/rhizome/internal/constants"
	"github.com/highgrav/rhizome/internal/dbmgr"
	"github.com/highgrav/rhizome/internal/pgif"
	"github.com/mattn/go-sqlite3"
	"github.com/tg123/go-htpasswd"
	"net"
	"os"
	"path"
	"strconv"
	"time"
)

func main() {
	var openedConns, closedConns, erroredConns int

	var htaccess *htpasswd.File = nil
	var htgroups *htpasswd.HTGroup = nil

	var portFlag = flag.Int("port", 5432, "Port to listen on")
	var dbDirFlag = flag.String("dir", "/tmp", "Directory for database files")
	var logLevelFlag = flag.Int("ll", 3, "Syslog level (0-7) for logging")
	var tlsDirFlag = flag.String("tlsdir", "", "Directory for TLS files, if any")
	var tlsCertFlag = flag.String("cert", "", "TLS Cert filename")
	var tlsKeyFlag = flag.String("key", "", "TLS Key filename")
	var accessDirFlag = flag.String("udir", "", "Directory of the user and group files, if any")
	var accessFileFlag = flag.String("ufile", "", "Path and name of the user file, if any")
	var groupFileFlag = flag.String("gfile", "", "Path and name of the groups file, if any")
	flag.Parse()

	htdir := *accessDirFlag
	htfile := *accessFileFlag
	grpfile := *groupFileFlag

	if htdir != "" && htfile != "" {
		fs, err := os.Stat(path.Join(htdir, htfile))
		if err != nil || fs.IsDir() {
			panic("user file " + path.Join(htdir, htfile) + " does not exist or is a directory")
		}
		htaccess, err = htpasswd.New(path.Join(htdir, htfile), htpasswd.DefaultSystems, nil)
		if err != nil {
			panic("failure to open user file " + path.Join(htdir, htfile) + ": " + err.Error())
		}
		if grpfile != "" {
			htgroups, err = htpasswd.NewGroups(path.Join(htdir, grpfile), nil)
			if err != nil {
				panic("failure to open groups file " + path.Join(htdir, grpfile))
			}
		}
	}
	if htaccess != nil {
		fmt.Println("Opened user file " + path.Join(htdir, htfile))
	}
	if htgroups != nil {
		fmt.Println("Opened group file " + path.Join(htdir, grpfile))
	}
	port := *portFlag
	dbDir := *dbDirFlag
	fs, err := os.Stat(dbDir)
	if err != nil || fs.IsDir() == false {
		panic("DB dir " + dbDir + " does not exist or is not a directory")
	}
	if *tlsDirFlag != "" {
		if *tlsDirFlag == "" || *tlsCertFlag == "" || *tlsKeyFlag == "" {
			panic("If tls=true, tlsdir, cert, and key must all be set")
		}
		fi, err := os.Stat(*tlsDirFlag)
		if err != nil {
			panic("error opening TLS dir: " + err.Error())
		}
		if fi.IsDir() == false {
			panic("error opening TLS dir: " + *tlsDirFlag + " is not a directory")
		}
		_, err = os.Stat(path.Join(*tlsDirFlag, *tlsCertFlag))
		if err != nil {
			panic("error opening TLS cert " + path.Join(*tlsDirFlag, *tlsCertFlag) + ": " + err.Error())
		}
		_, err = os.Stat(path.Join(*tlsDirFlag, *tlsKeyFlag))
		if err != nil {
			panic("error opening TLS key " + path.Join(*tlsDirFlag, *tlsKeyFlag) + ": " + err.Error())
		}
	}

	rhzCfg := pgif.BackendConfig{
		LogLevel:    *logLevelFlag,
		TLSCertDir:  *tlsDirFlag,
		TLSCertName: *tlsCertFlag,
		TLSKeyName:  *tlsKeyFlag,
	}
	if *tlsDirFlag != "" {
		rhzCfg.UseTLS = true
	}

	logFn := func(txt string) int {
		fmt.Println(txt)
		return len(txt)
	}

	authFn := func(actionCode int, arg1, arg2, arg3 string) int {
		if *logLevelFlag > constants.LogLevelDebug {
			fmt.Printf("Action_code %d, %q %q %q\n", actionCode, arg1, arg2, arg3)
		}
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
		return path.Join(dbDir, id+".db"), nil
	}

	fnCreate := func(id string, opts dbmgr.DBConnOptions) error {
		fname := dbDir + id + ".db"
		connstr := "file:" + fname + opts.ConnstrOpts("rwc")
		deck.Infof("creating test db %s with connstr %q", fname, connstr)
		db, err := sql.Open("sqlite3", connstr)
		if err != nil {
			deck.Fatalf("error creating new file: %s", err.Error())
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
		if htaccess == nil {
			return true, nil
		}
		if !htaccess.Match(username, pwd) {
			return false, nil
		}
		if htgroups == nil {
			return true, nil
		}
		return htgroups.IsUserInGroup(username, db), nil
	}

	cfg := dbmgr.DBManagerConfig{
		BaseDir:         dbDir,
		MaxDBsOpen:      1000,
		MaxIdleTime:     5 * time.Minute,
		SweepEach:       30 * time.Second,
		CheckpointEach:  5 * time.Minute,
		FnGetDB:         fnGet,
		FnNewDB:         fnCreate,
		FnCheckDBAccess: fnAuthorize,
		LogDbOpenClose:  true,
		LogLevel:        rhzCfg.LogLevel,
	}
	mgr := rhizome.NewDBManager(cfg, dbmgr.DBConnOptions{
		UseJModeWAL:           true,
		CacheShared:           false,
		SecureDeleteFast:      true,
		AutoVacuumIncremental: true,
		CaseSensitiveLike:     false,
		ForeignKeys:           false,
	})
	ln, err := net.Listen("tcp", ":"+strconv.FormatInt(int64(port), 10))
	deck.Infof("listening on port %d...\n", port)

	st := time.NewTicker(time.Second * 60)
	go func() {
		for {
			select {
			case _ = <-st.C:
				deck.Infof("conns: %d opened, %d closed, %d errs, %d active\n", openedConns, closedConns, erroredConns, (openedConns - closedConns))
			}
		}
	}()

	if err != nil {
		panic(err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			erroredConns++
			deck.Errorf("error accepting connection: %s", err)
		} else {
			openedConns++
			b := rhizome.NewRhizomeBackend(context.Background(), conn, mgr, rhzCfg)
			go func() {
				err := b.Run()
				if err != nil {
					deck.Errorf("error processing queries from %s: %s", conn.RemoteAddr(), err.Error())
				}
				if *logLevelFlag > constants.LogLevelDebug {
					deck.Infof("Closed connection from %s", conn.RemoteAddr())
				}
				closedConns++
			}()
		}
	}
}

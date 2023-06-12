package dbmgr

import (
	"database/sql"
	"github.com/google/deck"
	"github.com/highgrav/rhizome/internal/constants"
	"github.com/mattn/go-sqlite3"
	"sync"
	"sync/atomic"
	"time"
)

type DBManager struct {
	sync.Mutex
	Cfg         DBManagerConfig
	Driver      *sqlite3.SQLiteDriver
	DBs         map[string]*DBConn
	ticker      *time.Ticker
	done        chan bool
	GetFilename FnGetFilenameFromID
	CreateDb    FnCreateNewDB
	DefaultOpts DBConnOptions
	Stats       map[string]*atomic.Int64
}

func NewDBManager(cfg DBManagerConfig, defaultOpts DBConnOptions) *DBManager {
	dbm := &DBManager{
		Cfg:         cfg,
		Driver:      &sqlite3.SQLiteDriver{},
		DBs:         make(map[string]*DBConn),
		ticker:      time.NewTicker(cfg.SweepEach),
		done:        make(chan bool),
		GetFilename: cfg.FnGetDB,
		CreateDb:    cfg.FnNewDB,
		DefaultOpts: defaultOpts,
		Stats:       make(map[string]*atomic.Int64),
	}
	dbm.Stats[constants.StatOpenDbs] = &atomic.Int64{}

	go func() {
		for {
			select {
			case <-dbm.done:
				dbm.ticker.Stop()
				return
			case _ = <-dbm.ticker.C:
				dbm.sweep()
			}
		}
	}()
	return dbm
}

func (dbm *DBManager) UpdateStat(name string, val int64) {
	_, ok := dbm.Stats[name]
	if !ok {
		dbm.Stats[name] = &atomic.Int64{}
	}
	dbm.Stats[name].Add(val)
}

/*
AddConn() is used by open DBConns to deal with databases that have been unloaded, by allowing them to repopulate the DB
map. This also handles a race condition where two attempts to populate a DB overlap, by returning a sql.DB if a valid one
already exists. The DBConn is responsible for closing its own connection if this happens and instead using the one returned
from this function.
*/
func (dbm *DBManager) AddConn(id string, conn *DBConn) *sql.DB {
	dbm.Lock()
	defer dbm.Unlock()
	c, ok := dbm.DBs[id]
	if !ok {
		dbm.DBs[id] = conn
		return nil
	}
	if c.DB != nil {
		if c.DB.Ping() == nil {
			// we have an active connection, so return it instead
			return c.DB
		}
	}
	dbm.DBs[id] = conn
	return nil
}

func (dbm *DBManager) sweep() {
	var dbs []string = make([]string, 0)
	for k, v := range dbm.DBs {
		if v.LastAccessed.Before(time.Now().Add(-1 * dbm.Cfg.MaxIdleTime)) {
			dbs = append(dbs, k)
		}
	}
	if len(dbs) == 0 {
		return
	}
	dbm.Lock()
	defer dbm.Unlock()
	for _, v := range dbs {
		dbm.CloseDB(v)
	}
}

func (dbm *DBManager) Get(id string) (*DBConn, error) {
	conn, ok := dbm.DBs[id]
	if ok {
		return conn, nil
	}
	err := dbm.Open(id, dbm.DefaultOpts)
	if err != nil {
		return nil, err
	}
	conn, ok = dbm.DBs[id]
	if ok {
		return conn, nil
	}
	return nil, ErrCouldNotOpenFile
}

func (dbm *DBManager) GetOrCreate(id string) (*DBConn, error) {
	conn, ok := dbm.DBs[id]
	if ok {
		return conn, nil
	}
	err := dbm.OpenOrCreate(id, dbm.DefaultOpts)
	if err != nil {
		return nil, err
	}
	conn, ok = dbm.DBs[id]
	if ok {
		return conn, nil
	}
	return nil, ErrCouldNotOpenFile
}

func (dbm *DBManager) Open(id string, opts DBConnOptions) error {
	if _, ok := dbm.DBs[id]; ok {
		return nil
	}
	if dbm.Stats[constants.StatOpenDbs].Load() > int64(dbm.Cfg.MaxDBsOpen) {
		return ErrTooManyDBsOpen
	}
	dbm.Lock()
	defer dbm.Unlock()
	db, err := OpenDBConn(dbm, dbm.Driver, id, dbm.GetFilename, opts)
	if err != nil {
		deck.Errorf("failed to open database %s: %s", id, err.Error())
		return err
	}
	dbm.DBs[id] = db
	return nil
}

func (dbm *DBManager) OpenOrCreate(id string, opts DBConnOptions) error {
	if _, ok := dbm.DBs[id]; ok {
		return nil
	}
	if dbm.Stats[constants.StatOpenDbs].Load() > int64(dbm.Cfg.MaxDBsOpen) {
		return ErrTooManyDBsOpen
	}
	dbm.Lock()
	defer dbm.Unlock()
	db, err := OpenOrCreateDBConn(dbm, dbm.Driver, id, dbm.GetFilename, dbm.CreateDb, opts)
	if err != nil {
		deck.Errorf("failed to open or create database %s: %s", id, err.Error())
		return err
	}
	dbm.DBs[id] = db
	return nil
}

func (dbm *DBManager) Close() {
	dbm.Lock()
	defer dbm.Unlock()
	dbm.done <- true
	for k, _ := range dbm.DBs {
		dbm.CloseDB(k)
	}
}

func (dbm *DBManager) CloseDB(id string) {
	if dbm.Cfg.LogDbOpenClose {
		deck.Infof("Closing db %s", id)
	}
	conn, ok := dbm.DBs[id]
	if !ok || conn == nil {
		return
	}
	conn.Close()
	delete(dbm.DBs, id)
	return
}

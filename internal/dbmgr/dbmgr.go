package dbmgr

import (
	"github.com/mattn/go-sqlite3"
	"sync"
	"time"
)

type FnGetFilenameFromID func(id string) (string, error)
type FnCreateNewDB func(id string) error

type DBManager struct {
	sync.Mutex
	Cfg         DBManagerConfig
	Driver      *sqlite3.SQLiteDriver
	DBs         map[string]*DBConn
	ticker      *time.Ticker
	done        chan bool
	GetFilename FnGetFilenameFromID
	CreateDb    FnCreateNewDB
}

func NewDBManager(cfg DBManagerConfig, fnGet FnGetFilenameFromID, fnNew FnCreateNewDB) *DBManager {
	dbm := &DBManager{
		Cfg:         cfg,
		Driver:      &sqlite3.SQLiteDriver{},
		DBs:         make(map[string]*DBConn),
		ticker:      time.NewTicker(cfg.SweepEach),
		done:        make(chan bool),
		GetFilename: fnGet,
		CreateDb:    fnNew,
	}

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
	err := dbm.Open(id, DBConnOptions{})
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
	err := dbm.OpenOrCreate(id, DBConnOptions{})
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
	dbm.Lock()
	defer dbm.Unlock()
	if _, ok := dbm.DBs[id]; ok {
		return nil
	}

	db, err := OpenDBConn(dbm.Driver, id, dbm.GetFilename, opts)
	if err != nil {
		return err
	}
	dbm.DBs[id] = db
	return nil
}

func (dbm *DBManager) OpenOrCreate(id string, opts DBConnOptions) error {
	dbm.Lock()
	defer dbm.Unlock()
	if _, ok := dbm.DBs[id]; ok {
		return nil
	}

	db, err := OpenOrCreateDBConn(dbm.Driver, id, dbm.GetFilename, dbm.CreateDb, opts)
	if err != nil {
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
	conn, ok := dbm.DBs[id]
	if !ok || conn == nil {
		return
	}
	conn.Close()
	delete(dbm.DBs, id)
	return
}

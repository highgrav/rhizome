package dbmgr

import (
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
	DBs         map[string]*DBConnGroup
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
		DBs:         make(map[string]*DBConnGroup),
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

func (dbm *DBManager) AddConn(id string, conn *DBConn) error {
	dbm.Lock()
	defer dbm.Unlock()
	cgrp, ok := dbm.DBs[id]
	var err error
	if !ok || cgrp == nil {
		cgrp, err = NewDBConnGroup(id)
		if err != nil {
			return err
		}
		dbm.DBs[id] = cgrp
		err := dbm.DBs[id].AddConn(conn)
		if err != nil {
			return err
		}
		return nil
	}
	err = cgrp.AddConn(conn)
	if err != nil {
		return err
	}
	return nil
}

func (dbm *DBManager) sweep() {
	for _, varr := range dbm.DBs {
		varr.Sweep(dbm.Cfg.MaxIdleTime)
	}
}

func (dbm *DBManager) Get(id string) (*DBConn, error) {
	dbm.Lock()
	conngrp, ok := dbm.DBs[id]
	dbm.Unlock()
	if ok {
		conn, err := conngrp.NewConn(dbm, dbm.DefaultOpts)
		if err != nil {
			return nil, err
		}
		return conn, nil
	}
	err := dbm.Open(id)
	if err != nil {
		return nil, err
	}
	conngrp, ok = dbm.DBs[id]
	if ok {
		conn, err := conngrp.NewConn(dbm, dbm.DefaultOpts)
		if err != nil {
			return nil, err
		}
		return conn, nil
	}
	return nil, ErrCouldNotOpenFile
}

func (dbm *DBManager) GetOrCreate(id string) (*DBConn, error) {
	dbm.Lock()
	conngrp, ok := dbm.DBs[id]
	dbm.Unlock()
	if ok {
		conn, err := conngrp.NewConn(dbm, dbm.DefaultOpts)
		if err != nil {
			return nil, err
		}
		return conn, nil
	}
	err := dbm.OpenOrCreate(id)
	if err != nil {
		return nil, err
	}
	conngrp, ok = dbm.DBs[id]
	if ok {
		conn, err := conngrp.NewConn(dbm, dbm.DefaultOpts)
		if err != nil {
			return nil, err
		}
		return conn, nil
	}
	return nil, ErrCouldNotOpenFile
}

func (dbm *DBManager) Open(id string) error {
	dbm.Lock()
	defer dbm.Unlock()
	if _, ok := dbm.DBs[id]; ok {
		return nil
	}
	if dbm.Stats[constants.StatOpenDbs].Load() > int64(dbm.Cfg.MaxDBsOpen) {
		return ErrTooManyDBsOpen
	}
	conngrp, err := NewDBConnGroup(id)
	if err != nil {
		return err
	}
	dbm.DBs[id] = conngrp
	return nil
}

func (dbm *DBManager) OpenOrCreate(id string) error {
	dbm.Lock()
	defer dbm.Unlock()
	if _, ok := dbm.DBs[id]; ok {
		return nil
	}
	if dbm.Stats[constants.StatOpenDbs].Load() > int64(dbm.Cfg.MaxDBsOpen) {
		return ErrTooManyDBsOpen
	}
	dbm.Lock()
	defer dbm.Unlock()
	db, err := NewDBConnGroup(id)
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
	grp, ok := dbm.DBs[id]
	if !ok || grp == nil {
		return
	}
	grp.Close()
	delete(dbm.DBs, id)
	return
}

package dbmgr

import (
	"context"
	"database/sql"
	sqlite3 "github.com/mattn/go-sqlite3"
	"os"
	"sync"
	"time"
)

type DBConn struct {
	sync.RWMutex
	LastAccessed time.Time
	ID           string
	DB           *sql.DB
	driver       *sqlite3.SQLiteDriver
}

func OpenOrCreateDBConn(driver *sqlite3.SQLiteDriver, id string, fnGet FnGetFilenameFromID, fnCreate FnCreateNewDB, opts DBConnOptions) (*DBConn, error) {
	filepath, err := fnGet(id)
	if err != nil {
		return nil, err
	}
	connstr := "file:" + filepath + opts.ConnstrOpts("rw")

	db, err := sql.Open("sqlite3", connstr)
	if err != nil || db.Ping() != nil {
		// try to create the DB if necessary
		err2 := fnCreate(id, opts)
		if err2 != nil {
			return nil, err2
		}
		db, err = sql.Open("sqlite3", connstr)
		if err != nil {
			return nil, err
		}
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	dbc := &DBConn{
		LastAccessed: time.Now(),
		ID:           id,
		DB:           db,
		driver:       driver,
	}

	return dbc, nil
}

func OpenDBConn(driver *sqlite3.SQLiteDriver, id string, fnGet FnGetFilenameFromID, opts DBConnOptions) (*DBConn, error) {
	filepath, err := fnGet(id)
	if err != nil {
		return nil, err
	}
	fst, err := os.Stat(filepath)
	if err != nil {
		return nil, err
	}
	if fst.IsDir() {
		return nil, ErrCouldNotOpenFile
	}

	// TODO -- handle DB options here
	connstr := "file:" + filepath + opts.ConnstrOpts("rw")
	db, err := sql.Open("sqlite3", connstr)

	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	dbc := &DBConn{
		LastAccessed: time.Now(),
		ID:           id,
		DB:           db,
		driver:       driver,
	}
	return dbc, nil
}

func (dbc *DBConn) Ping() error {
	if dbc.DB == nil {
		return ErrDBNotOpen
	}
	return dbc.DB.Ping()
}

func (dbc *DBConn) Close() {
	if dbc.DB == nil {
		return
	}
	dbc.Lock()
	defer dbc.Unlock()
	dbc.DB.Close()
	dbc.DB = nil
}

func (dbc *DBConn) AuthEnabled() bool {
	if dbc.DB == nil {
		return false
	}
	c, err := dbc.DB.Conn(context.Background())
	if err != nil {
		return false
	}
	defer c.Close()
	var isAuth bool
	c.Raw(func(driverConn any) error {
		conn := driverConn.(*sqlite3.SQLiteConn)
		isAuth = conn.AuthEnabled()
		return nil
	})
	return isAuth
}

func (dbc *DBConn) Exec(query string, args ...any) (sql.Result, error) {
	if dbc.DB == nil {
		return nil, ErrDBNotOpen
	}
	dbc.Lock()
	defer dbc.Unlock()
	return dbc.DB.Exec(query, args...)
}

func (dbc *DBConn) Query(query string, args ...any) (*sql.Rows, error) {
	if dbc.DB == nil {
		return nil, ErrDBNotOpen
	}
	dbc.RLock()
	defer dbc.RUnlock()
	return dbc.QueryContext(context.Background(), query, args...)
}

func (dbc *DBConn) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if dbc.DB == nil {
		return nil, ErrDBNotOpen
	}
	dbc.RLock()
	defer dbc.RUnlock()
	return dbc.DB.QueryContext(ctx, query, args...)
}

func (dbc *DBConn) QueryRow(query string, args ...any) *sql.Row {
	if dbc.DB == nil {
		return nil
	}
	dbc.RLock()
	defer dbc.RUnlock()
	return dbc.DB.QueryRow(query, args...)
}

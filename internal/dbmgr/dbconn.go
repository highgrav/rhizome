package dbmgr

import (
	"context"
	"database/sql"
	"github.com/google/deck"
	"github.com/highgrav/rhizome/internal/constants"
	sqlite3 "github.com/mattn/go-sqlite3"
	"os"
	"sync"
	"time"
)

type DBConn struct {
	sync.RWMutex
	LastAccessed  time.Time
	ID            string
	DB            *sql.DB
	Mgr           *DBManager
	Grp           *DBConnGroup
	driver        *sqlite3.SQLiteDriver
	opts          DBConnOptions
	fnGet         FnGetFilenameFromID
	User          string
	PendingDelete bool
}

func OpenOrCreateDBConn(mgr *DBManager, grp *DBConnGroup, driver *sqlite3.SQLiteDriver, id string, fnGet FnGetFilenameFromID, fnCreate FnCreateNewDB, opts DBConnOptions) (*DBConn, error) {

	filepath, err := fnGet(id)
	if err != nil {
		return nil, err
	}
	connstr := "file:" + filepath + opts.ConnstrOpts("rw")

	db, err := sql.Open(constants.DBDriverName, connstr)

	if err != nil || db.Ping() != nil {
		// try to create the DB if necessary
		err2 := fnCreate(id, opts)
		if err2 != nil {
			return nil, err2
		}
		db, err = sql.Open(constants.DBDriverName, connstr)
		if err != nil {
			return nil, err
		}
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}
	if mgr != nil {
		mgr.UpdateStat(constants.StatOpenDbs, 1)
	}

	dbc := &DBConn{
		Mgr:           mgr,
		Grp:           grp,
		LastAccessed:  time.Now(),
		PendingDelete: false,
		ID:            id,
		DB:            db,
		driver:        driver,
		opts:          opts,
		fnGet:         fnGet,
	}

	return dbc, nil
}

func OpenDBConn(mgr *DBManager, grp *DBConnGroup, driver *sqlite3.SQLiteDriver, id string, fnGet FnGetFilenameFromID, opts DBConnOptions) (*DBConn, error) {
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

	connstr := "file:" + filepath + opts.ConnstrOpts("rw")
	db, err := sql.Open(constants.DBDriverName, connstr)

	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}
	if mgr != nil {
		mgr.UpdateStat(constants.StatOpenDbs, 1)
	}

	dbc := &DBConn{
		Mgr:           mgr,
		Grp:           grp,
		LastAccessed:  time.Now(),
		PendingDelete: false,
		ID:            id,
		DB:            db,
		driver:        driver,
		opts:          opts,
		fnGet:         fnGet,
	}
	return dbc, nil
}

func (dbc *DBConn) Authorize(username, pwd, db string) bool {
	if dbc.Mgr.Cfg.FnCheckDBAccess == nil {
		return true
	}
	v, err := dbc.Mgr.Cfg.FnCheckDBAccess(username, pwd, db)
	if err != nil {
		// TODO -- log this
		return false
	}
	return v
}

// TODO -- no longer needed once we're separating connections
func (dbc *DBConn) Reopen() error {
	dbc.Lock()
	defer dbc.Unlock()
	filepath, err := dbc.fnGet(dbc.ID)
	if err != nil {
		return err
	}
	fst, err := os.Stat(filepath)
	if err != nil {
		return err
	}
	if fst.IsDir() {
		return ErrCouldNotOpenFile
	}

	connstr := "file:" + filepath + dbc.opts.ConnstrOpts("rw")
	db, err := sql.Open(constants.DBDriverName, connstr)

	if err != nil {
		return err
	}

	err = db.Ping()
	if err != nil {
		return err
	}
	if dbc.Mgr != nil {
		dbc.Mgr.UpdateStat(constants.StatOpenDbs, 1)
	}
	dbc.LastAccessed = time.Now()
	dbc.PendingDelete = false

	if dbc.Mgr != nil {
		err := dbc.Mgr.AddConn(dbc.ID, dbc)
		if err != nil {
			return err
		}
	}
	if err != nil {
		dbc.DB = nil
		return err
	}
	dbc.DB = db

	return nil
}

func (dbc *DBConn) Conn(ctx context.Context) (*sql.Conn, error) {
	return dbc.DB.Conn(ctx)
}

func (dbc *DBConn) Ping() error {
	if dbc.DB == nil {
		return ErrDBNotOpen
	}
	return dbc.DB.Ping()
}

func (dbc *DBConn) Close() {
	if dbc.DB == nil {
		dbc.PendingDelete = true
		return
	}
	dbc.Lock()
	defer dbc.Unlock()

	err := dbc.DB.Close()
	if err == nil && dbc.Mgr != nil {
		dbc.Mgr.UpdateStat(constants.StatOpenDbs, -1)
	} else {
		dbc.PendingDelete = true
		deck.Errorf("error closing db " + dbc.ID + " (this may not be a problem)")
	}
	dbc.PendingDelete = true
	dbc.DB = nil
}

func (dbc *DBConn) AuthEnabled() bool {
	if dbc.DB == nil {
		err := dbc.Reopen()
		if err != nil {
			return false
		}
	}
	c, err := dbc.DB.Conn(context.Background())
	if err != nil {
		return false
	}
	defer func() {
		_ = c.Close()
	}()
	var isAuth bool
	_ = c.Raw(func(driverConn any) error {
		conn := driverConn.(*sqlite3.SQLiteConn)
		isAuth = conn.AuthEnabled()
		return nil
	})
	return isAuth
}

func (dbc *DBConn) Exec(query string, args ...any) (sql.Result, error) {
	if dbc.DB == nil {
		err := dbc.Reopen()
		if err != nil {
			return nil, err
		}
	}
	dbc.Lock()
	defer dbc.Unlock()
	dbc.LastAccessed = time.Now()
	dbc.PendingDelete = false

	r, err := dbc.DB.ExecContext(context.Background(), query, args...)
	if err != nil {
		deck.Errorf("failed exec()ing query %q on db %q: %q", query, dbc.ID, err.Error())
		return nil, err
	}
	return r, err
}

func (dbc *DBConn) Query(query string, args ...any) (*sql.Rows, error) {
	return dbc.QueryContext(context.Background(), query, args...)
}

func (dbc *DBConn) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if dbc.Mgr.Cfg.LogLevel >= constants.LogLevelDebug {
		deck.Infof("running query %q on db %s", query, dbc.ID)
	}
	if dbc.DB == nil {
		err := dbc.Reopen()
		if err != nil {
			deck.Errorf("failed reopening db %s: %q", dbc.ID, err.Error())
			return nil, err
		}
	}
	dbc.RLock()
	defer dbc.RUnlock()
	dbc.LastAccessed = time.Now()
	dbc.PendingDelete = false

	r, err := dbc.DB.QueryContext(ctx, query, args...)
	if err != nil {
		deck.Errorf("failed query %q on db %s: %q", query, dbc.ID, err.Error())
		return nil, err
	}
	return r, nil
}

func (dbc *DBConn) QueryRow(query string, args ...any) (*sql.Row, error) {
	return dbc.QueryRowContext(context.Background(), query, args...)
}

func (dbc *DBConn) QueryRowContext(ctx context.Context, query string, args ...any) (*sql.Row, error) {
	if dbc.DB == nil {
		err := dbc.Reopen()
		if err != nil {
			deck.Errorf("failed to reopen db %s: %q", dbc.ID, err.Error())
			return nil, err
		}
	}
	dbc.RLock()
	defer dbc.RUnlock()
	dbc.LastAccessed = time.Now()
	dbc.PendingDelete = false

	row := dbc.DB.QueryRowContext(ctx, query, args...)
	return row, nil
}

package dbmgr

import "github.com/mattn/go-sqlite3"

func (dbc *DBConn) mockPg(conn *sqlite3.SQLiteConn) error {
	return nil
}

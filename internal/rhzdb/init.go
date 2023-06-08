package rhzdb

import (
	"database/sql"
	"fmt"
	"github.com/mattn/go-sqlite3"
)

const DBDriverName string = "rhizome-db"

/*
Register a new sql.* driver so we can add in additional hooks.
*/
func init() {
	sql.Register(DBDriverName, &sqlite3.SQLiteDriver{
		Extensions: nil,
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			if err := conn.RegisterFunc("version", pgFnVersion, true); err != nil {
				return fmt.Errorf("cannot register version() function: " + err.Error())
			}
			return nil
		},
	})
}

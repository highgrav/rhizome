package rhizome

import (
	"database/sql"
	"fmt"
	"github.com/highgrav/rhizome/internal/constants"
	"github.com/mattn/go-sqlite3"
)

func Init(cfg RhizomeConfig) {
	for _, v := range sql.Drivers() {
		if v == constants.DBDriverName {
			return
		}
	}
	sql.Register(constants.DBDriverName, &sqlite3.SQLiteDriver{
		Extensions: nil,
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {

			if cfg.Aggregators != nil && len(cfg.Aggregators) > 0 {
				for _, v := range cfg.Aggregators {
					if v.Name != "" && v.Fn != nil {
						if err := conn.RegisterAggregator(v.Name, v.Fn, v.IsPure); err != nil {
							return fmt.Errorf("cannot register aggregation function: " + err.Error())
						}
					}
				}
			}

			if cfg.Collators != nil && len(cfg.Collators) > 0 {
				for _, v := range cfg.Collators {
					if v.Name != "" && v.Fn != nil {
						if err := conn.RegisterCollation(v.Name, v.Fn); err != nil {
							return fmt.Errorf("cannot register collation function: " + err.Error())
						}
					}
				}
			}

			if cfg.CustomFns != nil && len(cfg.CustomFns) > 0 {
				for _, v := range cfg.CustomFns {
					if v.Name != "" && v.Fn != nil {
						if err := conn.RegisterFunc(v.Name, v.Fn, v.IsPure); err != nil {
							return fmt.Errorf("cannot register custom function: " + err.Error())
						}
					}
				}
			}

			if cfg.Authorizer != nil {
				conn.RegisterAuthorizer(cfg.Authorizer)
			}
			if cfg.CommitHook != nil {
				conn.RegisterCommitHook(cfg.CommitHook)
			}
			if cfg.PreUpdateHook != nil {
				conn.RegisterPreUpdateHook(cfg.PreUpdateHook)
			}
			if cfg.RollbackHook != nil {
				conn.RegisterRollbackHook(cfg.RollbackHook)
			}
			if cfg.UpdateHook != nil {
				conn.RegisterUpdateHook(cfg.UpdateHook)
			}
			return nil
		},
	})
}

package rhizome

import (
	"context"
	"github.com/highgrav/rhizome/internal/dbmgr"
	"github.com/highgrav/rhizome/internal/pgif"
	"net"
)

func NewDBManager(cfg dbmgr.DBManagerConfig, defaultOpts dbmgr.DBConnOptions) *dbmgr.DBManager {
	return dbmgr.NewDBManager(cfg, defaultOpts)
}

func NewRhizomeBackend(ctx context.Context, conn net.Conn, db *dbmgr.DBManager, cfg pgif.BackendConfig) *pgif.RhizomeBackend {
	return pgif.NewRhizomeBackend(ctx, conn, db, cfg)
}

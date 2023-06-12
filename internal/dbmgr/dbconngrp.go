package dbmgr

import (
	"github.com/google/deck"
	"sync"
	"time"
)

type DBConnGroup struct {
	sync.Mutex
	ID    string
	Conns []*DBConn
}

func NewDBConnGroup(id string) (*DBConnGroup, error) {
	// TODO -- add some sanity checks on database accessibility so you can't DDOS the system by adding a bunch of spurious and invalid DB entries
	return &DBConnGroup{
		Mutex: sync.Mutex{},
		ID:    id,
		Conns: make([]*DBConn, 0),
	}, nil
}

func (grp *DBConnGroup) AddConn(conn *DBConn) error {
	return nil
}

func (grp *DBConnGroup) NewConn(dbm *DBManager, opts DBConnOptions) (*DBConn, error) {
	db, err := OpenDBConn(dbm, grp, dbm.Driver, grp.ID, dbm.GetFilename, opts)
	if err != nil {
		deck.Errorf("failed to open database %s: %s", grp.ID, err.Error())
		return nil, err
	}
	grp.Lock()
	defer grp.Unlock()
	grp.Conns = append(grp.Conns, db)
	return db, nil
}

func (grp *DBConnGroup) CloseConn(*DBConn) error {
	return nil
}

func (grp *DBConnGroup) Sweep(timeout time.Duration) {
	toDelete := 0
	for _, v := range grp.Conns {
		if v.PendingDelete || v.LastAccessed.Before(time.Now().Add(-1*timeout)) {
			v.PendingDelete = true
			toDelete++
		}
	}
	if toDelete == 0 {
		return
	}
	grp.Lock()
	defer grp.Unlock()
	deleted := 0
	for i := len(grp.Conns) - 1; i >= 0; i-- {
		conn := grp.Conns[i]
		if conn.PendingDelete {
			grp.Conns = append(grp.Conns[:i], grp.Conns[i+1:]...)
			deleted++
		}
	}
	deck.Infof("%s: marked %d, swept %d, open %d", grp.ID, toDelete, deleted, len(grp.Conns))

}

func (grp *DBConnGroup) Close() {

}

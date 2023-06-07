package dbmgr

import "time"

type FnGetFilenameFromID func(id string) (string, error)
type FnCreateNewDB func(id string, opts DBConnOptions) error

type FnAddUser func(username, pwd string) error
type FnDeleteUser func(username string) error
type FnModifyUser func(username, action, db string, args ...any)

type DBManagerConfig struct {
	BaseDir        string
	MaxDBsOpen     int
	LogDbOpenClose bool
	MaxIdleTime    time.Duration
	SweepEach      time.Duration
	CheckpointEach time.Duration
	UseWAL         bool
	FnGetDB        FnGetFilenameFromID
	FnNewDB        FnCreateNewDB
	FnAddUser      FnAddUser
	FnDeleteUser   FnDeleteUser
	FnModifyUser   FnModifyUser
}

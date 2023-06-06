package dbmgr

import "time"

type DBManagerConfig struct {
	BaseDir        string
	MaxDBsOpen     int
	MaxIdleTime    time.Duration
	SweepEach      time.Duration
	CheckpointEach time.Duration
	UseWAL         bool
}

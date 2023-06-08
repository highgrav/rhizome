package dbmgr

import "errors"

var ErrDBFileNotFound = errors.New("file not found")
var ErrCouldNotOpenFile = errors.New("could not open file")
var ErrDBNotOpen = errors.New("db not opened")
var ErrCriticalCannotOpen = errors.New("critical error, cannot open database")
var ErrDBDoesNotExist = errors.New("db does not exist")
var ErrWrongDBServer = errors.New("db doesn not exist on this server")
var ErrTooManyDBsOpen = errors.New("cannot open db: too many connections")

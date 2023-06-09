package rhizome

import "github.com/mattn/go-sqlite3"

type ISqlAggregator interface {
}

type CustomAggregator struct {
	Name   string
	Fn     ISqlAggregator
	IsPure bool
}

type FnCustomCollator func(string, string) int

type CustomCollator struct {
	Name string
	Fn   FnCustomCollator
}

type FnCommitHook func() int

type CustomFunction struct {
	Name   string
	Fn     interface{}
	IsPure bool
}

type FnPreUpdateHook func(data sqlite3.SQLitePreUpdateData)
type FnRollbackHook func()
type FnUpdateHook func(int, string, string, int64)
type FnCustomAuthorizer func(int, string, string, string) int

type RhizomeConfig struct {
	Aggregators   []CustomAggregator
	Collators     []CustomCollator
	Authorizer    FnCustomAuthorizer
	CommitHook    FnCommitHook
	PreUpdateHook FnPreUpdateHook
	RollbackHook  FnRollbackHook
	UpdateHook    FnUpdateHook
	CustomFns     []CustomFunction
}

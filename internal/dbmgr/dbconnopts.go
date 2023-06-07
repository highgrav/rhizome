package dbmgr

import (
	"strconv"
	"strings"
)

type DBConnOptions struct {
	UseJModeWAL      bool `opt:"_journal_mode=WAL"`
	UseJModeTruncate bool `opt:"_journal_mode=TRUNCATE"`
	UseJModePersist  bool `opt:"_journal_mode=PERSIST"`
	UseJModeMemory   bool `opt:"_journal_mode=MEMORY"`
	UseJModeOff      bool `opt:"_journal_mode=OFF"`

	CacheShared bool `opt:"cache=shared"`

	SecureDelete     bool `opt:"_secure_delete=on"`
	SecureDeleteFast bool `opt:"_secure_delete=FAST"`

	TxImmediate bool `opt:"_txlock=immediate"`
	TxDeferred  bool `opt:"_txlock=deferred"`
	TxExclusive bool `opt:"_txlock=exclusive"`

	AutoVacuumFull        bool `opt:"_auto_vacuum"`
	AutoVacuumIncremental bool

	SyncExtra bool `opt:"_sync=extra"`
	SyncFull  bool
	SyncOff   bool

	LockModeExclusive      bool `opt:"_locking_mode=EXCLUSIVE""`
	CaseSensitiveLike      bool `opt:"_case_sensitive_like"`
	ForeignKeys            bool `opt:"_foreign_keys"`
	IgnoreCheckConstraints bool `opt:"_ignore_check_constraints"`
	Immutable              bool `opt:"immutable"`
	CacheSize              int  `opt:"_cache_size"`
}

func (opts *DBConnOptions) ConnstrOpts(mode string) string {
	optlist := make([]string, 0)
	optlist = append(optlist, "_mutex=full")

	//Open Mode
	if mode != "ro" && mode != "rw" && mode != "rwc" && mode != "memory" {
		// default
		optlist = append(optlist, "mode=rw")
	} else {
		optlist = append(optlist, "mode="+mode)
	}

	// Journaling Mode
	if opts.UseJModeOff {
		optlist = append(optlist, "_journal=OFF")
	} else if opts.UseJModeWAL {
		optlist = append(optlist, "_journal=WAL")
	} else if opts.UseJModeMemory {
		optlist = append(optlist, "_journal=MEMORY")
	} else if opts.UseJModePersist {
		optlist = append(optlist, "_journal=PERSIST")
	} else if opts.UseJModeTruncate {
		optlist = append(optlist, "_journal=TRUNCATE")
	} else {
		// default
		optlist = append(optlist, "_journal=DELETE")
	}

	// Cache
	if opts.CacheShared {
		optlist = append(optlist, "cache=shared")
	} else {
		optlist = append(optlist, "cache=private")
	}

	// Secure Delete
	if opts.SecureDelete {
		optlist = append(optlist, "_secure_delete=true")
	} else if opts.SecureDeleteFast {
		optlist = append(optlist, "_secure_delete=FAST")
	} else {
		optlist = append(optlist, "_secure_delete=false")
	}

	// Txs
	if opts.TxExclusive {
		optlist = append(optlist, "_txlock=exclusive")
	} else if opts.TxImmediate {
		optlist = append(optlist, "_txlock=immediate")
	} else if opts.TxDeferred {
		optlist = append(optlist, "_txlock=deferred")
	}

	// Vacuum
	if opts.AutoVacuumIncremental {
		optlist = append(optlist, "_vacuum=incremental")
	} else if opts.AutoVacuumFull {
		optlist = append(optlist, "_vacuum=full")
	} else {
		optlist = append(optlist, "_vacuum=none")
	}

	// sync
	if opts.SyncExtra {
		optlist = append(optlist, "_sync=extra")
	} else if opts.SyncFull {
		optlist = append(optlist, "_sync=full")
	} else if opts.SyncOff {
		optlist = append(optlist, "_sync=off")
	} else {
		optlist = append(optlist, "_sync=normal")
	}

	if opts.LockModeExclusive {
		optlist = append(optlist, "_locking=exclusive")
	} else {
		optlist = append(optlist, "_locking=normal")
	}

	if opts.CaseSensitiveLike {
		optlist = append(optlist, "_cslike=true")
	} else {
		optlist = append(optlist, "_cslike=false")
	}

	if opts.ForeignKeys {
		optlist = append(optlist, "_fk=true")
	} else {
		optlist = append(optlist, "_fk=false")
	}

	if opts.IgnoreCheckConstraints {
		optlist = append(optlist, "_ignore_check_constraints=true")
	} else {
		optlist = append(optlist, "_ignore_check_constraints=false")
	}

	if opts.Immutable {
		optlist = append(optlist, "immutable=true")
	}

	if opts.CacheSize > 0 {
		optlist = append(optlist, "_cache_size="+strconv.FormatInt(int64(opts.CacheSize), 10))
	}

	return "?" + strings.Join(optlist, "&")
}

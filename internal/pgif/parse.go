package pgif

/*
These are definitions for basic meta-DDL parsing. Meta-DDL are statements within '[['...']]' tags, such as:
[[CREATE DATABASE 'MyDatabase' AS 'My Personal Database' WITH NOCONFLICT;]]
[[CREATE USER 'bob@example.com' WITH PWD 'some-password';]]
[[ADD USER 'bob@example.com' TO DB 'MyDatabase';]]
[[REMOVE USER ' jane@example.com' FROM DB 'MyDatabase';]]
[[DELETE USER 'jane@example.com';]]
[[ADD RIGHT 'db::admin' TO 'bob@example.com' ON DB 'MyDatabase';]]
*/

type DdlStmt struct {
}

type DdlStmtCreateDB struct {
}

type DdlStmtDropDB struct {
}

type DdlStmtAddUser struct {
}

type DdlStmtUpdateUser struct {
}

type DdlStmtDeleteUser struct {
}

type DdlStmtAddUserRight struct {
}

type DdlStmtRemoveUserRight struct {
}

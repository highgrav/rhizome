package pgif

import (
	"github.com/alecthomas/participle/v2/lexer"
)

/*
These are definitions for basic meta-DDL parsing. Meta-DDL are statements within '[['...']]' tags, such as:
[[CREATE DATABASE 'MyDatabase' AS 'My Personal Database' WITH NOCONFLICT;]]
[[CREATE USER 'bob@example.com' WITH PWD 'some-password';]]
[[ADD USER 'bob@example.com' TO DB 'MyDatabase';]]
[[REMOVE USER ' jane@example.com' FROM DB 'MyDatabase';]]
[[DELETE USER 'jane@example.com';]]
[[ADD RIGHT 'db::admin' TO 'bob@example.com' ON DB 'MyDatabase';]]
*/

type Boolean bool

func (b *Boolean) Capture(values []string) error {
	*b = values[0] == "TRUE"
	return nil
}

var ddlLexer = lexer.MustSimple([]lexer.SimpleRule{
	{`Keyword`, `(?i)\b([[|]]|CREATE|DATABASE|AS|WITH|NOCONFLICT|USER|ADD|RENOVE|DELETE|FROM|DB|RIGHT|TO|ON)\b`},
	{`Ident`, `[a-zA-Z_][a-zA-Z0-9_]*`},
	{`Number`, `[-+]?\d*\.?\d+([eE][-+]?\d+)?`},
	{`String`, `'[^']*'|"[^"]*"`},
	{`Operators`, `<>|!=|<=|>=|[-+*/%,.()=<>]`},
	{"whitespace", `\s+`},
})

/*
var DdlParser = participle.MustBuild[Ddl](
	participle.Lexer(ddlLexer),
	participle.Unquote("String"),
	participle.CaseInsensitive("Keyword"))
*/
type Ddl struct {
}

type DdlValue struct {
	Number  *float64 ` | @Number`
	String  *string  ` | @String`
	Boolean *Boolean ` | @("TRUE" | "FALSE")`
}

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

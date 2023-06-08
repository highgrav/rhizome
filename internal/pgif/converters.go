package pgif

import (
	"database/sql"
	"fmt"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	"strings"
)

/*
Helper function to convert Sqlite types. Note that we only support bigint, 64-bit floats, text, and bytea fields.
// TODO -- support bool and datetime as derived data types?
Notes:
  - Dates are sent back in CCYY-MM-DD with a column length of 4
  - Timestamps are sent back as CCYY-MM-DD HH:mm:SS-zz (e.g., 2004-10-19 03:23:54-05) with a column length of 8
  - Booleans are 't' or 'f' with a column length of 1
*/
func getPgTypeFromSqliteType(col *sql.ColumnType) uint32 {
	strn := strings.ToLower(col.DatabaseTypeName())
	if strn == "integer" || strn == "int" || strn == "tinyint" || strn == "smallint" || strn == "mediumint" || strn == "bigint" || strn == "unsigned big int" || strn == "int2" || strn == "int8" {
		return pgtype.Int8OID
	} else if strn == "float" || strn == "real" || strn == "double" || strn == "double precision" || strings.HasPrefix(strn, "decimal") {
		return pgtype.Float8OID
	} else if strn == "blob" {
		return pgtype.ByteaOID
	} else if strn == "datetime" {
		return pgtype.TimestamptzOID
	} else if strn == "boolean" {
		return pgtype.BoolOID
	} else if strn == "date" {
		return pgtype.DateOID
	}
	return pgtype.TextOID
}

func convertRowsToPgRows(rows *sql.Rows, cols []*sql.ColumnType) ([]*pgproto3.DataRow, error) {
	datarows := make([]*pgproto3.DataRow, 0)

	for rows.Next() {
		refs := make([]any, len(cols))
		vals := make([]any, len(cols))
		for i, _ := range refs {
			refs[i] = &vals[i]
		}
		err := rows.Scan(refs...)
		if err != nil {
			return datarows, err
		}
		pgrow := pgproto3.DataRow{
			Values: make([][]byte, len(vals)),
		}
		for i, _ := range vals {
			if vals[i] == nil {
				pgrow.Values[i] = nil
				continue
			}
			pgtyp := getPgTypeFromSqliteType(cols[i])
			if pgtyp == pgtype.Int8OID {
				v := vals[i].(int64)
				pgrow.Values[i] = []byte(fmt.Sprintf("%d", v))
			} else if pgtyp == pgtype.Float8OID {
				v := vals[i].(float64)
				pgrow.Values[i] = []byte(fmt.Sprintf("%f", v))
			} else if pgtyp == pgtype.ByteaOID {
				pgrow.Values[i] = []byte(fmt.Sprint(vals[i]))
			} else {
				pgrow.Values[i] = []byte(fmt.Sprint(vals[i]))
			}
		}
		datarows = append(datarows, &pgrow)
	}
	return datarows, nil
}

func convertColTypesToPgRowDescriptions(cols []*sql.ColumnType) *pgproto3.RowDescription {
	descs := &pgproto3.RowDescription{}
	for _, col := range cols {
		typ := getPgTypeFromSqliteType(col)
		fd := pgproto3.FieldDescription{
			Name:                 []byte(col.Name()),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          typ,
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0,
		}
		if typ == pgtype.Int8OID || typ == pgtype.Float8OID {
			fd.DataTypeSize = 8
			fd.Format = 0
		} else if typ == pgtype.ByteaOID {
			fd.Format = 1
		}

		descs.Fields = append(descs.Fields, fd)
	}
	return descs
}

package pgif

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"fmt"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	"math"
	"strconv"
	"strings"
)

/*
Helper function to convert Sqlite types. Note that we only support bigint, 64-bit floats, text, and bytea fields.
NOTE: THIS IS PROVISIONAL! This assumes that Sqlite is returning the "storage class" (https://www.sqlite.org/datatype3.html)
and not the "record format" (https://www.sqlite.org/fileformat2.html#record_format).
*/
func getPgTypeFromSqliteType(col *sql.ColumnType) uint32 {
	if strings.ToLower(col.DatabaseTypeName()) == "integer" {
		return pgtype.Int8OID
	} else if strings.ToLower(col.DatabaseTypeName()) == "real" {
		return pgtype.Float8OID
	} else if strings.ToLower(col.DatabaseTypeName()) == "blob" {
		return pgtype.ByteaOID
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
			pgtyp := getPgTypeFromSqliteType(cols[i])
			if pgtyp == pgtype.Int8OID {
				v := fmt.Sprint(vals[i])
				// TODO -- this is going to bottleneck performance
				vi, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					return datarows, err
				}
				buf := &bytes.Buffer{}
				binary.Write(buf, binary.BigEndian, vi)
				pgrow.Values[i] = buf.Bytes()
			} else if pgtyp == pgtype.Float8OID {
				v := vals[i].(float64)
				var buf []byte = make([]byte, 8)
				n := math.Float64bits(v)
				buf[0] = byte(n >> 56)
				buf[1] = byte(n >> 48)
				buf[2] = byte(n >> 40)
				buf[3] = byte(n >> 32)
				buf[4] = byte(n >> 24)
				buf[5] = byte(n >> 16)
				buf[6] = byte(n >> 8)
				buf[7] = byte(n)
				pgrow.Values[i] = buf
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
		fd := pgproto3.FieldDescription{
			Name:                 []byte(col.Name()),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          getPgTypeFromSqliteType(col),
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0,
		}

		descs.Fields = append(descs.Fields, fd)
	}
	return descs
}

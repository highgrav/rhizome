package pgif

import (
	"context"
	"errors"
	"fmt"
	"github.com/highgrav/rhizome/internal/dbmgr"
	"github.com/jackc/pgproto3/v2"
	"io"
	"net"
	"strings"
)

type RhizomeBackend struct {
	ctx     context.Context
	backend *pgproto3.Backend
	conn    net.Conn
	dbmgr   *dbmgr.DBManager
	db      *dbmgr.DBConn
	logMsgs bool
}

func NewRhizomeBackend(ctx context.Context, conn net.Conn, db *dbmgr.DBManager, logMsgs bool) *RhizomeBackend {
	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)

	handler := &RhizomeBackend{
		ctx:     ctx,
		backend: backend,
		conn:    conn,
		dbmgr:   db,
		logMsgs: logMsgs,
	}
	return handler
}

func (rz *RhizomeBackend) start() error {
	startMsg, err := rz.backend.ReceiveStartupMessage()
	if err != nil {
		return err
	}
	switch startMsg := startMsg.(type) {
	case *pgproto3.SSLRequest:
		// TODO -- right now we don't handle SSL connections
		_, err := rz.conn.Write([]byte("N"))
		if err != nil {
			return err
		}
		return rz.start()
	case *pgproto3.StartupMessage:
		dbname, ok := startMsg.Parameters["database"]
		if !ok {
			return errors.New("missing database name")
		}
		sqlconn, err := rz.dbmgr.Get(dbname)
		if err != nil {
			return err
		}
		rz.db = sqlconn
		buf := (&pgproto3.AuthenticationOk{}).Encode(nil)
		buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
		_, err = rz.conn.Write(buf)
		if err != nil {
			return fmt.Errorf("error sending ready for query: %w", err)
		}
	default:
		return fmt.Errorf("unknown pg startup msg: %#v", startMsg)
	}
	return nil
}

func (rz *RhizomeBackend) Run() error {
	defer rz.close()
	err := rz.start()
	if err != nil {
		return err
	}
	for {
		// process messages
		msg, err := rz.backend.Receive()
		if err != nil {
			return err
		}
		switch msg := msg.(type) {
		case *pgproto3.Bind:
			return errors.New("received unsupported bind request")
		case *pgproto3.CancelRequest:
			return errors.New("received unsupported cancel request")
		case *pgproto3.Close:
			return errors.New("received unsupported close request")
		case *pgproto3.CopyData:
			return errors.New("received unsupported copy request")
		case *pgproto3.CopyDone:
			return errors.New("received unsupported copy done request")
		case *pgproto3.CopyFail:
			return errors.New("received unsupported copy fail request")
		case *pgproto3.Describe:
			return errors.New("received unsupported describe request")
		case *pgproto3.Execute:
			return errors.New("received unsupported execute request")
		case *pgproto3.Flush:
			return errors.New("received unsupported flush statement")
		case *pgproto3.FunctionCall:
			return errors.New("received unsupported function call request")
		case *pgproto3.GSSEncRequest:
			return errors.New("received unsupported gssenc request")
		case *pgproto3.PasswordMessage:
			return errors.New("received unsupported password message request")
		case *pgproto3.SASLInitialResponse:
			return errors.New("received unsupported sasl initial response request")
		case *pgproto3.SASLResponse:
			return errors.New("received unsupported sasl response request")
		case *pgproto3.SSLRequest:
			return errors.New("received unsupported ssl request (out of sequence)")
		case *pgproto3.StartupMessage:
			return errors.New("received unsupported startup request (out of sequence)")
		case *pgproto3.Sync:
			// we can ignore these
			continue
		case *pgproto3.Terminate:
			// exit
			return nil
		case *pgproto3.Parse:
			if err := rz.handleParse(msg); err != nil {
				return err
			}
		case *pgproto3.Query:
			if err := rz.handleQuery(msg); err != nil {
				return err
			}
		default:
			return fmt.Errorf("received message other than Query from client: %#v", msg)
		}
	}
	return nil
}

func (rz *RhizomeBackend) close() error {
	return rz.conn.Close()
}

func (rz *RhizomeBackend) handleQuery(msg *pgproto3.Query) error {
	if rz.logMsgs {
		// TODO -- convert to deck logging
		fmt.Printf("handling query %q\n", msg.String)
	}
	if strings.HasPrefix(strings.TrimSpace(msg.String), "[[") {
		if rz.logMsgs {
			fmt.Printf("detected MetaDDL, rerouting...")
			// TODO -- route meta-DDL here (for now just ACK and move on)
			return writePgMsgs(rz.conn,
				&pgproto3.ReadyForQuery{TxStatus: 'E'},
			)
		}
	}

	// Validation check to make sure the database is open
	if rz.db == nil {
		return ErrDBNotOpen
	}

	// Run the query and check for errors
	rows, err := rz.db.QueryContext(rz.ctx, msg.String)
	if err != nil {
		return writePgMsgs(rz.conn,
			&pgproto3.ErrorResponse{Message: err.Error()},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		)
	}
	defer rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}

	// translate the Sqlite response to something PG clients can understand
	// Convert col descriptions
	cols, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	buf := convertColTypesToPgRowDescriptions(cols).Encode(nil)

	// Convert rows
	pgrows, err := convertRowsToPgRows(rows, cols)
	if err != nil {
		return err
	}
	for _, pgrow := range pgrows {
		buf = pgrow.Encode(buf)
	}

	// Mark command complete and ready for next query.
	buf = (&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")}).Encode(buf)
	buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)

	_, err = rz.conn.Write(buf)
	return nil
}

func (rz *RhizomeBackend) handleParse(msg *pgproto3.Parse) error {
	return nil
}

func writePgMsgs(w io.Writer, msgs ...pgproto3.Message) error {
	var buf []byte
	for _, msg := range msgs {
		buf = msg.Encode(buf)
	}
	_, err := w.Write(buf)
	return err
}

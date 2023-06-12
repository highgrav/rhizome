package pgif

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"github.com/google/deck"
	"github.com/highgrav/rhizome/internal/constants"
	"github.com/highgrav/rhizome/internal/dbmgr"
	"github.com/jackc/pgproto3/v2"
	"io"
	"net"
	"path"
	"strings"
)

type RhizomeBackend struct {
	ctx     context.Context
	backend *pgproto3.Backend
	sqlconn *sql.Conn
	conn    net.Conn
	dbmgr   *dbmgr.DBManager
	db      *dbmgr.DBConn
	stmts   map[string]*RhizomePreparedStatement
	portals map[string]*RhizomePortal
	cfg     *BackendConfig
}

type RhizomePreparedStatement struct {
	ID           string
	Stmt         string
	PreparedStmt *sql.Stmt
	ParamOIDs    []uint32
}

type RhizomePortal struct {
	ID                          string
	StmtID                      string
	ParamsUseBinaryFormatting   []bool
	ResultsUserBinaryFormatting []bool
	Params                      []any
	Cfg                         BackendConfig
}

func NewRhizomeBackend(ctx context.Context, conn net.Conn, db *dbmgr.DBManager, cfg BackendConfig) *RhizomeBackend {
	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)
	handler := &RhizomeBackend{
		ctx:     ctx,
		backend: backend,
		conn:    conn,
		dbmgr:   db,
		cfg:     &cfg,
		stmts:   make(map[string]*RhizomePreparedStatement),
		portals: make(map[string]*RhizomePortal),
	}
	return handler
}

func (rz *RhizomeBackend) upgradeToTLS() error {
	if rz.cfg.UseTLS == false || rz.cfg.TLSKeyName == "" || rz.cfg.TLSCertName == "" {
		_, err := rz.conn.Write([]byte("N"))
		if err != nil {
			deck.Errorf("error upgrading tls: %s", err.Error())
			return err
		}
		return nil
	}
	if rz.cfg.TLS == nil {
		cert, err := tls.LoadX509KeyPair(path.Join(rz.cfg.TLSCertDir, rz.cfg.TLSCertName), path.Join(rz.cfg.TLSCertDir, rz.cfg.TLSKeyName))
		if err != nil {
			deck.Errorf("failed to load TLS: %q", err.Error())
			return errors.New("could not load tls: " + err.Error())
		}
		rz.cfg.TLS = &tls.Config{
			Certificates: []tls.Certificate{cert},
			ClientAuth:   tls.VerifyClientCertIfGiven,
			ServerName:   rz.cfg.ServerName,
		}
		if rz.cfg.TLS.ServerName == "" {
			rz.cfg.TLS.ServerName = "localhost"
		}
	}
	var sslConn *tls.Conn
	sslConn = tls.Server(rz.conn, rz.cfg.TLS)
	if sslConn == nil {
		return errors.New("could not upgrade to ssl")
	}
	// let the client know we're ready to upgrade
	_, err := rz.conn.Write([]byte("S"))
	if err != nil {
		deck.Errorf("failed to write pg tls acknowledgement: %s", err.Error())
		return err
	}
	err = sslConn.Handshake()
	if err != nil {
		deck.Errorf("failed to tls handshake: %s", err.Error())
		return err
	}
	rz.conn = net.Conn(sslConn)
	rz.backend = pgproto3.NewBackend(pgproto3.NewChunkReader(rz.conn), rz.conn)
	return nil
}

func (rz *RhizomeBackend) processStart() error {
	startMsg, err := rz.backend.ReceiveStartupMessage()
	if err != nil {
		return err
	}
	switch startMsg := startMsg.(type) {
	case *pgproto3.SSLRequest:
		if rz.cfg.LogLevel >= constants.LogLevelDebug {
			deck.Infof("Detected FE SSLRequest msg during startup: %+v\n", startMsg)
		}
		err := rz.upgradeToTLS()
		if err != nil {
			return err
		}
		// if we got this before startup, then keep processing -- this should be the default
		if rz.db == nil {
			return rz.processStart()
		}
		return nil
	case *pgproto3.StartupMessage:
		var dbconn *dbmgr.DBConn
		var err error = nil
		if rz.cfg.LogLevel >= constants.LogLevelDebug {
			deck.Infof("Detected FE Startup msg: %+v\n", startMsg)
		}
		dbname, ok := startMsg.Parameters["database"]
		if !ok {
			return errors.New("missing database name")
		}
		username, ok := startMsg.Parameters["user"]
		if !ok {
			return errors.New("missing username")
		}
		dbconn, err = rz.dbmgr.Get(dbname)
		if err != nil {
			writePgMsgs(rz.conn,
				&pgproto3.ErrorResponse{
					Message: "error opening or unknown database " + dbname + ": " + err.Error(),
				},
			)
			return err
		}
		dbconn.User = username
		sqlconn, err := dbconn.Conn(rz.ctx)
		if err != nil {
			writePgMsgs(rz.conn,
				&pgproto3.ErrorResponse{
					Message: "error connecting to database " + dbname + ": " + err.Error(),
				},
			)
			return err
		}
		rz.db = dbconn
		rz.sqlconn = sqlconn
		buf := []byte{}
		buf = (&pgproto3.AuthenticationCleartextPassword{}).Encode(buf)
		_, err = rz.conn.Write(buf)
		return rz.processPwd()
	default:
		return fmt.Errorf("unknown pg startup msg: %#v", startMsg)
	}
	return nil
}

func (rz *RhizomeBackend) processPwd() error {
	pwdMsg, err := rz.backend.Receive()
	if err != nil {
		return err
	}
	switch pwdMsg := pwdMsg.(type) {
	case *pgproto3.SSLRequest:
		if rz.cfg.LogLevel >= constants.LogLevelDebug {
			deck.Infof("Detected FE SSLRequest msg during pwd: %+v\n", pwdMsg)
		}

		err := rz.upgradeToTLS()
		if err != nil {
			return err
		}
		return rz.processPwd()
	case *pgproto3.PasswordMessage:

		if rz.db.Authorize(rz.db.User, pwdMsg.Password, rz.db.ID) == false {
			return errors.New("not authorized")
		}

		buf := (&pgproto3.AuthenticationOk{}).Encode(nil)
		buf = (&pgproto3.ParameterStatus{
			Name:  "client_encoding",
			Value: "UTF8",
		}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{
			Name:  "server_encoding",
			Value: "UTF8",
		}).Encode(buf)

		if rz.cfg.ServerVersion != "" {
			buf = (&pgproto3.ParameterStatus{
				Name:  "server_version",
				Value: rz.cfg.ServerVersion,
			}).Encode(buf)
		}

		buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
		_, err = rz.conn.Write(buf)
		if err != nil {
			return fmt.Errorf("error sending ready for query: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unknown pg startup msg: %#v", pwdMsg)
	}
	return nil
}

func (rz *RhizomeBackend) Run() error {
	defer func() {
		rz.close()
	}()
	err := rz.processStart()
	if err != nil {
		var buf []byte
		buf = (&pgproto3.ErrorResponse{
			Severity: "fatal",
			Message:  "not authorized",
		}).Encode(buf)
		_, err = rz.conn.Write(buf)
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
			if rz.cfg.LogLevel >= constants.LogLevelDebug {
				deck.Infof("Detected FE Bind msg: %+v\n", msg)
			}
			if err := rz.handleBind(msg); err != nil {
				return err
			}
		case *pgproto3.CancelRequest:
			return errors.New("received unsupported cancel request")
		case *pgproto3.Close:
			if rz.cfg.LogLevel >= constants.LogLevelDebug {
				deck.Infof("Detected FE Close msg: %+v\n", msg)
			}
			if err := rz.handleClose(msg); err != nil {
				return err
			}
		case *pgproto3.CopyData:
			return errors.New("received unsupported copy request")
		case *pgproto3.CopyDone:
			return errors.New("received unsupported copy done request")
		case *pgproto3.CopyFail:
			return errors.New("received unsupported copy fail request")
		case *pgproto3.Describe:
			if rz.cfg.LogLevel >= constants.LogLevelDebug {
				deck.Infof("Detected FE Describe msg: %+v\n", msg)
			}
			if err := rz.handleDescribe(msg); err != nil {
				return err
			}
		case *pgproto3.Execute:
			if rz.cfg.LogLevel >= constants.LogLevelDebug {
				deck.Infof("Detected FE Execute msg: %+v\n", msg)
			}
			if err := rz.handleExecute(msg); err != nil {
				return err
			}
		case *pgproto3.Flush:
			if rz.cfg.LogLevel >= constants.LogLevelDebug {
				deck.Infof("Detected FE Flush msg: %+v\n", msg)
			}
			if err := rz.handleFlush(msg); err != nil {
				return err
			}
		case *pgproto3.FunctionCall:
			return errors.New("received unsupported function call request")
		case *pgproto3.GSSEncRequest:
			return errors.New("received unsupported gssenc request")
		case *pgproto3.PasswordMessage:
			return errors.New("received out of band password ")
		case *pgproto3.SASLInitialResponse:
			return errors.New("received unsupported sasl initial response request")
		case *pgproto3.SASLResponse:
			return errors.New("received unsupported sasl response request")
		case *pgproto3.SSLRequest:
			err := rz.upgradeToTLS()
			if err != nil {
				return err
			}
		case *pgproto3.StartupMessage:
			return errors.New("received unsupported startup request (out of sequence)")
		case *pgproto3.Sync:
			if rz.cfg.LogLevel >= constants.LogLevelDebug {
				deck.Infof("Detected FE Sync msg: %+v\n", msg)
			}
			if err := rz.handleSync(msg); err != nil {
				return err
			}
		case *pgproto3.Terminate:
			// exit
			return nil
		case *pgproto3.Parse:
			if rz.cfg.LogLevel >= constants.LogLevelDebug {
				deck.Infof("Detected FE Parse msg: %+v\n", msg)
			}
			if err := rz.handleParse(msg); err != nil {
				return err
			}
		case *pgproto3.Query:
			if rz.cfg.LogLevel >= constants.LogLevelDebug {
				deck.Infof("Detected FE Query msg: %+v\n", msg)
			}
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
	if rz.sqlconn != nil {
		_ = rz.sqlconn.Close()
	}
	return rz.conn.Close()
}

/*
handleQuery() responds to basic Query messages (which encode basic SQL as a text string).
*/
func (rz *RhizomeBackend) handleQuery(msg *pgproto3.Query) error {
	if rz.cfg.LogLevel >= constants.LogLevelDebug {
		// TODO -- convert to deck logging
		deck.Infof("handling query %q\n", msg.String)
	}
	if strings.HasPrefix(strings.TrimSpace(msg.String), "[[") {
		if rz.cfg.LogLevel >= constants.LogLevelDebug {
			deck.Infof("detected MetaDDL, rerouting...")
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

	if rz.sqlconn == nil {
		return ErrDBNotOpen
	}

	// Run the query and check for errors
	rows, err := rz.sqlconn.QueryContext(rz.ctx, msg.String)

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

/*
handleParse() and its siblings respond to Extended Query flows (as described in https://www.postgresql.org/docs/current/protocol-flow.html).
The message flow here is:
FE: Parse
BE: ParseComplete OR ErrorResponse
FE: Bind
BE: BindComplete OR ErrorResponse
FE: Execute
BE: CommandComplete OR EmptyQueryResponse OR ErrorResponse OR PortalSuspended
FE: Sync

Note that we don't try to handle these all inside the handleParse() func (as postlite does) in order to support pipelining
and named statements/portals. If the client doesn't name a statement/portal, then we just use the empty string as the
"unnamed" prepared statement or portal; it all goes into the map the same.
*/

/*
Parse statements
*/
func (rz *RhizomeBackend) handleParse(msg *pgproto3.Parse) error {
	if rz.cfg.LogLevel >= constants.LogLevelDebug {
		deck.Infof("Parsing query %q\n", msg.Query)
	}
	pstmt, err := rz.sqlconn.PrepareContext(rz.ctx, msg.Query)
	if err != nil {
		return writePgMsgs(rz.conn,
			&pgproto3.ErrorResponse{
				Message: err.Error(),
			},
		)
	}

	stmt := RhizomePreparedStatement{
		ID:           msg.Name,
		Stmt:         msg.Query,
		PreparedStmt: pstmt,
		ParamOIDs:    make([]uint32, 0),
	}
	for _, v := range msg.ParameterOIDs {
		stmt.ParamOIDs = append(stmt.ParamOIDs, v)
	}

	rz.stmts[msg.Name] = &stmt
	// TODO -- do we need to send ReadyForQuery?
	return writePgMsgs(rz.conn,
		&pgproto3.ParseComplete{},
	)
}

func (rz *RhizomeBackend) handleBind(msg *pgproto3.Bind) error {
	stmtptr, ok := rz.stmts[msg.PreparedStatement]
	if !ok || stmtptr == nil {
		return writePgMsgs(rz.conn,
			&pgproto3.ErrorResponse{
				Message: "statement '" + msg.PreparedStatement + "' does not exist",
			},
		)
	}
	portal := &RhizomePortal{
		ID:                          msg.DestinationPortal,
		StmtID:                      msg.PreparedStatement,
		ParamsUseBinaryFormatting:   make([]bool, 0),
		ResultsUserBinaryFormatting: make([]bool, 0),
		Params:                      make([]any, 0),
	}
	for _, v := range msg.ParameterFormatCodes {
		var b bool
		if v > 0 {
			b = true
		}
		portal.ParamsUseBinaryFormatting = append(portal.ParamsUseBinaryFormatting, b)
	}
	for _, v := range msg.ResultFormatCodes {
		var b bool
		if v > 0 {
			b = true
		}
		portal.ResultsUserBinaryFormatting = append(portal.ResultsUserBinaryFormatting, b)
	}
	for _, v := range msg.Parameters {
		portal.Params = append(portal.Params, v)
	}
	rz.portals[msg.DestinationPortal] = portal

	return writePgMsgs(rz.conn,
		&pgproto3.BindComplete{},
	)
}

func (rz *RhizomeBackend) handleExecute(msg *pgproto3.Execute) error {
	if rz.cfg.LogLevel >= constants.LogLevelDebug {
		deck.Infof("Attempting to execute portal %q\n", msg.Portal)
	}
	portalptr, ok := rz.portals[msg.Portal]
	if !ok || portalptr == nil {
		return writePgMsgs(rz.conn,
			&pgproto3.ErrorResponse{
				Message: "portal '" + msg.Portal + "' does not exist",
			},
		)
	}
	if rz.cfg.LogLevel >= constants.LogLevelDebug {
		deck.Infof("Attempting to execute stmt ID %q\n", portalptr.StmtID)
	}
	stmtptr, ok := rz.stmts[portalptr.StmtID]
	if !ok || stmtptr == nil {
		return writePgMsgs(rz.conn,
			&pgproto3.ErrorResponse{
				Message: "statement '" + portalptr.StmtID + "' does not exist",
			},
		)
	}
	if rz.cfg.LogLevel >= constants.LogLevelDebug {
		deck.Infof("Attempting to execute stmt literal %q\n", stmtptr.Stmt)
	}

	rows, err := stmtptr.PreparedStmt.QueryContext(rz.ctx, portalptr.Params...)
	if err != nil {
		return writePgMsgs(rz.conn,
			&pgproto3.ErrorResponse{
				Message: err.Error(),
			},
		)
	}
	cols, err := rows.ColumnTypes()
	if err != nil {
		return writePgMsgs(rz.conn,
			&pgproto3.ErrorResponse{
				Message: err.Error(),
			},
		)
	}

	buf := make([]byte, 0)
	pgrows, err := convertRowsToPgRows(rows, cols)
	if err != nil {
		deck.Errorf("failed to convert rows for executed query: %s", err.Error())
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

func (rz *RhizomeBackend) handleSync(msg *pgproto3.Sync) error {
	return writePgMsgs(rz.conn,
		&pgproto3.ReadyForQuery{
			TxStatus: 'I',
		},
	)
}

func (rz *RhizomeBackend) handleDescribe(msg *pgproto3.Describe) error {
	// RowDescription OR NoData OR ErrorResponse
	if msg.ObjectType == 'P' || msg.ObjectType == 'p' {
		_, ok := rz.portals[msg.Name]
		if !ok {
			return writePgMsgs(rz.conn,
				&pgproto3.ErrorResponse{},
			)
		}
		return writePgMsgs(rz.conn,
			&pgproto3.NoData{},
		)
	}
	if msg.ObjectType == 'S' || msg.ObjectType == 's' {
		st, ok := rz.stmts[msg.Name]
		if !ok {
			return writePgMsgs(rz.conn,
				&pgproto3.ErrorResponse{},
			)
		}
		return writePgMsgs(rz.conn,
			&pgproto3.ParameterDescription{
				ParameterOIDs: st.ParamOIDs,
			},
		)
	}
	return writePgMsgs(rz.conn,
		&pgproto3.NoData{},
	)
}

func (rz *RhizomeBackend) handleClose(msg *pgproto3.Close) error {
	if msg.ObjectType == 'S' || msg.ObjectType == 's' {
		s, ok := rz.stmts[msg.Name]
		if ok {
			_ = s.PreparedStmt.Close()
			delete(rz.stmts, msg.Name)
		}
	} else if msg.ObjectType == 'P' || msg.ObjectType == 'p' {
		delete(rz.portals, msg.Name)
	}
	return writePgMsgs(rz.conn,
		&pgproto3.CloseComplete{},
	)
}

func (rz *RhizomeBackend) handleFlush(msg *pgproto3.Flush) error {
	return writePgMsgs(rz.conn,
		&pgproto3.ReadyForQuery{
			TxStatus: 'I',
		},
	)
}

/*
See https://www.postgresql.org/docs/current/errcodes-appendix.html for codes
*/
func (rz *RhizomeBackend) CloseConnection(code, msg string) error {
	return writePgMsgs(rz.conn,
		&pgproto3.ErrorResponse{
			Severity: "FATAL",
			Message:  msg,
			Code:     code,
		},
	)
}

func (rz *RhizomeBackend) cleanup() error {
	if rz.sqlconn != nil {
		return rz.sqlconn.Close()
	}
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

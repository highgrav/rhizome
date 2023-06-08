package pgif

import (
	"github.com/jackc/pgproto3/v2"
)

type MetaCommandType string

const (
	MetaAddUserCmd          MetaCommandType = "adduser"
	MetaDeleteUserCmd       MetaCommandType = "deluser"
	MetaAddUserToDbCmd      MetaCommandType = "dbadduser"
	MetaRemoveUserFromDbCmd MetaCommandType = "dbremuser"
	MetaAddUserRight        MetaCommandType = "adduserright"
	MetaRemoveUserRight     MetaCommandType = "remuserright"
)

func (rz RhizomeBackend) handleBackupDb(msg *pgproto3.Query) error {

	return nil
}

func (rz RhizomeBackend) handleCreateDb(msg *pgproto3.Query) error {
	return nil
}

func (rz RhizomeBackend) handleAddUser(msg *pgproto3.Query) error {
	return nil
}

func (rz RhizomeBackend) handleDeleteUser(msg *pgproto3.Query) error {
	return nil
}

func (rz RhizomeBackend) handleUpdateUser(msg *pgproto3.Query) error {
	return nil
}

func (rz RhizomeBackend) handleAddUserToDb(msg *pgproto3.Query) error {
	return nil
}

func (rz RhizomeBackend) handleRemoveUserFromDb(msg *pgproto3.Query) error {
	return nil
}

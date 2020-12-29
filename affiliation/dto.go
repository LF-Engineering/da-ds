package affiliation

import "database/sql"

// Identity contains sortingHat user Identity
type Identity struct {
	ID            sql.NullString
	UUID          sql.NullString
	Name          sql.NullString
	Username      sql.NullString
	Email         sql.NullString
	Domain        sql.NullString
	Gender        sql.NullString
	GenderACC     *int `db:"gender_acc"`
	OrgName       sql.NullString
	IsBot         bool `db:"is_bot"`
	MultiOrgNames []string
}

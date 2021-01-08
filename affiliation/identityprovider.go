package affiliation

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/LF-Engineering/dev-analytics-libraries/uuid"
)

// IdentityProvider manages user identities
type IdentityProvider struct {
	db DBConnector
}

// DBConnector contains dataAccess functionalities
type DBConnector interface {
	Get(dest interface{}, query string, args ...interface{}) error
	Select(dest interface{}, query string, args ...interface{}) error
	MustBegin() *sqlx.Tx
}

// NewIdentityProvider initiates a new IdentityProvider instance
func NewIdentityProvider(db DBConnector) *IdentityProvider {
	return &IdentityProvider{db: db}
}

// GetIdentity ...
func (i *IdentityProvider) GetIdentity(key string, val string) (*Identity, error) {
	query := fmt.Sprintf(`SELECT 
       identities.id,
       identities.uuid,
       profiles.name,
       identities.username,
       profiles.email,
       profiles.gender,
       profiles.gender_acc,
       profiles.is_bot
FROM 
     identities LEFT JOIN (profiles)
                 ON (identities.uuid = profiles.uuid)
where 
      identities.%s='%s';`, key, val)

	var identity Identity
	err := i.db.Get(&identity, query)
	if err != nil {
		return nil, err
	}

	return &identity, nil
}

// GetOrganizations gets user's enrolled organizations until given time
func (i *IdentityProvider) GetOrganizations(uuid string, date time.Time) ([]string, error) {
	query := fmt.Sprintf(`select distinct o.name 
		from enrollments e, organizations o
		where e.organization_id = o.id and
		e.uuid = '%s' and
       '%s' between e.start and e.end order by e.id desc`,
		uuid, date.Format(time.RFC3339))

	var multiOrg []string
	err := i.db.Select(&multiOrg, query)
	if err != nil {
		return nil, err
	}

	return multiOrg, nil
}

// CreateIdentity insert new identity to affiliations
func (i *IdentityProvider) CreateIdentity(ident Identity, source string) error {
	if !ident.Name.Valid && !ident.Email.Valid && !ident.Username.Valid {
		return fmt.Errorf("name, email and username are all empty")
	}

	// if username matches a real email and there is no email set, assume email=username
	if !ident.Email.Valid && ident.Username.Valid && IsValidEmail(ident.Username.String) {
		ident.Email = ident.Username
	}

	// if name matches a real email and there is no email set, assume email=name
	if !ident.Email.Valid && ident.Name.Valid && IsValidEmail(ident.Name.String) {
		ident.Email = ident.Name
	}
	// generate identity uuid
	uid, err := uuid.GenerateIdentity(&source, &ident.Email.String, &ident.Name.String, &ident.Username.String)
	if err != nil {
		return err
	}

	ident.UUID.String = uid
	return i.insertIdentity(ident, source)
}

func (i *IdentityProvider) insertIdentity(identity Identity, source string) error {
	now := time.Now()
	uuid := identity.UUID.String
	name := identity.Name.String
	userName := identity.Username.String
	email := identity.Email.String
	tx := i.db.MustBegin()
	res := tx.MustExec("insert ignore into uidentities(uuid,last_modified) values(?, ?)", uuid, now)
	affected, err := res.RowsAffected()
	if affected == 0 || err != nil {
		return tx.Rollback()
	}

	res = tx.MustExec("insert ignore into identities(id,source,name,email,username,uuid,last_modified) values (?, ?, ?, ?, ?, ?, ?)", uuid, source, name, email, userName, uuid, now)
	affected, err = res.RowsAffected()
	if affected == 0 || err != nil {
		return tx.Rollback()
	}

	tx.MustExec("insert ignore into profiles(uuid,name,email) values (?, ?, ?)", uuid, name, email)
	affected, err = res.RowsAffected()
	if affected == 0 || err != nil {
		return tx.Rollback()
	}
	return tx.Commit()

}

// IsValidEmail check if email is a valid email
func IsValidEmail(email string) (valid bool) {

	emailLen := len(email)
	if emailLen < 3 && emailLen > 254 {
		return
	}

	if !EmailRegex.MatchString(email) {
		return
	}
	parts := strings.Split(email, "@")
	mx, err := net.LookupMX(parts[1])
	if err != nil || len(mx) == 0 {
		return
	}
	valid = true
	return
}

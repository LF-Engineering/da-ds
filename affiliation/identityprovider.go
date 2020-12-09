package affiliation

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

// IdentityProvider manages user identities
type IdentityProvider struct {
	db *sqlx.DB
}

func NewIdentityProvider(db *sqlx.DB) *IdentityProvider {
	return &IdentityProvider{db: db}
}

// GetIdentityByUsername ...
func (i *IdentityProvider) GetIdentityByUsername(key string, val string) (*Identity, error) {
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



// GetEnrollments ...
func (i *IdentityProvider) GetOrganizations(uuid string, date time.Time) ([]string , error) {
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

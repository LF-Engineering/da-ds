package affiliation

import (
	"fmt"

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
func (i *IdentityProvider) GetIdentityByUsername(username string) (*Identity, error) {
	query := fmt.Sprintf(`SELECT 
       identities.id,
       identities.uuid,
       profiles.name,
       identities.username,
       profiles.email,
       profiles.gender,
       profiles.gender_acc 
FROM 
     identities LEFT JOIN (profiles)
                 ON (identities.uuid = profiles.uuid)
where 
      identities.username='%s';`, username)

	var identity Identity
	err := i.db.Get(&identity, query)
	if err != nil {
		return nil, err
	}

	return &identity, nil
}

// GetIdentityByEmail ...
func (i *IdentityProvider) GetIdentityByEmail(email string) (*Identity, error) {
	query := fmt.Sprintf(`SELECT 
       identities.id,
       identities.uuid,
       profiles.name,
       identities.username,
       profiles.email,
       profiles.gender,
       profiles.gender_acc 
FROM 
     identities LEFT JOIN (profiles)
                 ON (identities.uuid = profiles.uuid)
where 
      identities.email='%s';`, email)

	var identity Identity
	err := i.db.Get(&identity, query)
	if err != nil {
		return nil, err
	}

	return &identity, nil
}

package affiliation

import (
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/LF-Engineering/da-ds/affiliation/mocks"

	"github.com/stretchr/testify/assert"
)

func TestGetIdentityByUsername(t *testing.T) {
	// Arrange
	dataBase := &mocks.DBConnector{}
	key := "username"
	val := "vvavrychuk"
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

	ide := Identity{}
	dataBase.On("Get", &ide, query).Run(func(args mock.Arguments) {
		email := "ayman@gmail.com"
		o := Identity{
			ID:        sql.NullString{String: "5", Valid: true},
			UUID:      sql.NullString{String: "5", Valid: true},
			Name:      sql.NullString{String: "vvavrychuk", Valid: true},
			Username:  sql.NullString{String: "vvavrychuk", Valid: true},
			Email:     sql.NullString{String: email, Valid: true},
			Domain:    sql.NullString{String: "inc.com", Valid: true},
			Gender:    sql.NullString{},
			GenderACC: nil,
			OrgName:   sql.NullString{},
			IsBot:     false,
		}
		reflect.ValueOf(args.Get(0)).Elem().Set(reflect.ValueOf(o))
	}).Return(nil)

	// Act
	srv := NewIdentityProvider(dataBase)
	res, err := srv.GetIdentity(key, val)
	// Assert
	assert.NoError(t, err)
	assert.Equal(t, res.UUID.String, "5")
	assert.Equal(t, res.Domain.String, "inc.com")
	assert.Equal(t, res.Email.String, "ayman@gmail.com")
	assert.Equal(t, res.IsBot, false)

}

func TestGetOrganizations(t *testing.T) {
	// Arrange
	dataBase := &mocks.DBConnector{}
	fakeUUID := "fakeUUID"
	date := time.Now()
	query := fmt.Sprintf(`select distinct o.name 
		from enrollments e, organizations o
		where e.organization_id = o.id and
		e.uuid = '%s' and
       '%s' between e.start and e.end order by e.id desc`,
		fakeUUID, date.Format(time.RFC3339))
	var orgs []string
	dataBase.On("Select", &orgs, query).Run(func(args mock.Arguments) {
		o := []string{
			"LF",
			"LFX",
		}
		reflect.ValueOf(args.Get(0)).Elem().Set(reflect.ValueOf(o))
	}).Return(nil)

	// Act
	srv := NewIdentityProvider(dataBase)
	res, err := srv.GetOrganizations(fakeUUID, date)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 2, len(res))
	assert.Equal(t, "LF", res[0])
	assert.Equal(t, "LFX", res[1])
}

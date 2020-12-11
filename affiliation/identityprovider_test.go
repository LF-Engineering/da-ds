package affiliation

import (
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

		o := Identity{
			ID:        "5",
			UUID:      "5",
			Name:      "vvavrychuk",
			Username:  "vvavrychuk",
			Email:     nil,
			Domain:    "inc.com",
			Gender:    nil,
			GenderACC: nil,
			OrgName:   nil,
			IsBot:     false,
		}
		reflect.ValueOf(args.Get(0)).Elem().Set(reflect.ValueOf(o))
	}).Return(nil)

	// Act
	srv := NewIdentityProvider(dataBase)
	res, err := srv.GetIdentity(key, val)
	fmt.Println(res)
	// Assert
	assert.NoError(t, err)
	assert.Equal(t, res.UUID, "5")
	assert.Equal(t, res.Domain, "inc.com")
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
	fmt.Println(res)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 2, len(res))
	assert.Equal(t, "LF", res[0])
	assert.Equal(t, "LFX", res[1])
}

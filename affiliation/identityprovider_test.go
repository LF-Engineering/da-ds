package affiliation

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/LF-Engineering/da-ds/affiliation/mocks"

	db "github.com/LF-Engineering/da-ds/db"

	"github.com/stretchr/testify/assert"
)

func TestGetIdentityByUsername(t *testing.T) {
	// Arrange
	x := "lfinsights_test:urpialruvCadcyakhokcect2@tcp(lfinsights-db.clctyzfo4svp.us-west-2.rds.amazonaws.com)/lfinsights_test?charset=utf8"
	dataBase, err := db.NewConnector("mysql", x)
	if err != nil {
		fmt.Println("jjjjjjj")
		fmt.Println(err)
	}
	// Act
	srv := NewIdentityProvider(dataBase)
	res, err := srv.GetIdentityByUsername("username", "vvavrychuk")
	fmt.Println(res)
	// Assert
	assert.NoError(t, err)

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

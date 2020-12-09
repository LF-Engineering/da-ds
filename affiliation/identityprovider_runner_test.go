package affiliation

import (
	"fmt"
	"testing"
	"time"

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



func TestGetEnrollments(t *testing.T) {
	// Arrange
	x := "lfinsights_test:urpialruvCadcyakhokcect2@tcp(lfinsights-db.clctyzfo4svp.us-west-2.rds.amazonaws.com)/lfinsights_test?charset=utf8"
	dataBase, err := db.NewConnector("mysql", x)
	if err != nil {
		fmt.Println("jjjjjjj")
		fmt.Println(err)
	}
	// Act
	srv := NewIdentityProvider(dataBase)
	now := time.Now()
	res, err := srv.GetOrganizations("5d408e590365763c3927084d746071fa84dc8e52", now )
	fmt.Println(res)
	// Assert
	assert.NoError(t, err)

}
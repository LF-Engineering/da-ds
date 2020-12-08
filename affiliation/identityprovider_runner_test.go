package affiliation

import (
	"fmt"
	"testing"

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
	res, err := srv.GetIdentityByUsername("vvavrychuk")
	fmt.Println(res)
	// Assert
	assert.NoError(t, err)

}

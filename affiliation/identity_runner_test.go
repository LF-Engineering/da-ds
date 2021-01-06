package affiliation

import (
	"testing"

	"github.com/LF-Engineering/da-ds/db"

	"github.com/stretchr/testify/assert"
)

func TestCreateIdentity(t *testing.T) {

	// Arrange
	var iden Identity
	iden.Email.String = "test@test.com"
	iden.Email.Valid = true

	iden.Username.String = "testMe"
	iden.Username.Valid = true

	iden.Name.String = "testMe"
	iden.Name.Valid = true

	// Act
	dataBase, err := db.NewConnector("mysql", "root:changeme@tcp(127.0.0.1:3306)/sh-local")
	srv := NewIdentityProvider(dataBase)
	srv.CreateIdentity(iden, "bugzillaTest")

	// Assert
	assert.NoError(t, err)

}

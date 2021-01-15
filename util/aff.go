package util

import (
	"fmt"
	"log"

	jsoniter "github.com/json-iterator/go"
)

type AuthProvider interface {
	GenerateToken() string
}

type HTTPClientProvider interface {
	Request(url string, method string, header map[string]string, body []byte, params map[string]string) (statusCode int, resBody []byte, err error)
}

type Params struct {
	AuthProvider       AuthProvider
	HttpClientProvider HTTPClientProvider
	AffAPI             string
	Key                string
	Value              string
	ProjectSlug        string
	Data               *Person
	Source             string
}

// CreateNewIdentity ...
func CreateNewIdentity(params Params) {

	data := params.Data
	var identity IdentityData
	if data == nil {
		log.Print("Err : identity data is empty")
		return
	}
	if data.Name != "" {
		identity.Name = &data.Name
	}
	if data.Username != "" {
		identity.Username = &data.Username
	}
	// if username exist and there is no name assume name = username
	if data.Username != "" && data.Name == "" {
		identity.Name = &data.Username
	}

	token := params.AuthProvider.GenerateToken()
	header := make(map[string]string)
	header["Authorization"] = token

	bData, err := jsoniter.Marshal(identity)
	if err != nil {
		log.Printf("Err : %s", err.Error())
		return
	}

	createIdentityAPI := fmt.Sprintf("%s/v1/affiliation/%s/add_identity/%s", params.AffAPI, params.ProjectSlug, params.Source)
	_, _, err = params.HttpClientProvider.Request(createIdentityAPI, "Post", header, bData, nil)
	if err != nil {
		log.Printf("Err : %s", err.Error())
		return
	}
	return

}

// GetAffiliationIdentity gets author SH identity data
func GetAffiliationIdentity(params Params) (*AffIdentity, error) {

	token := params.AuthProvider.GenerateToken()
	header := make(map[string]string)
	header["Authorization"] = token
	var bData []byte
	getIdentityAPI := fmt.Sprintf("%s/v1/affiliation/identity/%s/%s", params.AffAPI, params.Key, params.Value)
	_, identityRes, err := params.HttpClientProvider.Request(getIdentityAPI, "GET", header, bData, nil)
	if err != nil {
		return nil, err
	}

	var ident IdentityData
	err = jsoniter.Unmarshal(identityRes, &ident)
	if err != nil {
		return nil, err
	}

	getProfileAPI := fmt.Sprintf("%s/v1/affiliation/%s/get_profile/%v", params.AffAPI, params.ProjectSlug, ident.UUID)
	_, profileRes, err := params.HttpClientProvider.Request(getProfileAPI, "GET", header, bData, nil)
	if err != nil {
		return nil, err
	}

	var profile UniqueIdentityFullProfile
	err = jsoniter.Unmarshal(profileRes, &profile)
	if err != nil {
		return nil, err
	}

	var identity AffIdentity
	identity.UUID = ident.UUID
	identity.Name = *ident.Name
	identity.Username = *ident.Username
	identity.Email = *ident.Email
	identity.ID = &ident.ID

	identity.IsBot = profile.Profile.IsBot
	identity.Gender = profile.Profile.Gender
	identity.GenderACC = profile.Profile.GenderAcc

	if len(profile.Enrollments) > 1 {
		identity.OrgName = &profile.Enrollments[0].Organization.Name
		for _, org := range profile.Enrollments {
			identity.MultiOrgNames = append(identity.MultiOrgNames, org.Organization.Name)
		}
	} else if len(profile.Enrollments) == 1 {
		identity.OrgName = &profile.Enrollments[0].Organization.Name
		identity.MultiOrgNames = append(identity.MultiOrgNames, profile.Enrollments[0].Organization.Name)
	}

	return &identity, nil
}

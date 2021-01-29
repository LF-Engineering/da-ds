package finosmeetings

import (
	"fmt"
	"strings"
	"time"

	"github.com/LF-Engineering/da-ds/affiliation"
)

// Enricher contains dockerhub datasource enrich logic
type Enricher struct {
	identityProvider      IdentityProvider
	DSName                string // Datasource will be used as key for ES
	ElasticSearchProvider ESClientProvider
	BackendVersion        string
}

type IdentityProvider interface {
	GetIdentity(key string, val string) (*affiliation.Identity, error)
	GetOrganizations(uuid string, date time.Time) ([]string, error)
}

// // TopHits result
// type TopHits struct {
// 	Took         int          `json:"took"`
// 	Hits         Hits         `json:"hits"`
// 	Aggregations Aggregations `json:"aggregations"`
// }

// // Hits result
// type Hits struct {
// 	Total    Total        `json:"total"`
// 	MaxScore float32      `json:"max_score"`
// 	Hits     []NestedHits `json:"hits"`
// }

// // Total result
// type Total struct {
// 	Value    int    `json:"value"`
// 	Relation string `json:"relation"`
// }

// // NestedHits result
// type NestedHits struct {
// 	Index  string         `json:"_index"`
// 	Type   string         `json:"_type"`
// 	ID     string         `json:"_id"`
// 	Score  float64        `json:"_score"`
// 	Source *RepositoryRaw `json:"_source"`
// }

// // Aggregations result
// type Aggregations struct {
// 	LastDate LastDate `json:"last_date"`
// }

// // LastDate result
// type LastDate struct {
// 	Value         float64 `json:"value"`
// 	ValueAsString string  `json:"value_as_string"`
// }

// NewEnricher initiates a new Enricher
func NewEnricher(identProvider IdentityProvider, backendVersion string, esClientProvider ESClientProvider) *Enricher {
	return &Enricher{
		identityProvider:      identProvider,
		DSName:                Finosmeetings,
		ElasticSearchProvider: esClientProvider,
		BackendVersion:        backendVersion,
	}
}

// EnrichItem enriches raw item
func (e *Enricher) EnrichItem(rawItem FinosmeetingsRaw, now time.Time) (*FinosmeetingsEnrich, error) {

	enriched := FinosmeetingsEnrich{}

	enriched.Project = rawItem.Data.CMTitle

	enriched.MetadataBackendName = fmt.Sprintf("%sEnrich", strings.Title(e.DSName))
	enriched.BackendVersion = e.BackendVersion
	enriched.Date = rawItem.Data.Date
	enriched.DateIsoFormat = rawItem.Data.DateIsoFormat
	now = now.UTC()
	enriched.MetadataEnrichedOn = now

	enriched.CMProgram = rawItem.Data.CMProgram
	enriched.CMTitle = rawItem.Data.CMTitle
	enriched.CMType = rawItem.Data.CMType

	enriched.MetadataTimestamp = rawItem.MetadataTimestamp
	enriched.MetadataUpdatedOn = rawItem.MetadataUpdatedOn.UTC()
	enriched.CreationDate = rawItem.Data.DateIsoFormat

	enriched.AuthorUserName = rawItem.Data.GithubID

	enriched.Origin = rawItem.Origin
	enriched.Tag = rawItem.Origin
	enriched.UUID = rawItem.UUID
	enriched.GithubID = rawItem.Data.GithubID
	enriched.IsFinosMeetingEntry = 1

	emailField := "email"
	unknown := "Unknown"
	emailIdentity, err := e.identityProvider.GetIdentity(emailField, rawItem.Data.Email)

	fmt.Println("THIS IS EMAIL IDENTITY")
	fmt.Println(emailIdentity)

	if err == nil {
		enriched.AuthorID = emailIdentity.ID
		enriched.AuthorUUID = emailIdentity.UUID
		enriched.AuthorName = emailIdentity.Name

		enriched.EmailID = emailIdentity.ID
		enriched.EmailUUID = emailIdentity.UUID
		enriched.EmailBot = emailIdentity.IsBot
		enriched.Email = *emailIdentity.Email
		enriched.EmailDomain = strings.Split(enriched.Email, "@")[1]
		enriched.AuthorDomain = strings.Split(enriched.Email, "@")[1]
		enriched.EmailName = emailIdentity.Name
		enriched.Name = emailIdentity.Name
		enriched.EmailUsername = rawItem.Data.GithubID
		//enriched.CSVOrg = *emailIdentity.OrgName

		emailOrgs, err := e.identityProvider.GetOrganizations(emailIdentity.UUID, enriched.MetadataUpdatedOn)

		if err == nil {
			enriched.EmailMultiOrgNames = emailOrgs

			if len(emailOrgs) == 0 {
				enriched.EmailOrgName = unknown
				enriched.CSVOrg = unknown
				enriched.AuthorOrgName = unknown
			} else {
				enriched.EmailOrgName = emailOrgs[0]
				enriched.CSVOrg = emailOrgs[0]
				enriched.AuthorOrgName = emailOrgs[0]
			}
		}

		if emailIdentity.Gender != nil {
			enriched.EmailGender = *emailIdentity.Gender
			enriched.AuthorGender = *emailIdentity.Gender
		} else {
			enriched.EmailGender = unknown
			enriched.AuthorGender = unknown
		}

		if emailIdentity.GenderACC != nil {
			enriched.EmailGenderAcc = *emailIdentity.GenderACC
		} else {
			enriched.EmailGenderAcc = 0
		}

	}

	return &enriched, nil
}

// HandleMapping creates rich mapping
func (e *Enricher) HandleMapping(index string) error {
	_, err := e.ElasticSearchProvider.CreateIndex(index, FinosmeetingsRichMapping)
	return err
}

package pipermail

import (
	"fmt"
	"log"
	"strings"
	"time"

	lib "github.com/LF-Engineering/da-ds"
	"github.com/LF-Engineering/da-ds/affiliation"
	libAffiliations "github.com/LF-Engineering/dev-analytics-libraries/affiliation"
)

// Enricher contains pipermail datasource enrich logic
type Enricher struct {
	identityProvider           IdentityProvider
	DSName                     string // Datasource will be used as key for ES
	ElasticSearchProvider      ESClientProvider
	BackendVersion             string
	affiliationsClientProvider *libAffiliations.Affiliation
}

// IdentityProvider manages user identity
type IdentityProvider interface {
	GetIdentity(key string, val string) (*affiliation.Identity, error)
	GetOrganizations(uuid string, date time.Time) ([]string, error)
}

// NewEnricher initiates a new Enricher
func NewEnricher(identProvider IdentityProvider, backendVersion string, esClientProvider ESClientProvider, affiliationsClientProvider *libAffiliations.Affiliation) *Enricher {
	return &Enricher{
		identityProvider:           identProvider,
		DSName:                     Pipermail,
		ElasticSearchProvider:      esClientProvider,
		BackendVersion:             backendVersion,
		affiliationsClientProvider: affiliationsClientProvider,
	}
}

// EnrichMessage enriches raw message
func (e *Enricher) EnrichMessage(rawMessage *RawMessage, now time.Time) (*EnrichMessage, error) {

	now = now.UTC()
	enriched := EnrichMessage{
		ID:                   rawMessage.Data.MessageID,
		ProjectTS:            0,
		FromUserName:         "",
		TZ:                   rawMessage.Data.DateTZ,
		MessageID:            rawMessage.Data.MessageID,
		UUID:                 rawMessage.UUID,
		AuthorName:           "",
		Root:                 false,
		FromUUID:             "",
		AuthorGenderACC:      0,
		FromName:             "",
		AuthorOrgName:        "",
		AuthorUserName:       "",
		AuthorBot:            false,
		BodyExtract:          rawMessage.Data.Data.Text.Plain[0].Data,
		AuthorID:             "",
		SubjectAnalyzed:      rawMessage.Data.Subject,
		FromBot:              false,
		Project:              rawMessage.Project,
		ProjectSlug:          rawMessage.ProjectSlug,
		MboxAuthorDomain:     "",
		Date:                 rawMessage.Data.Date,
		IsPipermailMessage:   1,
		FromGender:           "",
		FromMultipleOrgNames: nil,
		FromOrgName:          "",
		FromDomain:           "",
		List:                 rawMessage.Origin,
		AuthorUUID:           "",
		AuthorMultiOrgNames:  nil,
		Origin:               rawMessage.Origin,
		Size:                 rawMessage.Data.MboxByteLength,
		Tag:                  rawMessage.Origin,
		Subject:              rawMessage.Data.Subject,
		FromID:               "",
		AuthorGender:         "",
		FromGenderAcc:        "",
		EmailDate:            rawMessage.Data.Date,
		MetadataTimestamp:    rawMessage.MetadataTimestamp,
		MetadataBackendName:  fmt.Sprintf("%sEnrich", strings.Title(e.DSName)),
		MetadataUpdatedOn:    rawMessage.MetadataUpdatedOn,
		MetadataEnrichedOn:   now,
		BackendVersion:       rawMessage.BackendVersion,
		ChangedDate:          rawMessage.ChangedAt,
	}

	userAffiliationsEmail := e.HandleObfuscatedEmail(rawMessage.Data.From)
	userData, err := e.identityProvider.GetIdentity("email", userAffiliationsEmail)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	enriched.MboxAuthorDomain = e.GetEmailDomain(userAffiliationsEmail)
	// if user is already affiliated
	if userData != nil {
		if userData.ID.Valid {
			enriched.AuthorID = userData.ID.String
		}
		if userData.Name.Valid {
			enriched.AuthorName = userData.Name.String
			enriched.FromName = userData.Name.String
		}
		if userData.Username.Valid {
			enriched.FromUserName = userData.Username.String
		}
		if userData.OrgName.Valid {
			enriched.FromOrgName = userData.OrgName.String
		}
		if userData.UUID.Valid {
			enriched.AuthorUUID = userData.UUID.String
		}
		if userData.Gender.Valid {
			enriched.AuthorGender = userData.Gender.String
			enriched.FromGender = userData.Gender.String
		}
		mUpdatedOn, err := lib.TimeParseES(rawMessage.MetadataUpdatedOn)
		assignedToMultiOrg, err := e.identityProvider.GetOrganizations(userData.UUID.String, mUpdatedOn)
		if err == nil {
			if len(assignedToMultiOrg) != 0 {
				enriched.AuthorMultiOrgNames = assignedToMultiOrg
			}
		}

		enriched.FromBot = userData.IsBot
	} else {
		name := e.GetUserName(rawMessage.Data.From)
		userIdentity := libAffiliations.Identity{
			LastModified: time.Time{},
			Name:         name,
			Source:       Pipermail,
			Username:     "",
			Email:        userAffiliationsEmail,
			UUID:         rawMessage.UUID,
		}
		if ok := e.affiliationsClientProvider.AddIdentity(&userIdentity); !ok {
			log.Printf("failed to add identity for [%+v]", userAffiliationsEmail)
		} else {
			log.Printf("add identity for [%+v]", name)
		}
	}

	if rawMessage.Data.InReplyTo != "" {
		enriched.Root = true
	}

	return &enriched, nil
}

// EnrichAffiliation gets author Affiliations identity data
func (e *Enricher) EnrichAffiliation(key string, val string) (*affiliation.Identity, error) {
	return e.identityProvider.GetIdentity(key, val)
}

// HandleMapping creates rich mapping
func (e *Enricher) HandleMapping(index string) error {
	_, err := e.ElasticSearchProvider.CreateIndex(index, PiperRichMapping)
	return err
}

// HandleObfuscatedEmail ...
func (e *Enricher) HandleObfuscatedEmail(rawMailString string) (email string) {
	for _, pattern := range EmailObfuscationPatterns {
		trimBraces := strings.Split(rawMailString, " (")
		obfuscatedEmail := strings.TrimSpace(trimBraces[0])
		if strings.Contains(obfuscatedEmail, pattern) {
			email = strings.Replace(obfuscatedEmail, pattern, "@", 1)
			return
		}
	}
	return ""
}

// GetEmailDomain ...
func (e *Enricher) GetEmailDomain(email string) string {
	domain := strings.Split(email, "@")
	return domain[1]
}

// GetUserName ...
func (e *Enricher) GetUserName(rawMailString string) (username string) {
	trimBraces := strings.Split(rawMailString, " (")
	username = strings.TrimSpace(trimBraces[1])
	username = strings.TrimSpace(strings.Replace(username, ")", "", 1))
	return
}

// FormatTimestampString returns a formatted RFC 33339 Datetime string
func (e *Enricher) FormatTimestampString(str string) (*time.Time, error) {
	layout := "2006-01-02T15:04:05.000Z"
	t, err := time.Parse(layout, str)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

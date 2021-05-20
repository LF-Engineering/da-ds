package pipermail

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/LF-Engineering/da-ds/affiliation"
	libAffiliations "github.com/LF-Engineering/dev-analytics-libraries/affiliation"
	"github.com/LF-Engineering/dev-analytics-libraries/uuid"
)

// Enricher contains pipermail datasource enrich logic
type Enricher struct {
	DSName                     string // Datasource will be used as key for ES
	ElasticSearchProvider      ESClientProvider
	BackendVersion             string
	affiliationsClientProvider *libAffiliations.Affiliation
}

// AffiliationClient manages user identity
type AffiliationClient interface {
	GetIdentity(key string, val string) (*affiliation.Identity, error)
	AddIdentity(identity *affiliation.Identity) bool
	GetOrganizations(uuid string, date time.Time) ([]string, error)
}

// NewEnricher initiates a new Enricher
func NewEnricher(backendVersion string, esClientProvider ESClientProvider, affiliationsClientProvider *libAffiliations.Affiliation) *Enricher {
	return &Enricher{
		DSName:                     Pipermail,
		ElasticSearchProvider:      esClientProvider,
		BackendVersion:             backendVersion,
		affiliationsClientProvider: affiliationsClientProvider,
	}
}

// EnrichMessage enriches raw message
func (e *Enricher) EnrichMessage(rawMessage *RawMessage, now time.Time) (*EnrichMessage, error) {

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
		AuthorOrgName:        Unknown,
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
		FromGender:           Unknown,
		FromMultipleOrgNames: []string{Unknown},
		FromOrgName:          Unknown,
		FromDomain:           "",
		List:                 rawMessage.Origin,
		AuthorUUID:           "",
		AuthorMultiOrgNames:  []string{Unknown},
		Origin:               rawMessage.Origin,
		Size:                 rawMessage.Data.MboxByteLength,
		Tag:                  rawMessage.Origin,
		Subject:              rawMessage.Data.Subject,
		FromID:               "",
		EmailDate:            rawMessage.Data.Date,
		MetadataTimestamp:    rawMessage.MetadataTimestamp,
		MetadataBackendName:  fmt.Sprintf("%sEnrich", strings.Title(e.DSName)),
		MetadataUpdatedOn:    rawMessage.MetadataUpdatedOn,
		MetadataEnrichedOn:   now,
		ChangedDate:          rawMessage.ChangedAt,
	}

	if rawMessage.Data.InReplyTo != "" {
		enriched.Root = true
	}

	userAffiliationsEmail := e.HandleObfuscatedEmail(rawMessage.Data.From)
	userData, err := e.affiliationsClientProvider.GetIdentityByUser("email", userAffiliationsEmail)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	enriched.MboxAuthorDomain = e.GetEmailDomain(userAffiliationsEmail)
	// if user is already affiliated
	if userData != nil {
		if userData.ID != nil {
			enriched.AuthorID = *userData.ID
			enriched.FromID = *userData.ID
		}
		if userData.Name != "" {
			enriched.AuthorName = userData.Name
			enriched.FromName = userData.Name
		}
		if userData.Username != "" {
			enriched.FromUserName = userData.Username
		}
		if userData.OrgName != nil {
			enriched.FromOrgName = *userData.OrgName
			enriched.AuthorOrgName = *userData.OrgName
		}
		if userData.UUID != nil {
			enriched.AuthorUUID = *userData.UUID
			enriched.FromUUID = *userData.UUID
		}

		enrollments := e.affiliationsClientProvider.GetOrganizations(*userData.UUID, rawMessage.ProjectSlug)
		if enrollments != nil {
			organizations := make([]string, 0)
			for _, enrollment := range *enrollments {
				organizations = append(organizations, enrollment.Organization.Name)
			}

			if len(organizations) != 0 {
				enriched.AuthorMultiOrgNames = organizations
			}
		}

		if userData.IsBot != nil {
			if *userData.IsBot == 1 {
				enriched.FromBot = true
			}
		}
	} else {
		// add new affiliation if email format is valid
		if ok := e.IsValidEmail(userAffiliationsEmail); ok {
			name := e.GetUserName(rawMessage.Data.From)
			source := Pipermail
			authorUUID, err := uuid.GenerateIdentity(&source, &userAffiliationsEmail, &name, nil)
			if err != nil {
				fmt.Println(err)
				return nil, err
			}

			userIdentity := libAffiliations.Identity{
				LastModified: time.Now(),
				Name:         name,
				Source:       source,
				Email:        userAffiliationsEmail,
				UUID:         authorUUID,
			}
			fmt.Println(userIdentity)
			if ok := e.affiliationsClientProvider.AddIdentity(&userIdentity); !ok {
				log.Printf("failed to add identity for [%+v]", userAffiliationsEmail)
			}

			enriched.AuthorID = authorUUID
			enriched.AuthorName = name
			enriched.AuthorUUID = authorUUID
			enriched.FromID = authorUUID
			enriched.FromName = name
			enriched.FromUUID = authorUUID
		}
		log.Println(err)
	}

	return &enriched, nil
}

// IsValidEmail validates email string
func (e *Enricher) IsValidEmail(rawMailString string) bool {
	if strings.Contains(rawMailString, "...") {
		log.Println("email contains ellipsis")
		return false
	}

	return true
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

package pipermail

import (
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/LF-Engineering/dev-analytics-libraries/affiliation"
	"github.com/LF-Engineering/dev-analytics-libraries/uuid"
	"github.com/araddon/dateparse"
)

var emailRegex = regexp.MustCompile("^[a-zA-Z0-9.!#$%&'*+\\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")

// AffiliationClient manages user identity
type AffiliationClient interface {
	GetIdentityByUser(key string, value string) (*affiliation.AffIdentity, error)
	AddIdentity(identity *affiliation.Identity) bool
	GetOrganizations(uuid string, projectSlug string) *[]affiliation.Enrollment
}

// Enricher contains pipermail datasource enrich logic
type Enricher struct {
	DSName                     string // Datasource will be used as key for ES
	ElasticSearchProvider      ESClientProvider
	BackendVersion             string
	affiliationsClientProvider AffiliationClient
}

// NewEnricher initiates a new Enricher
func NewEnricher(backendVersion string, esClientProvider ESClientProvider, affiliationsClientProvider *affiliation.Affiliation) *Enricher {
	return &Enricher{
		DSName:                     Pipermail,
		ElasticSearchProvider:      esClientProvider,
		BackendVersion:             backendVersion,
		affiliationsClientProvider: affiliationsClientProvider,
	}
}

// EnrichMessage enriches raw message
func (e *Enricher) EnrichMessage(rawMessage *RawMessage, now time.Time) (*EnrichedMessage, error) {
	var bodyExtract string
	if len(rawMessage.Data.Data.Text.Plain) > 0 {
		bodyExtract = rawMessage.Data.Data.Text.Plain[0].Data
	}

	date, err := dateparse.ParseAny(rawMessage.Data.Date)
	if err != nil {
		log.Println(err)
	}

	enrichedMessage := EnrichedMessage{
		ID:                  rawMessage.Data.MessageID,
		TZ:                  rawMessage.Data.DateTZ,
		MessageID:           rawMessage.Data.MessageID,
		UUID:                rawMessage.UUID,
		Root:                false,
		AuthorName:          "",
		AuthorUUID:          "",
		AuthorID:            "",
		AuthorBot:           false,
		AuthorOrgName:       Unknown,
		AuthorMultiOrgNames: []string{Unknown},
		MboxAuthorDomain:    "",
		BodyExtract:         bodyExtract,
		SubjectAnalyzed:     rawMessage.Data.Subject,
		Project:             rawMessage.Project,
		ProjectSlug:         rawMessage.ProjectSlug,
		Date:                date,
		IsPipermailMessage:  1,
		List:                rawMessage.Origin,
		Origin:              rawMessage.Origin,
		Tag:                 rawMessage.Origin,
		GroupName:           rawMessage.GroupName,
		Size:                rawMessage.Data.MboxByteLength,
		Subject:             rawMessage.Data.Subject,
		EmailDate:           date,
		MetadataTimestamp:   rawMessage.MetadataTimestamp,
		MetadataBackendName: fmt.Sprintf("%sEnrich", strings.Title(e.DSName)),
		MetadataUpdatedOn:   date,
		MetadataEnrichedOn:  now,
		ChangedAt:           date,
		Slug:                rawMessage.ProjectSlug,
		References:          rawMessage.Data.References,
	}

	if rawMessage.Data.InReplyTo != "" {
		enrichedMessage.Root = true
	}

	userData := new(affiliation.AffIdentity)
	userAffiliationsEmail := e.HandleObfuscatedEmail(rawMessage.Data.From)
	enrichedMessage.MboxAuthorDomain = e.GetEmailDomain(userAffiliationsEmail)
	name := e.RemoveSpecialCharactersFromString(e.GetUserName(rawMessage.Data.From))
	source := Pipermail
	authorUUID, err := uuid.GenerateIdentity(&source, &userAffiliationsEmail, name, nil)
	if err != nil {
		log.Println(err)
	}

	if ok := e.IsValidEmail(userAffiliationsEmail); ok {
		userIdentity := affiliation.Identity{
			LastModified: now,
			Name:         *name,
			Source:       source,
			Email:        userAffiliationsEmail,
			ID:           authorUUID,
		}

		if ok := e.affiliationsClientProvider.AddIdentity(&userIdentity); !ok {
			log.Printf("failed to add identity for [%+v]", userAffiliationsEmail)
		}

		enrichedMessage.AuthorID = authorUUID
		enrichedMessage.AuthorUUID = authorUUID
		enrichedMessage.AuthorName = *name
	}

	userData, err = e.affiliationsClientProvider.GetIdentityByUser("id", authorUUID)
	if err != nil {
		errMessage := fmt.Sprintf("%+v : %+v", userAffiliationsEmail, err)
		log.Println(errMessage)
	}

	if userData != nil {
		// handle affiliations if userEmailsMapping exists
		if userData.ID != nil {
			enrichedMessage.AuthorID = *userData.ID
		}
		if userData.Name != "" {
			enrichedMessage.AuthorName = userData.Name
		}

		if userData.OrgName != nil {
			enrichedMessage.AuthorOrgName = *userData.OrgName
		}
		if userData.UUID != nil {
			enrichedMessage.AuthorUUID = *userData.UUID
		}

		if userData.UUID != nil {
			slug := rawMessage.ProjectSlug
			enrollments := e.affiliationsClientProvider.GetOrganizations(*userData.UUID, slug)
			if enrollments != nil {
				metaDataEpochMills := enrichedMessage.MetadataUpdatedOn.UnixNano() / 1000000
				organizations := make([]string, 0)
				for _, enrollment := range *enrollments {
					organizations = append(organizations, enrollment.Organization.Name)
				}

				for _, enrollment := range *enrollments {
					affStartEpoch := enrollment.Start.UnixNano() / 1000000
					affEndEpoch := enrollment.End.UnixNano() / 1000000
					if affStartEpoch <= metaDataEpochMills && affEndEpoch >= metaDataEpochMills {
						enrichedMessage.AuthorOrgName = enrollment.Organization.Name
						break
					}
				}

				if len(organizations) != 0 {
					enrichedMessage.AuthorMultiOrgNames = organizations
				}

				if enrichedMessage.AuthorName == Unknown && len(organizations) >= 1 {
					enrichedMessage.AuthorOrgName = organizations[0]
				}
			}
		}

		if userData.IsBot != nil {
			if *userData.IsBot == 1 {
				enrichedMessage.AuthorBot = true
			}
		}
	}

	return &enrichedMessage, nil
}

// IsValidEmail validates email string
func (e *Enricher) IsValidEmail(rawMailString string) bool {
	if strings.Contains(rawMailString, "...") {
		log.Println("email contains ellipsis")
		return false
	}

	if ok := emailRegex.MatchString(rawMailString); !ok {
		log.Println("invalid email pattern: ", rawMailString)
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
	return e.GetEmailUsername(username)
}

// GetEmailUsername ...
func (e *Enricher) GetEmailUsername(email string) string {
	username := strings.Split(email, "@")
	if len(username) > 1 {
		return username[0]
	}
	return email
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

// RemoveSpecialCharactersFromString ...
func (e *Enricher) RemoveSpecialCharactersFromString(s string) (val *string) {
	value := s
	// trim leading space
	value = strings.TrimLeft(value, " ")
	// trim trailing space
	value = strings.TrimRight(value, " ")
	// trim angle braces
	value = strings.Trim(value, "<>")
	// trim square braces
	value = strings.Trim(value, "[]")
	// trim brackets
	value = strings.Trim(value, "()")
	// remove all comas from name
	value = strings.ReplaceAll(value, ",", "")
	// trim quotes
	value = e.trimQuotes(value)

	return &value
}

func (e *Enricher) trimQuotes(s string) string {
	if len(s) >= 2 {
		if c := s[len(s)-1]; s[0] == c && (c == '"' || c == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

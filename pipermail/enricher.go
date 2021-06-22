package pipermail

import (
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/LF-Engineering/dev-analytics-libraries/affiliation"
	"github.com/LF-Engineering/dev-analytics-libraries/uuid"
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
	log.Println("In EnrichedMessage")
	var bodyExtract string
	if len(rawMessage.Data.Data.Text.Plain) > 0 {
		bodyExtract = rawMessage.Data.Data.Text.Plain[0].Data
	}
	enriched := EnrichedMessage{
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
		Date:                rawMessage.Data.Date,
		IsPipermailMessage:  1,
		List:                rawMessage.Origin,
		Origin:              rawMessage.Origin,
		Tag:                 rawMessage.Origin,
		GroupName:           e.GetGroupName(rawMessage.Origin),
		Size:                rawMessage.Data.MboxByteLength,
		Subject:             rawMessage.Data.Subject,
		EmailDate:           rawMessage.Data.Date,
		MetadataTimestamp:   rawMessage.MetadataTimestamp,
		MetadataBackendName: fmt.Sprintf("%sEnrich", strings.Title(e.DSName)),
		MetadataUpdatedOn:   rawMessage.MetadataUpdatedOn,
		MetadataEnrichedOn:  now,
		ChangedAt:           rawMessage.ChangedAt,
		Slug:                rawMessage.ProjectSlug,
	}

	if rawMessage.Data.InReplyTo != "" {
		enriched.Root = true
	}

	userAffiliationsEmail := e.HandleObfuscatedEmail(rawMessage.Data.From)
	userData, err := e.affiliationsClientProvider.GetIdentityByUser("email", userAffiliationsEmail)
	if err != nil {
		errMessage := fmt.Sprintf("%+v : %+v", userAffiliationsEmail, err)
		log.Println(errMessage)
	}
	enriched.MboxAuthorDomain = e.GetEmailDomain(userAffiliationsEmail)
	// if user is already affiliated
	if userData != nil {
		if userData.ID != nil {
			enriched.AuthorID = *userData.ID
		}
		if userData.Name != "" {
			enriched.AuthorName = userData.Name
		}
		if userData.OrgName != nil {
			enriched.AuthorOrgName = *userData.OrgName
		}
		if userData.UUID != nil {
			enriched.AuthorUUID = *userData.UUID
		}

		if userData.UUID != nil {
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
		}

		if userData.IsBot != nil {
			if *userData.IsBot == 1 {
				enriched.AuthorBot = true
			}
		}
	} else {
		// add new affiliation if email format is valid
		if ok := e.IsValidEmail(userAffiliationsEmail); ok {
			name := e.GetUserName(rawMessage.Data.From)
			source := Pipermail
			authorUUID, err := uuid.GenerateIdentity(&source, &userAffiliationsEmail, &name, nil)
			if err != nil {
				errMessage := fmt.Sprintf("%+v : %+v", authorUUID, err)
				log.Println(errMessage)
				return nil, err
			}

			userIdentity := affiliation.Identity{
				LastModified: time.Now(),
				Name:         name,
				Source:       source,
				Email:        userAffiliationsEmail,
				UUID:         authorUUID,
			}
			if ok := e.affiliationsClientProvider.AddIdentity(&userIdentity); !ok {
				log.Printf("failed to add identity for [%+v]", userAffiliationsEmail)
			}

			enriched.AuthorID = authorUUID
			enriched.AuthorName = name
			enriched.AuthorUUID = authorUUID
		}
	}

	return &enriched, nil
}

// IsValidEmail validates email string
func (e *Enricher) IsValidEmail(rawMailString string) bool {
	if strings.Contains(rawMailString, "...") {
		log.Println("email contains ellipsis")
		return false
	}

	if ok := emailRegex.MatchString(rawMailString); !ok {
		log.Println("invalid email pattern: ", rawMailString)
	}

	return true
}

// GetGroupName parses given url string and returns the path
func (e *Enricher) GetGroupName(s string) string {
	u, err := url.Parse(s)
	if err != nil {
		log.Println(err)
		return ""
	}
	return u.Path
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
	return &value
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

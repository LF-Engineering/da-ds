package googlegroups

import (
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/LF-Engineering/dev-analytics-libraries/affiliation"
	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
	"github.com/LF-Engineering/dev-analytics-libraries/uuid"
)

var emailRegex = regexp.MustCompile("^[a-zA-Z0-9.!#$%&'*+\\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")

// AffiliationClient manages user identity
type AffiliationClient interface {
	GetIdentityByUser(key string, value string) (*affiliation.AffIdentity, error)
	AddIdentity(identity *affiliation.Identity) bool
	GetOrganizations(uuid string, projectSlug string) *[]affiliation.Enrollment
}

// Enricher contains google groups datasource enrich logic
type Enricher struct {
	DSName                     string // Datasource will be used as key for ES
	ElasticSearchProvider      *elastic.ClientProvider
	affiliationsClientProvider AffiliationClient
}

// NewEnricher initiates a new Enricher
func NewEnricher(esClientProvider *elastic.ClientProvider, affiliationsClientProvider *affiliation.Affiliation) *Enricher {
	return &Enricher{
		DSName:                     GoogleGroups,
		ElasticSearchProvider:      esClientProvider,
		affiliationsClientProvider: affiliationsClientProvider,
	}
}

// EnrichMessage enriches raw message
func (e *Enricher) EnrichMessage(rawMessage *RawMessage, now time.Time) (*EnrichedMessage, error) {
	enrichedMessage := EnrichedMessage{
		From:                 rawMessage.From,
		Date:                 rawMessage.Date,
		To:                   rawMessage.To,
		MessageID:            rawMessage.MessageID,
		InReplyTo:            rawMessage.InReplyTo,
		References:           rawMessage.References,
		Subject:              rawMessage.Subject,
		Topic:                rawMessage.Topic,
		MessageBody:          rawMessage.MessageBody,
		TopicID:              rawMessage.TopicID,
		BackendVersion:       rawMessage.BackendVersion,
		UUID:                 rawMessage.UUID,
		MetadataUpdatedOn:    rawMessage.MetadataUpdatedOn,
		MetadataTimestamp:    rawMessage.MetadataTimestamp,
		MetadataEnrichedOn:   now,
		BackendName:          fmt.Sprintf("%sEnrich", strings.Title(e.DSName)),
		ProjectSlug:          rawMessage.ProjectSlug,
		GroupName:            rawMessage.GroupName,
		Project:              rawMessage.Project,
		ChangedAt:            rawMessage.ChangedAt,
		IsGoogleGroupMessage: 1,
		Origin:               rawMessage.Origin,
		AuthorMultiOrgNames:  []string{Unknown},
		AuthorOrgName:        Unknown,
		Timezone:             rawMessage.Timezone,
		ViaCommunityGroup:    false,
	}

	if rawMessage.InReplyTo == "" {
		enrichedMessage.Root = true
	}

	affsEmail := e.GetEmailAddress(rawMessage.From)
	if affsEmail == nil {
		str := fmt.Sprintf("missing email address: messageID in raw doc: [%+v]", rawMessage.MessageID)
		log.Println(str)
		return nil, errors.New(str)
	}
	userAffiliationsEmail := *affsEmail
	enrichedMessage.MboxAuthorDomain = e.GetEmailDomain(userAffiliationsEmail)
	enrichedMessage.From = e.GetUserName(rawMessage.From)
	enrichedMessage.AuthorName = e.GetUserName(rawMessage.From)
	source := GoogleGroups
	name := enrichedMessage.AuthorName
	authorUUID, err := uuid.GenerateIdentity(&source, &userAffiliationsEmail, &name, nil)
	if err != nil {
		log.Println(err)
	}

	userData := new(affiliation.AffIdentity)
	if strings.Contains(enrichedMessage.AuthorName, " via ") {
		enrichedMessage.AuthorName = strings.Split(enrichedMessage.AuthorName, " via ")[0]
		enrichedMessage.ViaCommunityGroup = true
	}

	if strings.Contains(enrichedMessage.AuthorName, "(Jira)") {
		enrichedMessage.AuthorName = strings.Replace(enrichedMessage.AuthorName, "(Jira)", "", -1)
		enrichedMessage.ViaCommunityGroup = true
	}

	if strings.Contains(enrichedMessage.From, " via ") || strings.Contains(enrichedMessage.From, "(Jira)") {
		name := enrichedMessage.AuthorName
		source := GoogleGroups
		authorUUID, _ := uuid.GenerateIdentity(&source, &userAffiliationsEmail, &name, nil)
		userData, err = e.affiliationsClientProvider.GetIdentityByUser("id", authorUUID)
		if err != nil {
			errMessage := fmt.Sprintf("%+v : %+v", userAffiliationsEmail, err)
			log.Println(errMessage)
		}

	}

	if ok := e.IsValidEmail(userAffiliationsEmail); ok {
		userIdentity := affiliation.Identity{
			LastModified: now,
			Name:         name,
			Source:       source,
			Email:        userAffiliationsEmail,
			ID:           authorUUID,
		}

		if ok := e.affiliationsClientProvider.AddIdentity(&userIdentity); !ok {
			log.Printf("failed to add identity for [%+v]", userAffiliationsEmail)
		}

		enrichedMessage.AuthorID = authorUUID
		enrichedMessage.AuthorUUID = authorUUID
		enrichedMessage.AuthorName = name
	}

	// get user by id instead of uuid because when a user is merged to a profile, they get
	// the uuid of the profile and it might not match with the user id
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
			if strings.Contains(slug, "finos") {
				slug = "finos-f"
			}
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
				enrichedMessage.FromBot = true
				enrichedMessage.AuthorBot = true
			}
		}
	}

	return &enrichedMessage, nil
}

// GetEmailAddress ...
func (e *Enricher) GetEmailAddress(rawMailString string) (mail *string) {
	trimBraces := strings.Split(rawMailString, " <")
	if len(trimBraces) > 1 {
		email := strings.TrimSpace(trimBraces[1])
		email = strings.TrimSpace(strings.Replace(email, ">", "", 1))
		mail = e.RemoveSpecialCharactersFromString(email)
		return
	}

	if ok := emailRegex.MatchString(trimBraces[0]); ok {
		return e.RemoveSpecialCharactersFromString(trimBraces[0])
	}
	return
}

// GetEmailDomain ...
func (e *Enricher) GetEmailDomain(email string) string {
	domain := strings.Split(email, "@")
	if len(domain) > 1 {
		return domain[1]
	}
	return ""
}

// GetEmailUsername ...
func (e *Enricher) GetEmailUsername(email string) string {
	username := strings.Split(email, "@")
	if len(username) > 1 {
		return username[0]
	}
	return email
}

// GetUserName ...
func (e *Enricher) GetUserName(rawMailString string) (username string) {
	trimBraces := strings.Split(rawMailString, " <")
	if len(trimBraces) > 1 {
		username = strings.TrimSpace(trimBraces[0])
		username = strings.TrimSpace(strings.Replace(username, ")", "", 1))
		// check for square braces [...]
		if strings.Contains(username, "[") {
			trimSquareBraces := strings.Split(username, " [")
			username = strings.TrimSpace(trimSquareBraces[0])
		}
	}
	if strings.TrimSpace(username) != "" {
		return e.GetEmailUsername(username)
	}

	return e.GetEmailUsername(trimBraces[1])
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

// Find takes a slice and looks for an element in it. If found it will
// return it's true, otherwise it will return false.
func (e *Enricher) Find(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

// SanitizeEmails returns the first well formed email address for a given user
// it filters out norely emails and emails with ellipsis
func (e *Enricher) SanitizeEmails(emails []string) string {
	validEmails := make([]string, 0)
	if len(emails) > 0 {
		for _, email := range emails {
			if strings.Contains(email, "noreply") {
				continue
			}

			if strings.Contains(email, "...") {
				continue
			}

			validEmails = append(validEmails, email)
		}
		return validEmails[0]
	}
	return ""
}

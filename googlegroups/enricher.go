package googlegroups

import (
	"fmt"
	"github.com/LF-Engineering/dev-analytics-libraries/uuid"
	"github.com/badoux/checkmail"
	"log"
	"strings"
	"time"

	"github.com/LF-Engineering/dev-analytics-libraries/affiliation"
	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
)

// Enricher contains google groups datasource enrich logic
type Enricher struct {
	DSName                     string // Datasource will be used as key for ES
	ElasticSearchProvider      *elastic.ClientProvider
	affiliationsClientProvider *affiliation.Affiliation
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
	log.Println("In EnrichMessage")
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
		UpdatedOn:            rawMessage.UpdatedOn,
		Timestamp:            rawMessage.Timestamp,
		BackendName:          fmt.Sprintf("%sEnrich", strings.Title(e.DSName)),
		ProjectSlug:          rawMessage.ProjectSlug,
		GroupName:            rawMessage.GroupName,
		Project:              rawMessage.Project,
		ChangedAt:            rawMessage.ChangedAt,
		IsGoogleGroupMessage: 1,
		Origin:               rawMessage.Origin,
		AuthorMultiOrgNames:  []string{Unknown},
		AuthorOrgName: Unknown,
		AuthorGender: Unknown,

	}

	if rawMessage.InReplyTo == "" {
		enrichedMessage.Root = true
	}

	userAffiliationsEmail := e.GetEmailAddress(rawMessage.From)
	enrichedMessage.MboxAuthorDomain = e.GetEmailDomain(userAffiliationsEmail)
	enrichedMessage.From = e.GetUserName(rawMessage.From)
	enrichedMessage.AuthorName = e.GetUserName(rawMessage.From)

	userData, err := e.affiliationsClientProvider.GetIdentityByUser("email", userAffiliationsEmail)
	if err != nil {
		log.Println(err)
	}

	if userData != nil {
		// handle affiliations if user exists
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
		if userData.Gender != nil {
			enrichedMessage.AuthorGender = *userData.Gender
		}

		if userData.GenderACC != nil {
			enrichedMessage.AuthorGenderAcc = *userData.GenderACC
		}

		enrollments := e.affiliationsClientProvider.GetOrganizations(*userData.UUID, rawMessage.ProjectSlug)
		if enrollments != nil {
			organizations := make([]string, 0)
			for _, enrollment := range *enrollments {
				organizations = append(organizations, enrollment.Organization.Name)
			}

			if len(organizations) != 0 {
				enrichedMessage.AuthorMultiOrgNames = organizations
			}
		}

		if *userData.IsBot == 1 {
			enrichedMessage.FromBot = true
		}
	} else {
		// add new affiliation if email format is valid
		if ok := e.IsValidEmail(userAffiliationsEmail); ok {
			name := e.GetUserName(rawMessage.From)
			source := GoogleGroups
			authorUUID, err := uuid.GenerateIdentity(&source, &userAffiliationsEmail, &name, nil)
			if err == nil {
				userIdentity := affiliation.Identity{
					LastModified: time.Now(),
					Name:         name,
					Source:       source,
					Email:        userAffiliationsEmail,
					UUID:         authorUUID,
				}

				if ok := e.affiliationsClientProvider.AddIdentity(&userIdentity); !ok {
					log.Printf("failed to add identity for [%+v]", userAffiliationsEmail)
				} else {
					log.Printf("added identity for [%+v]", name)
					multipleOrganizations := []string{Unknown}

					// add user data to enriched object
					enrichedMessage.AuthorID = authorUUID
					enrichedMessage.AuthorUUID = authorUUID
					enrichedMessage.AuthorName = name
					enrichedMessage.AuthorMultiOrgNames = multipleOrganizations
				}
			}
			log.Println(err)
		}
	}

	return &enrichedMessage, nil
}

// GetEmailAddress ...
func (e *Enricher) GetEmailAddress(rawMailString string) (email string) {
	trimBraces := strings.Split(rawMailString, " <")
	if len(trimBraces) > 1 {
		email = strings.TrimSpace(trimBraces[1])
		email = strings.TrimSpace(strings.Replace(email, ">", "", 1))
		return
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
			return
		}
		return
	}
	return trimBraces[0]
}

// IsValidEmail validates email string
func (e *Enricher) IsValidEmail(rawMailString string) bool {
	if strings.Contains(rawMailString, "...") {
		log.Println("email contains ellipsis")
		return false
	}

	err := checkmail.ValidateFormat(rawMailString)
	if err != nil {
		log.Println("invalid email format: ", err)
		return false
	}

	return true
}

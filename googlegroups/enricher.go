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
		From:                   rawMessage.From,
		Date:                   rawMessage.Date,
		To:                     rawMessage.To,
		MessageID:              rawMessage.MessageID,
		InReplyTo:              rawMessage.InReplyTo,
		References:             rawMessage.References,
		Subject:                rawMessage.Subject,
		MessageBody:            rawMessage.MessageBody,
		TopicID:                rawMessage.TopicID,
		BackendVersion:         rawMessage.BackendVersion,
		UUID:                   rawMessage.UUID,
		MetadataUpdatedOn:      rawMessage.MetadataUpdatedOn,
		BackendName:            fmt.Sprintf("%sEnrich", strings.Title(e.DSName)),
		MetadataTimestamp:      rawMessage.MetadataTimestamp,
		MetadataEnrichedOn:     now,
		ProjectSlug:            rawMessage.ProjectSlug,
		GroupName:              rawMessage.GroupName,
		Project:                rawMessage.Project,
		ChangedAt:              rawMessage.ChangedAt,
		IsGoogleGroupMessage:   1,
		AuthorMultipleOrgNames: []string{Unknown},
	}

	if rawMessage.InReplyTo == "" {
		enrichedMessage.Root = true
	}

	userAffiliationsEmail := e.GetEmailAddress(rawMessage.From)
	enrichedMessage.EmailDomain = e.GetEmailDomain(userAffiliationsEmail)

	userData, err := e.affiliationsClientProvider.GetIdentityByUser("email", userAffiliationsEmail)
	if err != nil {
		fmt.Println(err)
		return nil, err
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
			enrichedMessage.FromOrgName = *userData.OrgName
		}
		if userData.UUID != nil {
			enrichedMessage.AuthorUUID = *userData.UUID
		}
		if userData.Gender != nil {
			enrichedMessage.AuthorGender = *userData.Gender
			enrichedMessage.FromGender = *userData.Gender
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
				enrichedMessage.AuthorMultipleOrgNames = organizations
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
			if err != nil {
				fmt.Println(err)
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
			} else {
				log.Printf("added identity for [%+v]", name)
				multipleOrganizations := []string{Unknown}

				// add user data to enriched object
				enrichedMessage.AuthorID = authorUUID
				enrichedMessage.AuthorUUID = authorUUID
				enrichedMessage.AuthorName = name
				enrichedMessage.FromOrgName = Unknown
				enrichedMessage.AuthorGender = Unknown
				enrichedMessage.AuthorMultipleOrgNames = multipleOrganizations
			}
		}
	}

	return &enrichedMessage, nil
}

// GetEmailAddress ...
func (e *Enricher) GetEmailAddress(rawMailString string) (email string) {
	trimBraces := strings.Split(rawMailString, " <")
	email = strings.TrimSpace(trimBraces[1])
	email = strings.TrimSpace(strings.Replace(email, ">", "", 1))
	return
}

// GetEmailDomain ...
func (e *Enricher) GetEmailDomain(email string) string {
	domain := strings.Split(email, "@")
	return domain[1]
}

// GetUserName ...
func (e *Enricher) GetUserName(rawMailString string) (username string) {
	trimBraces := strings.Split(rawMailString, " <")
	username = strings.TrimSpace(trimBraces[0])
	username = strings.TrimSpace(strings.Replace(username, ")", "", 1))
	return
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

package bugzilla

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/LF-Engineering/dev-analytics-libraries/uuid"

	"github.com/LF-Engineering/da-ds/affiliation"
	libAffiliations "github.com/LF-Engineering/dev-analytics-libraries/affiliation"
	timeLib "github.com/LF-Engineering/dev-analytics-libraries/time"
)

// Enricher enrich Bugzilla raw
type Enricher struct {
	identityProvider           IdentityProvider
	DSName                     string
	BackendVersion             string
	Project                    string
	affiliationsClientProvider *libAffiliations.Affiliation
}

// IdentityProvider manages user identity
type IdentityProvider interface {
	GetIdentity(key string, val string) (*affiliation.Identity, error)
	GetOrganizations(uuid string, date time.Time) ([]string, error)
	CreateIdentity(ident affiliation.Identity, source string) error
}

// NewEnricher intiate a new enricher instance
func NewEnricher(identProvider IdentityProvider, backendVersion string, project string, affiliationsClientProvider *libAffiliations.Affiliation) *Enricher {
	return &Enricher{
		identityProvider:           identProvider,
		DSName:                     Bugzilla,
		BackendVersion:             backendVersion,
		Project:                    project,
		affiliationsClientProvider: affiliationsClientProvider,
	}
}

// EnrichItem enrich Bugzilla raw item
func (e *Enricher) EnrichItem(rawItem BugRaw, now time.Time) (*BugEnrich, error) {
	enriched := &BugEnrich{}

	enriched.Category = "bug"
	enriched.Project = e.Project
	enriched.ChangedDate = rawItem.ChangedAt
	enriched.DeltaTs = rawItem.DeltaTs
	enriched.Changes = rawItem.ActivityCount
	enriched.Labels = rawItem.Keywords
	enriched.Priority = rawItem.Priority
	enriched.Severity = rawItem.Severity
	enriched.OpSys = rawItem.OpSys
	enriched.Product = rawItem.Product
	enriched.Component = rawItem.Component
	enriched.Platform = rawItem.RepPlatform

	enriched.Tag = rawItem.Tag
	enriched.UUID = rawItem.UUID
	enriched.MetadataUpdatedOn = rawItem.MetadataUpdatedOn
	enriched.MetadataTimestamp = rawItem.MetadataTimestamp
	enriched.MetadataEnrichedOn = now
	enriched.MetadataFilterRaw = nil
	enriched.MetadataBackendName = fmt.Sprintf("%sEnrich", strings.Title(e.DSName))
	enriched.MetadataBackendVersion = e.BackendVersion
	enriched.IsBugzillaBug = 1
	enriched.URL = rawItem.Origin + "/show_bug.cgi?id=" + fmt.Sprint(rawItem.BugID)
	enriched.CreationDate = rawItem.CreationTS

	enriched.ResolutionDays = timeLib.GetDaysBetweenDates(enriched.DeltaTs, enriched.CreationDate)
	if rawItem.StatusWhiteboard != "" {
		enriched.Whiteboard = rawItem.StatusWhiteboard
	}
	unknown := "Unknown"
	multiOrgs := []string{unknown}

	if rawItem.Assignee.Username != "" && rawItem.Assignee.Name != "" {
		enriched.Assigned = rawItem.Assignee.Username

		// Enrich assigned to
		assignedToFieldName := "username"
		if strings.Contains(rawItem.AssignedTo, "@") {
			assignedToFieldName = "email"
		}

		assignedTo, err := e.identityProvider.GetIdentity(assignedToFieldName, rawItem.Assignee.Username)
		if err == nil {
			enriched.AssignedToID = assignedTo.ID.String
			enriched.AssignedToUUID = assignedTo.UUID.String
			enriched.AssignedToName = assignedTo.Name.String
			enriched.AssignedToUserName = assignedTo.Username.String
			enriched.AssignedToDomain = assignedTo.Domain.String
			enriched.AssignedToBot = assignedTo.IsBot

			if assignedTo.Gender.Valid {
				enriched.AssignedToGender = assignedTo.Gender.String
			} else {
				enriched.AssignedToGender = unknown
			}

			if assignedTo.GenderACC != nil {
				enriched.AssignedToGenderAcc = *assignedTo.GenderACC
			} else {
				enriched.AssignedToGenderAcc = 0
			}

			if assignedTo.OrgName.Valid {
				enriched.AssignedToOrgName = assignedTo.OrgName.String
			} else {
				enriched.AssignedToOrgName = unknown
			}

			assignedToMultiOrg, err := e.identityProvider.GetOrganizations(assignedTo.UUID.String, enriched.MetadataUpdatedOn)
			if err == nil {
				enriched.AssignedToMultiOrgName = multiOrgs

				if len(assignedToMultiOrg) != 0 {
					enriched.AssignedToMultiOrgName = assignedToMultiOrg
				}
			}
		} else {
			assignee := rawItem.Assignee
			source := Bugzilla
			authorUUID, err := uuid.GenerateIdentity(&source, &assignee.Username, &assignee.Name, nil)
			if err != nil {
				fmt.Println(err)
				return nil, err
			}

			userIdentity := libAffiliations.Identity{
				LastModified: time.Now(),
				Name:         assignee.Name,
				Source:       Bugzilla,
				Email:        assignee.Username,
				UUID:         authorUUID,
			}

			ok := e.affiliationsClientProvider.AddIdentity(&userIdentity)
			if !ok {
				log.Printf("failed to add identity for [%+v]", assignee.Username)
			} else {
				log.Printf("added identity for [%+v]", assignee.Username)
				// add enriched data
				enriched.AssignedToID = authorUUID
				enriched.AssignedToUUID = authorUUID
				enriched.AssignedToName = assignee.Name
				enriched.AssignedToUserName = assignee.Username
				enriched.AssignedToOrgName = unknown
				enriched.AssignedToMultiOrgName = multiOrgs

			}
		}
	}

	if rawItem.Reporter.Username != "" {
		enriched.ReporterUserName = rawItem.Reporter.Username
		enriched.AuthorName = rawItem.Reporter.Username

		// Enrich reporter
		reporterFieldName := "username"
		if strings.Contains(enriched.ReporterUserName, "@") {
			reporterFieldName = "email"
		}

		reporter, err := e.identityProvider.GetIdentity(reporterFieldName, enriched.ReporterUserName)

		if err == nil {
			enriched.ReporterID = reporter.ID.String
			enriched.ReporterUUID = reporter.UUID.String
			enriched.ReporterName = reporter.Name.String
			enriched.ReporterUserName = reporter.Username.String
			enriched.ReporterDomain = reporter.Domain.String

			enriched.AuthorID = reporter.ID.String
			enriched.AuthorUUID = reporter.UUID.String
			enriched.AuthorName = reporter.Name.String
			enriched.AuthorUserName = reporter.Username.String
			enriched.AuthorDomain = reporter.Domain.String

			if reporter.Gender.Valid {
				enriched.ReporterGender = reporter.Gender.String
				enriched.AuthorGender = reporter.Gender.String
			} else {
				enriched.ReporterGender = unknown
				enriched.AuthorGender = unknown
			}
			if reporter.GenderACC != nil {
				enriched.ReporterGenderACC = *reporter.GenderACC
				enriched.AuthorGenderAcc = *reporter.GenderACC
			} else {
				enriched.ReporterGenderACC = 0
				enriched.AuthorGenderAcc = 0
			}
			if reporter.OrgName.Valid {
				enriched.ReporterOrgName = reporter.OrgName.String
				enriched.AuthorOrgName = reporter.OrgName.String
			} else {
				enriched.ReporterOrgName = unknown
				enriched.AuthorOrgName = unknown
			}

			enriched.ReporterBot = reporter.IsBot
			enriched.AuthorBot = reporter.IsBot

			reporterMultiOrg, err := e.identityProvider.GetOrganizations(reporter.UUID.String, enriched.MetadataUpdatedOn)
			if err == nil {
				enriched.ReporterMultiOrgName = multiOrgs
				enriched.AuthorMultiOrgName = multiOrgs

				if len(reporterMultiOrg) != 0 {
					enriched.ReporterMultiOrgName = reporterMultiOrg
					enriched.AuthorMultiOrgName = reporterMultiOrg
				}
			}
		} else {
			reporter := rawItem.Reporter
			source := Bugzilla
			authorUUID, err := uuid.GenerateIdentity(&source, &reporter.Name, &reporter.Username, nil)
			if err != nil {
				return nil, err
			}

			userIdentity := libAffiliations.Identity{
				LastModified: time.Now(),
				Name:         reporter.Name,
				Source:       Bugzilla,
				Email:        reporter.Name,
				UUID:         authorUUID,
			}

			ok := e.affiliationsClientProvider.AddIdentity(&userIdentity)
			if !ok {
				log.Printf("failed to add identity for [%+v]", reporter.Username)
			} else {
				log.Printf("added identity for [%+v]", reporter.Name)
				// add enriched data
				enriched.ReporterID = authorUUID
				enriched.ReporterUUID = authorUUID
				enriched.ReporterName = reporter.Name
				enriched.ReporterUserName = reporter.Username

				enriched.AuthorID = authorUUID
				enriched.AuthorUUID = authorUUID
				enriched.AuthorName = reporter.Name
				enriched.AuthorUserName = reporter.Username

				enriched.ReporterOrgName = unknown
				enriched.ReporterMultiOrgName = multiOrgs

				enriched.AuthorOrgName = unknown
				enriched.AuthorMultiOrgName = multiOrgs

			}
		}

	}
	if rawItem.Resolution != "" {
		enriched.Resolution = rawItem.Resolution
	}
	if rawItem.ShortDescription != "" {
		enriched.MainDescription = rawItem.ShortDescription
		enriched.MainDescriptionAnalyzed = rawItem.ShortDescription
	}
	if rawItem.Summary != "" {
		enriched.Summary = rawItem.Summary
		enriched.SummaryAnalyzed = rawItem.Summary[:1000]
	}

	enriched.Status = rawItem.BugStatus
	enriched.BugID = rawItem.BugID
	enriched.Comments = 0
	if len(rawItem.LongDesc) > 0 {
		enriched.Comments = len(rawItem.LongDesc)
	}
	enriched.RepositoryLabels = nil

	return enriched, nil
}

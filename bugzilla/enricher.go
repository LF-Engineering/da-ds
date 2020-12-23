package bugzilla

import (
	"fmt"
	"strings"
	"time"

	"github.com/LF-Engineering/da-ds/affiliation"

	timeLib "github.com/LF-Engineering/dev-analytics-libraries/time"
)

// Enricher enrich Bugzilla raw
type Enricher struct {
	identityProvider IdentityProvider
	DSName           string
	BackendVersion   string
	Project          string
}

// IdentityProvider manages user identity
type IdentityProvider interface {
	GetIdentity(key string, val string) (*affiliation.Identity, error)
	GetOrganizations(uuid string, date time.Time) ([]string, error)
}

// NewEnricher intiate a new enricher instance
func NewEnricher(identProvider IdentityProvider, backendVersion string, project string) *Enricher {
	return &Enricher{
		identityProvider: identProvider,
		DSName:           Bugzilla,
		BackendVersion:   backendVersion,
		Project:          project,
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
	if rawItem.AssignedTo != "" {
		enriched.Assigned = rawItem.AssignedTo

		// Enrich assigned to
		assignedToFieldName := "username"
		if strings.Contains(rawItem.AssignedTo, "@") {
			assignedToFieldName = "email"
		}

		assignedTo, err := e.identityProvider.GetIdentity(assignedToFieldName, enriched.Assigned)
		if err == nil {
			enriched.AssignedToID = assignedTo.ID
			enriched.AssignedToUUID = assignedTo.UUID
			enriched.AssignedToName = assignedTo.Name
			enriched.AssignedToUserName = assignedTo.Username
			enriched.AssignedToDomain = assignedTo.Domain
			enriched.AssignedToBot = assignedTo.IsBot

			if assignedTo.Gender != nil {
				enriched.AssignedToGender = *assignedTo.Gender
			} else {
				enriched.AssignedToGender = unknown
			}

			if assignedTo.GenderACC != nil {
				enriched.AssignedToGenderAcc = *assignedTo.GenderACC
			} else {
				enriched.AssignedToGenderAcc = 0
			}

			if assignedTo.OrgName != nil {
				enriched.AssignedToOrgName = *assignedTo.OrgName
			} else {
				enriched.AssignedToOrgName = unknown
			}

			assignedToMultiOrg, err := e.identityProvider.GetOrganizations(assignedTo.UUID, enriched.MetadataUpdatedOn)
			if err == nil {
				enriched.AssignedToMultiOrgName = multiOrgs

				if len(assignedToMultiOrg) != 0 {
					enriched.AssignedToMultiOrgName = assignedToMultiOrg
				}
			}
		}
	}

	if rawItem.Reporter != "" {
		enriched.ReporterUserName = rawItem.Reporter
		enriched.AuthorName = rawItem.Reporter

		// Enrich reporter
		reporterFieldName := "username"
		if strings.Contains(enriched.ReporterUserName, "@") {
			reporterFieldName = "email"
		}

		reporter, err := e.identityProvider.GetIdentity(reporterFieldName, enriched.ReporterUserName)

		if err == nil {
			enriched.ReporterID = reporter.ID
			enriched.ReporterUUID = reporter.UUID
			enriched.ReporterName = reporter.Name
			enriched.ReporterUserName = reporter.Username
			enriched.ReporterDomain = reporter.Domain

			enriched.AuthorID = reporter.ID
			enriched.AuthorUUID = reporter.UUID
			enriched.AuthorName = reporter.Name
			enriched.AuthorUserName = reporter.Username
			enriched.AuthorDomain = reporter.Domain

			if reporter.Gender != nil {
				enriched.ReporterGender = *reporter.Gender
				enriched.AuthorGender = *reporter.Gender
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
			if reporter.OrgName != nil {
				enriched.ReporterOrgName = *reporter.OrgName
				enriched.AuthorOrgName = *reporter.OrgName
			} else {
				enriched.ReporterOrgName = unknown
				enriched.AuthorOrgName = unknown
			}

			enriched.ReporterBot = reporter.IsBot
			enriched.AuthorBot = reporter.IsBot

			reporterMultiOrg, err := e.identityProvider.GetOrganizations(reporter.UUID, enriched.MetadataUpdatedOn)
			if err == nil {
				enriched.ReporterMultiOrgName = multiOrgs
				enriched.AuthorMultiOrgName = multiOrgs

				if len(reporterMultiOrg) != 0 {
					enriched.ReporterMultiOrgName = reporterMultiOrg
					enriched.AuthorMultiOrgName = reporterMultiOrg
				}
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

// EnrichAffiliation gets author SH identity data
func (e *Enricher) EnrichAffiliation(key string, val string) (*affiliation.Identity, error) {
	return e.identityProvider.GetIdentity(key, val)
}

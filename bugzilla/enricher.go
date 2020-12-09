package bugzilla

import (
	"fmt"
	"strings"
	"time"

	"github.com/LF-Engineering/da-ds/affiliation"

	"github.com/LF-Engineering/da-ds/utils"
)

// Enricher ...
type Enricher struct {
	identityProvider IdentityProvider
}

type IdentityProvider interface {
	GetIdentity(key string, val string) (*affiliation.Identity, error)
}

// NewEnricher
func NewEnricher(identProvider IdentityProvider) *Enricher {
	return &Enricher{
		identityProvider: identProvider,
	}
}

func (e *Enricher) EnrichItem(rawItem BugRaw, now time.Time) (*EnrichedItem, error) {
	enriched := &EnrichedItem{}

	enriched.Category = "bug"
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
	enriched.MetadataEnrichedOn = rawItem.MetadataUpdatedOn
	enriched.MetadataFilterRaw = nil
	enriched.IsBugzillaBug = 1
	enriched.Url = rawItem.Origin + "/show_bug.cgi?id=" + fmt.Sprint(rawItem.BugID)
	enriched.CreationDate = rawItem.CreationTS

	enriched.ResolutionDays = utils.GetDaysbetweenDates(enriched.DeltaTs, enriched.CreationDate)
	//enriched.TimeOpenDays = utils.GetDaysbetweenDates(enriched.CreationDate, enriched.MetadataUpdatedOn)
	if rawItem.StatusWhiteboard != "" {
		enriched.Whiteboard = rawItem.StatusWhiteboard
	}
	if rawItem.AssignedTo != "" {
		enriched.Assigned = rawItem.AssignedTo

		// Enrich reporter
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
			enriched.AssignedToMultiOrgName = assignedTo.MultiOrgNames
			enriched.AssignedToBot = assignedTo.IsBot

			if assignedTo.Gender != nil {
				enriched.AssignedToGender = *assignedTo.Gender
			}
			if assignedTo.GenderACC != nil {
				enriched.AssignedToGenderAcc = *assignedTo.GenderACC
			}
			if assignedTo.OrgName != nil {
				enriched.AssignedToOrgName = *assignedTo.OrgName
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
			}
			if reporter.GenderACC != nil {
				enriched.ReporterGenderACC = *reporter.GenderACC
				enriched.AuthorGenderAcc = *reporter.GenderACC
			}
			if reporter.OrgName != nil {
				enriched.ReporterOrgName = *reporter.OrgName
				enriched.AuthorOrgName = *reporter.OrgName
			}

			enriched.ReporterMultiOrgName = reporter.MultiOrgNames
			enriched.ReporterBot = reporter.IsBot

			enriched.AuthorMultiOrgName = reporter.MultiOrgNames
			enriched.AuthorBot = reporter.IsBot
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
	enriched.BugId = rawItem.BugID
	enriched.Comments = 0
	if len(rawItem.LongDesc) > 0 {
		enriched.Comments = len(rawItem.LongDesc)
	}
	enriched.LongDesc = len(rawItem.LongDesc)

	return enriched, nil
}

// EnrichAffiliation gets author SH identity data
func (e *Enricher) EnrichAffiliation(key string, val string) (*affiliation.Identity, error) {
	return e.identityProvider.GetIdentity(key, val)
}

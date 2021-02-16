package bugzilla

import (
	"fmt"
	"log"
	"math"
	"strings"
	"time"

	"github.com/LF-Engineering/dev-analytics-libraries/uuid"

	libAffiliations "github.com/LF-Engineering/dev-analytics-libraries/affiliation"
	timeLib "github.com/LF-Engineering/dev-analytics-libraries/time"
)

// Enricher enrich Bugzilla raw
type Enricher struct {
	DSName             string
	BackendVersion     string
	Project            string
	affiliationsClient AffiliationClient
}

// AffiliationClient manages user identity
type AffiliationClient interface {
	GetIdentityByUser(key string, value string) (*libAffiliations.AffIdentity, error)
	AddIdentity(identity *libAffiliations.Identity) bool
}

// NewEnricher intiate a new enricher instance
func NewEnricher(backendVersion string, project string, affiliationsClient AffiliationClient) *Enricher {
	return &Enricher{
		DSName:             Bugzilla,
		BackendVersion:     backendVersion,
		Project:            project,
		affiliationsClient: affiliationsClient,
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

	enriched.ResolutionDays = math.Round(timeLib.GetDaysBetweenDates(enriched.DeltaTs, enriched.CreationDate)*100) / 100
	if rawItem.StatusWhiteboard != "" {
		enriched.Whiteboard = rawItem.StatusWhiteboard
	}
	unknown := "Unknown"
	multiOrgs := []string{unknown}

	if enriched.Labels == nil {
		enriched.Labels = make([]string, 0)
	}

	if rawItem.Assignee.Username != "" && rawItem.Assignee.Name != "" {
		enriched.Assigned = rawItem.Assignee.Username

		// Enrich assigned to
		assignedToFieldName := "username"
		if strings.Contains(rawItem.AssignedTo, "@") {
			assignedToFieldName = "email"
		}
		assignedTo, err := e.affiliationsClient.GetIdentityByUser(assignedToFieldName, rawItem.Assignee.Username)
		if err == nil {
			enriched.AssignedToID = *assignedTo.ID
			enriched.AssignedToUUID = *assignedTo.UUID
			enriched.AssignedToName = assignedTo.Name
			enriched.AssignedToUserName = assignedTo.Username
			enriched.AssignedToDomain = assignedTo.Domain
			if *assignedTo.IsBot != 0 {
				enriched.AssignedToBot = true
			}

			if assignedTo.Gender != nil {
				enriched.AssignedToGender = *assignedTo.Gender
			} else {
				enriched.AssignedToGender = unknown
			}

			if assignedTo.GenderACC != nil {
				enriched.AssignedToGenderAcc = int(*assignedTo.GenderACC)
			} else {
				enriched.AssignedToGenderAcc = 0
			}

			if assignedTo.OrgName != nil {
				enriched.AssignedToOrgName = *assignedTo.OrgName
			} else {
				enriched.AssignedToOrgName = unknown
			}

			enriched.AssignedToMultiOrgName = multiOrgs

			if len(assignedTo.MultiOrgNames) != 0 {
				enriched.AssignedToMultiOrgName = assignedTo.MultiOrgNames
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

			ok := e.affiliationsClient.AddIdentity(&userIdentity)
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

		reporter, err := e.affiliationsClient.GetIdentityByUser(reporterFieldName, enriched.ReporterUserName)
		if err == nil {
			enriched.ReporterID = *reporter.ID
			enriched.ReporterUUID = *reporter.UUID
			enriched.ReporterName = reporter.Name
			enriched.ReporterUserName = reporter.Username
			enriched.ReporterDomain = reporter.Domain

			enriched.AuthorID = *reporter.ID
			enriched.AuthorUUID = *reporter.UUID
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
				enriched.ReporterGenderACC = int(*reporter.GenderACC)
				enriched.AuthorGenderAcc = int(*reporter.GenderACC)
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

			if reporter.IsBot != nil {
				enriched.ReporterBot = false
				enriched.AuthorBot = false
			}

			enriched.ReporterMultiOrgName = multiOrgs
			enriched.AuthorMultiOrgName = multiOrgs

			if len(reporter.MultiOrgNames) != 0 {
				enriched.ReporterMultiOrgName = reporter.MultiOrgNames
				enriched.AuthorMultiOrgName = reporter.MultiOrgNames
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

			ok := e.affiliationsClient.AddIdentity(&userIdentity)
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

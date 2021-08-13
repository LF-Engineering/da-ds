package bugzilla

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/LF-Engineering/da-ds/util"

	dads "github.com/LF-Engineering/da-ds"

	"github.com/LF-Engineering/dev-analytics-libraries/uuid"

	libAffiliations "github.com/LF-Engineering/dev-analytics-libraries/affiliation"
	timeLib "github.com/LF-Engineering/dev-analytics-libraries/time"
)

const (
	status     = "Status"
	resolved   = "RESOLVED"
	inProgress = "IN PROGRESS REVIEW"
)

// Enricher enrich Bugzilla raw
type Enricher struct {
	DSName             string
	BackendVersion     string
	Project            string
	affiliationsClient AffiliationClient
	auth0Client        Auth0Client
	httpClientProvider HTTPClientProvider
	affBaseURL         string
	projectSlug        string
}

// AffiliationClient manages user identity
type AffiliationClient interface {
	GetIdentityByUser(key string, value string) (*libAffiliations.AffIdentity, error)
	AddIdentity(identity *libAffiliations.Identity) bool
}

// NewEnricher intiate a new enricher instance
func NewEnricher(backendVersion string, project string, affiliationsClient AffiliationClient, auth0Client Auth0Client, httpClientProvider HTTPClientProvider, affBaseURL string, projectSlug string) *Enricher {
	return &Enricher{
		DSName:             Bugzilla,
		BackendVersion:     backendVersion,
		Project:            project,
		affiliationsClient: affiliationsClient,
		auth0Client:        auth0Client,
		httpClientProvider: httpClientProvider,
		affBaseURL:         affBaseURL,
		projectSlug:        projectSlug,
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
	enriched.TimeOpenDays = math.Round(timeLib.GetDaysBetweenDates(enriched.MetadataEnrichedOn, enriched.CreationDate)*100) / 100
	if rawItem.StatusWhiteboard != "" {
		enriched.Whiteboard = rawItem.StatusWhiteboard
	}

	isAssigned := false
	isResolved := false
	var assignedAt time.Time
	var resolvedAt time.Time

	for _, history := range rawItem.Activities {
		actiDate, err := time.Parse("2006-01-02 15:04:05 MST", history.When)
		if history.Added == status && !isAssigned {
			isAssigned = true
			assignedAt = actiDate
			if err != nil {
				continue
			}
			continue
		}
		if history.Added == resolved && !isAssigned {
			isAssigned = true
			assignedAt = actiDate
		}
		if history.Added == resolved && isAssigned {
			isResolved = true
			resolvedAt = actiDate
		}
	}

	if isAssigned {
		enriched.TimeOpenDays = math.Abs(math.Round(timeLib.GetDaysBetweenDates(assignedAt, rawItem.CreationTS)*100) / 100)
		enriched.TimeToClose = math.Abs(math.Round(timeLib.GetDaysBetweenDates(now, assignedAt)*100) / 100)
	}

	if isResolved {
		enriched.TimeToClose = math.Abs(math.Round(timeLib.GetDaysBetweenDates(resolvedAt, assignedAt)*100) / 100)
	}

	unknown := "Unknown"
	multiOrgs := []string{unknown}

	if enriched.Labels == nil {
		enriched.Labels = make([]string, 0)
	}

	if rawItem.Assignee.Username != "" && rawItem.Assignee.Name != "" {
		enriched.Assigned = rawItem.Assignee.Username

		key, value := getCont(&rawItem.Assignee)
		assignedTo, err := e.affiliationsClient.GetIdentityByUser(key, value)
		if err == nil && assignedTo != nil {
			enriched.AssignedToID = *assignedTo.ID
			enriched.AssignedToUUID = *assignedTo.UUID
			enriched.AssignedToName = assignedTo.Name
			enriched.AssignedToUserName = assignedTo.Username
			enriched.AssignedToDomain = assignedTo.Domain
			if assignedTo.IsBot != nil && *assignedTo.IsBot != 0 {
				enriched.AssignedToBot = true
			}

			org, orgs, err := util.GetEnrollments(e.auth0Client, e.httpClientProvider, e.affBaseURL, e.projectSlug, *assignedTo.UUID, rawItem.MetadataUpdatedOn)
			if err != nil {
				dads.Printf("[dads-bugzilla] EnrichItem GetEnrollments error : %+v\n", err)
			}

			enriched.AssignedToOrgName = unknown
			if org != "" && org != unknown {
				enriched.AssignedToOrgName = org
			}

			if len(orgs) != 0 && orgs[0] != unknown {
				enriched.AssignedToMultiOrgName = orgs
			}

		} else {
			assignee := rawItem.Assignee
			source := Bugzilla
			authorUUID, err := uuid.GenerateIdentity(&source, &assignee.Username, &assignee.Name, nil)
			if err != nil {
				dads.Printf("[dads-bugzilla] EnrichItem GenerateIdentity error : %+v\n", err)
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
				dads.Printf("[dads-bugzilla] EnrichItem AddIdentity failed to add identity for: %+v\n", assignee.Username)
			} else {
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

		key, value := getCont(&rawItem.Reporter)
		reporter, err := e.affiliationsClient.GetIdentityByUser(key, value)
		if err == nil && reporter != nil {
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

			org, orgs, err := util.GetEnrollments(e.auth0Client, e.httpClientProvider, e.affBaseURL, e.projectSlug, *reporter.UUID, rawItem.MetadataUpdatedOn)
			if err != nil {
				dads.Printf("[dads-bugzilla] EnrichItem GetEnrollments error : %+v\n", err)
			}

			enriched.ReporterOrgName = unknown
			enriched.AuthorOrgName = unknown
			if org != "" && org != unknown {
				enriched.ReporterOrgName = org
				enriched.AuthorOrgName = org
			}

			if reporter.IsBot != nil {
				enriched.ReporterBot = false
				enriched.AuthorBot = false
			}

			if len(orgs) != 0 && orgs[0] != unknown {
				enriched.ReporterMultiOrgName = orgs
				enriched.AuthorMultiOrgName = orgs
			}

		} else {
			reporter := rawItem.Reporter
			source := Bugzilla
			authorUUID, err := uuid.GenerateIdentity(&source, &reporter.Name, &reporter.Username, nil)
			if err != nil {
				dads.Printf("[dads-bugzilla] EnrichItem GenerateIdentity failed to generate identity for: %+v\n", err)
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
				dads.Printf("[dads-bugzilla] EnrichItem AddIdentity failed to add identity for: %+v\n", reporter.Username)
			} else {
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

func getCont(con *Person) (string, string) {
	key := "username"
	val := ""

	if con.Name != "" {
		val = con.Name
		key = "name"
		if strings.Contains(con.Name, "@") && util.IsEmailValid(con.Name) {
			key = "email"
		}
		return key, val
	}

	if con.Username != "" {
		val = con.Username
		if strings.Contains(con.Username, "@") && util.IsEmailValid(con.Username) {
			key = "email"
		}
	}

	return key, val
}

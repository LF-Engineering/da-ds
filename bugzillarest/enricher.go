package bugzillarest

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
	inProgress = "IN_PROGRESS"
	resolved   = "RESOLVED"
)

// Enricher enrich Bugzilla raw
type Enricher struct {
	DSName              string
	BackendVersion      string
	Project             string
	affiliationsClient  AffiliationClient
	auth0ClientProvider Auth0ClientProvider
	httpClientProvider  HTTPClientProvider
	affBaseURL          string
	projectSlug         string
}

// AffiliationClient manages user identity
type AffiliationClient interface {
	GetIdentityByUser(key string, value string) (*libAffiliations.AffIdentity, error)
	AddIdentity(identity *libAffiliations.Identity) bool
}

// EnricherParams required parameters for bugzilla enricher
type EnricherParams struct {
	BackendVersion string
	Project        string
}

// NewEnricher initiate a new enricher instance
func NewEnricher(params *EnricherParams, affiliationsClient AffiliationClient, auth0ClientProvider Auth0ClientProvider, httpClientProvider HTTPClientProvider, affBaseURL string, projectSlug string) *Enricher {
	return &Enricher{
		DSName:              BugzillaRest,
		BackendVersion:      params.BackendVersion,
		Project:             params.Project,
		affiliationsClient:  affiliationsClient,
		auth0ClientProvider: auth0ClientProvider,
		httpClientProvider:  httpClientProvider,
		affBaseURL:          affBaseURL,
		projectSlug:         projectSlug,
	}
}

// EnrichItem enrich Bugzilla raw item
func (e *Enricher) EnrichItem(rawItem Raw, now time.Time) (*BugRestEnrich, error) {
	enriched := &BugRestEnrich{}

	enriched.Project = e.Project
	enriched.ChangedDate = rawItem.Data.LastChangeTime
	enriched.DeltaTs = rawItem.Data.LastChangeTime
	enriched.Product = rawItem.Data.Product
	enriched.Component = rawItem.Data.Component

	enriched.Tag = rawItem.Tag
	enriched.UUID = rawItem.UUID
	enriched.MetadataUpdatedOn = rawItem.MetadataUpdatedOn
	enriched.MetadataTimestamp = rawItem.MetadataTimestamp
	enriched.MetadataEnrichedOn = now
	enriched.MetadataFilterRaw = nil
	enriched.ProjectTs = timeLib.ConvertTimeToFloat(now)
	enriched.ID = rawItem.Data.ID
	enriched.MetadataBackendName = fmt.Sprintf("%sEnrich", strings.Title(e.DSName))
	enriched.MetadataBackendVersion = e.BackendVersion
	enriched.ISBugzillarestBugrest = 1
	enriched.Origin = rawItem.Origin
	enriched.URL = rawItem.Origin + "show_bug.cgi?id=" + fmt.Sprint(rawItem.Data.ID)
	enriched.CreationDate = rawItem.Data.CreationTime
	enriched.Status = rawItem.Data.Status
	enriched.CreationTs = rawItem.Data.CreationTime.Format("2006-01-02T15:04:05")
	enriched.ISOpen = rawItem.Data.IsOpen
	enriched.TimeToLastUpdateDays = math.Abs(math.Round(timeLib.GetDaysBetweenDates(enriched.DeltaTs, rawItem.Data.CreationTime)*100) / 100)
	enriched.TimeOpenDays = math.Abs(math.Round(timeLib.GetDaysBetweenDates(now, rawItem.Data.CreationTime)*100) / 100)
	enriched.TimeToClose = enriched.TimeOpenDays

	if rawItem.Data.Whiteboard != "" {
		enriched.Whiteboard = &rawItem.Data.Whiteboard
	}

	enriched.Changes = 0
	isAssigned := false
	isResolved := false
	var assignedAt time.Time
	var resolvedAt time.Time
	for _, history := range *rawItem.Data.History {
		if len(history.Changes) > 0 {
			enriched.Changes += len(history.Changes)
		}

		for _, c := range history.Changes {
			if c.Added == inProgress && !isAssigned {
				isAssigned = true
				assignedAt = history.When
				continue
			}
			if c.Added == resolved && !isAssigned {
				isAssigned = true
				assignedAt = history.When
			}
			if c.Added == resolved && isAssigned {
				isResolved = true
				resolvedAt = history.When
			}
		}
	}

	if isAssigned {
		enriched.TimeOpenDays = math.Abs(math.Round(timeLib.GetDaysBetweenDates(assignedAt, rawItem.Data.CreationTime)*100) / 100)
		enriched.TimeToClose = math.Abs(math.Round(timeLib.GetDaysBetweenDates(now, assignedAt)*100) / 100)
	}

	if isResolved {
		enriched.TimeToClose = math.Abs(math.Round(timeLib.GetDaysBetweenDates(resolvedAt, assignedAt)*100) / 100)
	}

	enriched.NumberOfComments = 0
	enriched.Comments = len(rawItem.Data.Comments)

	if rawItem.Data.CreatorDetail != nil && rawItem.Data.CreatorDetail.RealName != "" {
		enriched.Creator = rawItem.Data.CreatorDetail.RealName
	}

	unknown := "Unknown"
	multiOrgs := []string{unknown}
	if rawItem.Data.AssignedToDetail != nil {
		enriched.AssignedTo = rawItem.Data.AssignedToDetail.RealName
		key, value := getCont(rawItem.Data.AssignedToDetail)

		assignedTo, err := e.affiliationsClient.GetIdentityByUser(key, value)
		if err == nil && assignedTo != nil {
			enriched.AssignedToDetailID = *assignedTo.ID
			enriched.AssignedToDetailUUID = *assignedTo.UUID
			enriched.AssignedToDetailName = assignedTo.Name
			enriched.AssignedToDetailUserName = assignedTo.Username
			enriched.AssignedToDetailDomain = assignedTo.Domain

			if assignedTo.IsBot != nil && *assignedTo.IsBot != 0 {
				enriched.AssignedToDetailBot = true
			}

			enriched.AssignedToUUID = *assignedTo.UUID

			org, orgs, err := util.GetEnrollments(e.auth0ClientProvider, e.httpClientProvider, e.affBaseURL, e.projectSlug, *assignedTo.UUID, rawItem.MetadataUpdatedOn)
			if err != nil {
				dads.Printf("[dads-bugzilla] EnrichItem GetEnrollments error : %+v\n", err)
			}

			enriched.AssignedToDetailOrgName = unknown
			enriched.AssignedToOrgName = unknown
			if org != "" && org != unknown {
				enriched.AssignedToDetailOrgName = *assignedTo.OrgName
				enriched.AssignedToOrgName = *assignedTo.OrgName
			}

			if len(orgs) != 0 && orgs[0] != unknown {
				enriched.AssignedToDetailMultiOrgName = orgs
			}
		} else {
			assignedToDetail := rawItem.Data.AssignedToDetail
			source := BugzillaRest
			authorUUID, err := uuid.GenerateIdentity(&source, &assignedToDetail.Name, &assignedToDetail.RealName, nil)
			if err != nil {
				dads.Printf("[dads-bugzillarest] EnrichItem GenerateIdentity error : %+v\n", err)
				return nil, err
			}

			userIdentity := libAffiliations.Identity{
				LastModified: time.Now(),
				Name:         assignedToDetail.RealName,
				Source:       BugzillaRest,
				Email:        assignedToDetail.Name,
				UUID:         authorUUID,
			}
			// Todo: add identity should be updates to return UniqueIdentityNestedDataOutput and error instead of bool
			ok := e.affiliationsClient.AddIdentity(&userIdentity)
			if !ok {
				dads.Printf("[dads-bugzilla] EnrichItem AddIdentity failed to add identity for: %+v\n", assignedToDetail.Name)
			} else {
				enriched.AssignedToDetailID = authorUUID
				enriched.AssignedToDetailUUID = authorUUID
				enriched.AssignedToDetailName = assignedToDetail.RealName
				enriched.AssignedToDetailUserName = assignedToDetail.Name
				enriched.AssignedToUUID = authorUUID
				enriched.AssignedToDetailOrgName = unknown
				enriched.AssignedToOrgName = unknown
			}
		}
	}

	if rawItem.Data.CreatorDetail != nil {
		enriched.Creator = rawItem.Data.CreatorDetail.RealName
		enriched.AuthorName = rawItem.Data.CreatorDetail.RealName

		key, value := getCont(rawItem.Data.CreatorDetail)
		creator, err := e.affiliationsClient.GetIdentityByUser(key, value)
		if err == nil && creator != nil {
			enriched.CreatorDetailID = *creator.ID
			enriched.CreatorDetailUUID = *creator.UUID
			enriched.CreatorDetailName = creator.Name
			enriched.CreatorDetailUserName = creator.Username
			enriched.CreatorDetailDomain = creator.Domain

			enriched.AuthorID = *creator.ID
			enriched.AuthorUUID = *creator.UUID
			enriched.AuthorName = creator.Name
			enriched.AuthorUserName = creator.Username
			enriched.AuthorDomain = creator.Domain

			org, orgs, err := util.GetEnrollments(e.auth0ClientProvider, e.httpClientProvider, e.affBaseURL, e.projectSlug, *creator.UUID, rawItem.MetadataUpdatedOn)
			if err != nil {
				dads.Printf("[dads-bugzilla] EnrichItem GetEnrollments error : %+v\n", err)
			}

			enriched.CreatorDetailOrgName = unknown
			enriched.AuthorOrgName = unknown
			if org != "" && org != unknown {
				enriched.CreatorDetailOrgName = *creator.OrgName
				enriched.AuthorOrgName = *creator.OrgName
			}

			if creator.IsBot != nil && *creator.IsBot != 0 {
				enriched.CreatorDetailBot = true
				enriched.AuthorBot = true
			}

			if len(orgs) != 0 && orgs[0] != "unknown" {
				enriched.CreatorDetailMultiOrgName = orgs
				enriched.AuthorMultiOrgNames = orgs
			}

		} else {
			creatorDetail := rawItem.Data.CreatorDetail
			source := BugzillaRest
			authorUUID, err := uuid.GenerateIdentity(&source, &creatorDetail.Name, &creatorDetail.RealName, nil)
			if err != nil {
				dads.Printf("[dads-bugzillarest] EnrichItem GenerateIdentity failed to generate identity for: %+v\n", err)
				return nil, err
			}

			userIdentity := libAffiliations.Identity{
				LastModified: time.Now(),
				Name:         creatorDetail.RealName,
				Source:       BugzillaRest,
				Email:        creatorDetail.Name,
				UUID:         authorUUID,
			}

			ok := e.affiliationsClient.AddIdentity(&userIdentity)
			if !ok {
				dads.Printf("[dads-bugzilla] EnrichItem AddIdentity failed to add identity for: %+v\n", creatorDetail.Name)
			} else {
				enriched.CreatorDetailID = authorUUID
				enriched.CreatorDetailUUID = authorUUID
				enriched.CreatorDetailName = creatorDetail.RealName
				enriched.CreatorDetailUserName = creatorDetail.Name

				enriched.AuthorID = authorUUID
				enriched.AuthorUUID = authorUUID
				enriched.AuthorName = creatorDetail.RealName
				enriched.AuthorUserName = creatorDetail.Name

				enriched.CreatorDetailOrgName = unknown
				enriched.CreatorDetailMultiOrgName = multiOrgs

				enriched.AuthorOrgName = unknown
				enriched.AuthorMultiOrgNames = multiOrgs

			}

		}

	}

	if rawItem.Data.Summary != "" {
		enriched.Summary = rawItem.Data.Summary
		enriched.SummaryAnalyzed = rawItem.Data.Summary

		enriched.MainDescription = rawItem.Data.Summary
		enriched.MainDescriptionAnalyzed = rawItem.Data.Summary

	}

	enriched.RepositoryLabels = nil

	return enriched, nil
}

func getCont(con *PersonDetail) (string, string) {
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

	if con.RealName != "" {
		val = con.RealName
		if strings.Contains(con.RealName, "@") && util.IsEmailValid(con.RealName) {
			key = "email"
		}
	}

	return key, val
}

package bugzillarest

import (
	"fmt"
	"github.com/LF-Engineering/da-ds/utils"
	"strings"
	"time"

	"github.com/LF-Engineering/da-ds/affiliation"
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
		DSName:           "BugzillaREST",
		BackendVersion:   backendVersion,
		Project:          project,
	}
}

// EnrichItem enrich Bugzilla raw item
func (e *Enricher) EnrichItem(rawItem BugzillaRestRaw, now time.Time) (*BugRestEnrich, error) {
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
	enriched.ProjectTs = utils.ConvertTimeToFloat(now)
	enriched.ID = rawItem.Data.ID
	enriched.MetadataBackendName = fmt.Sprintf("%sEnrich", strings.Title(e.DSName))
	enriched.MetadataBackendVersion = e.BackendVersion
	enriched.ISBugzillarestBugrest = 1
	enriched.Origin = rawItem.Origin
	enriched.URL = rawItem.Origin + "/show_bug.cgi?id=" + fmt.Sprint(rawItem.Data.ID)
	enriched.CreationDate = rawItem.Data.CreationTime
	enriched.Status = rawItem.Data.Status
	enriched.CreationTs = rawItem.Data.CreationTime.Format("2006-01-02T15:04:05")
	enriched.ISOpen = rawItem.Data.IsOpen

	if rawItem.Data.Whiteboard != "" {
		enriched.Whiteboard = &rawItem.Data.Whiteboard
	}

	enriched.Changes = 0
	// count history changes
	for _, history := range rawItem.Data.History {
		if len(history.Changes) > 0 {
			enriched.Changes += len(history.Changes)
		}
	}

	enriched.NumberOfComments = 0
	enriched.Comments = len(rawItem.Data.Comments)

	if rawItem.Data.CreatorDetail != nil && rawItem.Data.CreatorDetail.RealName != "" {
		enriched.Creator = rawItem.Data.CreatorDetail.RealName
	}

	unknown := "Unknown"
	multiOrgs := []string{unknown}
	if rawItem.Data.AssignedToDetail != nil && rawItem.Data.AssignedToDetail.RealName != "" {
		enriched.AssignedTo = rawItem.Data.AssignedToDetail.RealName

		// Enrich assigned to
		assignedToFieldName := "username"
		if rawItem.Data.AssignedToDetail != nil {
			if strings.Contains(rawItem.Data.AssignedToDetail.Name, "@") {
				assignedToFieldName = "email"
			}
		}
		assignedTo, err := e.identityProvider.GetIdentity(assignedToFieldName, enriched.AssignedTo)
		if err == nil {
			enriched.AssignedToDetailID = assignedTo.ID
			enriched.AssignedToDetailUUID = assignedTo.UUID
			enriched.AssignedToDetailName = assignedTo.Name
			enriched.AssignedToDetailUserName = assignedTo.Username
			enriched.AssignedToDetailDomain = assignedTo.Domain
			enriched.AssignedToDetailBot = assignedTo.IsBot

			enriched.AssignedToUUID = assignedTo.UUID

			if assignedTo.Gender != nil {
				enriched.AssignedToDetailGender = *assignedTo.Gender
			} else if assignedTo.Gender == nil {
				enriched.AssignedToDetailGender = unknown
			}

			if assignedTo.GenderACC != nil {
				enriched.AssignedToDetailGenderAcc = *assignedTo.GenderACC
			} else if assignedTo.GenderACC == nil {
				enriched.AssignedToDetailGenderAcc = 0
			}
			if assignedTo.OrgName != nil {
				enriched.AssignedToDetailOrgName = *assignedTo.OrgName
				enriched.AssignedToOrgName = *assignedTo.OrgName
			} else if assignedTo.OrgName == nil {
				enriched.AssignedToDetailOrgName = unknown
				enriched.AssignedToOrgName = unknown
			}

			assignedToMultiOrg, err := e.identityProvider.GetOrganizations(assignedTo.UUID, enriched.MetadataUpdatedOn)
			if err == nil {
				enriched.AssignedToDetailMultiOrgName = multiOrgs

				if len(assignedToMultiOrg) != 0 {
					enriched.AssignedToDetailMultiOrgName = assignedToMultiOrg
				}
			}
		}
	}

	if rawItem.Data.CreatorDetail != nil {
		enriched.Creator = rawItem.Data.CreatorDetail.RealName
		enriched.AuthorName = rawItem.Data.CreatorDetail.RealName

		// Enrich reporter
		reporterFieldName := "username"
		if strings.Contains(enriched.Creator, "@") {
			reporterFieldName = "email"
		}

		creator, err := e.identityProvider.GetIdentity(reporterFieldName, enriched.Creator)

		if err == nil {
			enriched.CreatorDetailID = creator.ID
			enriched.CreatorDetailUUID = creator.UUID
			enriched.CreatorDetailName = creator.Name
			enriched.CreatorDetailUserName = creator.Username
			enriched.CreatorDetailDomain = creator.Domain

			enriched.AuthorID = creator.ID
			enriched.AuthorUUID = creator.UUID
			enriched.AuthorName = creator.Name
			enriched.AuthorUserName = creator.Username
			enriched.AuthorDomain = creator.Domain

			if creator.Gender != nil {
				enriched.CreatorDetailGender = *creator.Gender
				enriched.AuthorGender = *creator.Gender
			} else if creator.Gender == nil {
				enriched.CreatorDetailGender = unknown
				enriched.AuthorGender = unknown
			}
			if creator.GenderACC != nil {
				enriched.CreatorDetailGenderACC = *creator.GenderACC
				enriched.AuthorGenderAcc = *creator.GenderACC
			} else if creator.GenderACC == nil {
				enriched.CreatorDetailGenderACC = 0
				enriched.AuthorGenderAcc = 0
			}
			if creator.OrgName != nil {
				enriched.CreatorDetailOrgName = *creator.OrgName
				enriched.AuthorOrgName = *creator.OrgName
			} else if creator.OrgName == nil {
				enriched.CreatorDetailOrgName = unknown
				enriched.AuthorOrgName = unknown
			}

			enriched.CreatorDetailBot = creator.IsBot
			enriched.AuthorBot = creator.IsBot

			reporterMultiOrg, err := e.identityProvider.GetOrganizations(creator.UUID, enriched.MetadataUpdatedOn)
			if err == nil {
				enriched.CreatorDetailMultiOrgName = multiOrgs
				enriched.AuthorMultiOrgNames = multiOrgs

				if len(reporterMultiOrg) != 0 {
					enriched.CreatorDetailMultiOrgName = reporterMultiOrg
					enriched.AuthorMultiOrgNames = reporterMultiOrg
				}
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

// EnrichAffiliation gets author SH identity data
func (e *Enricher) EnrichAffiliation(key string, val string) (*affiliation.Identity, error) {
	return e.identityProvider.GetIdentity(key, val)
}

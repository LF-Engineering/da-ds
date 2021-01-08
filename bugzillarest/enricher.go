package bugzillarest

import (
	"fmt"
	"log"
	"strings"
	"time"

	timeLib "github.com/LF-Engineering/dev-analytics-libraries/time"

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
	CreateIdentity(ident affiliation.Identity, source string) error
}

// NewEnricher intiate a new enricher instance
func NewEnricher(identProvider IdentityProvider, backendVersion string, project string) *Enricher {
	return &Enricher{
		identityProvider: identProvider,
		DSName:           BugzillaRest,
		BackendVersion:   backendVersion,
		Project:          project,
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

	if rawItem.Data.Whiteboard != "" {
		enriched.Whiteboard = &rawItem.Data.Whiteboard
	}

	// count history changes
	enriched.Changes = 0
	for _, history := range *rawItem.Data.History {
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
			enriched.AssignedToDetailID = assignedTo.ID.String
			enriched.AssignedToDetailUUID = assignedTo.UUID.String
			enriched.AssignedToDetailName = assignedTo.Name.String
			enriched.AssignedToDetailUserName = assignedTo.Username.String
			enriched.AssignedToDetailDomain = assignedTo.Domain.String
			enriched.AssignedToDetailBot = assignedTo.IsBot

			enriched.AssignedToUUID = assignedTo.UUID.String

			if assignedTo.Gender.Valid {
				enriched.AssignedToDetailGender = assignedTo.Gender.String
			} else {
				enriched.AssignedToDetailGender = unknown
			}

			if assignedTo.GenderACC != nil {
				enriched.AssignedToDetailGenderAcc = *assignedTo.GenderACC
			} else {
				enriched.AssignedToDetailGenderAcc = 0
			}
			if assignedTo.OrgName.Valid {
				enriched.AssignedToDetailOrgName = assignedTo.OrgName.String
				enriched.AssignedToOrgName = assignedTo.OrgName.String
			} else {
				enriched.AssignedToDetailOrgName = unknown
				enriched.AssignedToOrgName = unknown
			}

			assignedToMultiOrg, err := e.identityProvider.GetOrganizations(assignedTo.UUID.String, enriched.MetadataUpdatedOn)
			if err == nil {
				enriched.AssignedToDetailMultiOrgName = multiOrgs

				if len(assignedToMultiOrg) != 0 {
					enriched.AssignedToDetailMultiOrgName = assignedToMultiOrg
				}
			}
		} else {
			e.createNewIdentity(rawItem.Data.AssignedToDetail)
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
			enriched.CreatorDetailID = creator.ID.String
			enriched.CreatorDetailUUID = creator.UUID.String
			enriched.CreatorDetailName = creator.Name.String
			enriched.CreatorDetailUserName = creator.Username.String
			enriched.CreatorDetailDomain = creator.Domain.String

			enriched.AuthorID = creator.ID.String
			enriched.AuthorUUID = creator.UUID.String
			enriched.AuthorName = creator.Name.String
			enriched.AuthorUserName = creator.Username.String
			enriched.AuthorDomain = creator.Domain.String

			if creator.Gender.Valid {
				enriched.CreatorDetailGender = creator.Gender.String
				enriched.AuthorGender = creator.Gender.String
			} else {
				enriched.CreatorDetailGender = unknown
				enriched.AuthorGender = unknown
			}

			if creator.GenderACC != nil {
				enriched.CreatorDetailGenderACC = *creator.GenderACC
				enriched.AuthorGenderAcc = *creator.GenderACC
			} else {
				enriched.CreatorDetailGenderACC = 0
				enriched.AuthorGenderAcc = 0
			}

			if creator.OrgName.Valid {
				enriched.CreatorDetailOrgName = creator.OrgName.String
				enriched.AuthorOrgName = creator.OrgName.String
			} else {
				enriched.CreatorDetailOrgName = unknown
				enriched.AuthorOrgName = unknown
			}

			enriched.CreatorDetailBot = creator.IsBot
			enriched.AuthorBot = creator.IsBot

			reporterMultiOrg, err := e.identityProvider.GetOrganizations(creator.UUID.String, enriched.MetadataUpdatedOn)
			if err == nil {
				enriched.CreatorDetailMultiOrgName = multiOrgs
				enriched.AuthorMultiOrgNames = multiOrgs

				if len(reporterMultiOrg) != 0 {
					enriched.CreatorDetailMultiOrgName = reporterMultiOrg
					enriched.AuthorMultiOrgNames = reporterMultiOrg
				}
			}
		} else {
			e.createNewIdentity(rawItem.Data.CreatorDetail)
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

func (e *Enricher) createNewIdentity(data *PersonDetail) {
	// add new identity to affiliation DB
	var identity affiliation.Identity
	if data != nil {
		if data.Name != "" {
			identity.Username.String = data.Name
			identity.Username.Valid = true
		}
		if data.RealName != "" {
			identity.Name.String = data.RealName
			identity.Name.Valid = true
		} else {
			identity.Name.String = data.Name
			identity.Name.Valid = true
		}
		err := e.identityProvider.CreateIdentity(identity, BugzillaRest)
		if err != nil {
			log.Printf("Err : %s", err.Error())
		}
	}
}

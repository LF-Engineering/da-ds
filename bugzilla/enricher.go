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
	roles            []string
}

type IdentityProvider interface {
	GetIdentityByUsername(username string) (*affiliation.Identity, error)
	GetIdentityByEmail(email string) (*affiliation.Identity, error)
}

// NewEnricher
func NewEnricher(identProvider IdentityProvider) *Enricher {
	return &Enricher{
		identityProvider: identProvider,
		roles:            []string{"assigned_to", "reporter", "qa_contact"},
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
	enriched.MetadataEnrichedOn = rawItem.MetadataTimestamp
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
	}
	if rawItem.Reporter != "" {
		enriched.ReporterName = rawItem.Reporter
		enriched.AuthorName = rawItem.Reporter
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

// EnrichAffiliation Adds sorting hat enrichment fields for different roles
// If there are no roles, just add the author fields.
func (e *Enricher) EnrichAffiliation(raw *BugRaw) error {
	// Enrich Reporter
	var reporterIdentity *affiliation.Identity
	reporterKey := raw.Reporter
	if strings.Contains(reporterKey, "@") {
		reporterIdentity, err := e.identityProvider.GetIdentityByEmail(reporterKey)
		if err != nil {
			return err
		}

	}

}

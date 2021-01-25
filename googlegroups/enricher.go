package googlegroups

import (
	"fmt"
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

	enrichedMessage := EnrichedMessage{
		From:               rawMessage.From,
		Date:               rawMessage.Date,
		To:                 rawMessage.To,
		MessageID:          rawMessage.MessageID,
		InReplyTo:          rawMessage.InReplyTo,
		References:         rawMessage.References,
		Subject:            rawMessage.References,
		MessageBody:        rawMessage.MessageBody,
		TopicID:            rawMessage.TopicID,
		BackendVersion:     rawMessage.BackendVersion,
		UUID:               rawMessage.UUID,
		MetadataUpdatedOn:  rawMessage.MetadataUpdatedOn,
		BackendName:        fmt.Sprintf("%sEnrich", strings.Title(e.DSName)),
		MetadataTimestamp:  rawMessage.MetadataTimestamp,
		MetadataEnrichedOn: now,
		ProjectSlug:        rawMessage.ProjectSlug,
		GroupName:          rawMessage.GroupName,
		Project:            rawMessage.Project,
		ChangedAt:          rawMessage.ChangedAt,
	}
	return &enrichedMessage, nil
}

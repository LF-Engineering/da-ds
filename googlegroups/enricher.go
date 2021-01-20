package googlegroups

import (
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/LF-Engineering/dev-analytics-libraries/affiliation"
	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
	jsoniter "github.com/json-iterator/go"
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
func (e *Enricher) EnrichMessage(path string, now time.Time) ([]*EnrichedMessage, error) {
	bites, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var rawMessages GoogleGroupMessages
	err = jsoniter.Unmarshal(bites, &rawMessages)
	if err != nil {
		return nil, err
	}

	var enrichedMessages []*EnrichedMessage
	for i, message := range rawMessages.Messages {
		messageDetails := message.Messages[i]
		enrichedMessage := EnrichedMessage{
			Topic:               message.Topic,
			TopicID:             message.ID,
			Message:             "",
			ID:                  messageDetails.ID,
			Author:              messageDetails.Author,
			Date:                messageDetails.Date,
			File:                messageDetails.File,
			UUID:                "",
			MetadataTimestamp:   time.Time{},
			MetadataBackendName: fmt.Sprintf("%sEnrich", strings.Title(e.DSName)),
			MetadataUpdatedOn:   time.Time{},
			MetadataEnrichedOn:  time.Time{},
			ChangedDate:         now,
		}

		enrichedMessages = append(enrichedMessages, &enrichedMessage)
	}

	return enrichedMessages, nil
}

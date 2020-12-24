package pipermail

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/LF-Engineering/da-ds/affiliation"
)

// Enricher contains pipermail datasource enrich logic
type Enricher struct {
	identityProvider      IdentityProvider
	DSName                string // Datasource will be used as key for ES
	ElasticSearchProvider ESClientProvider
	BackendVersion        string
}

// IdentityProvider manages user identity
type IdentityProvider interface {
	GetIdentity(key string, val string) (*affiliation.Identity, error)
	GetOrganizations(uuid string, date time.Time) ([]string, error)
}

// TopHits result
type TopHits struct {
	Took         int          `json:"took"`
	Hits         Hits         `json:"hits"`
	Aggregations Aggregations `json:"aggregations"`
}

// Hits result
type Hits struct {
	Total    Total        `json:"total"`
	MaxScore float32      `json:"max_score"`
	Hits     []NestedHits `json:"hits"`
}

// Total result
type Total struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

// NestedHits result
type NestedHits struct {
	Index  string      `json:"_index"`
	Type   string      `json:"_type"`
	ID     string      `json:"_id"`
	Score  float64     `json:"_score"`
	Source *RawMessage `json:"_source"`
}

// Aggregations result
type Aggregations struct {
	LastDate LastDate `json:"last_date"`
}

// LastDate result
type LastDate struct {
	Value         float64 `json:"value"`
	ValueAsString string  `json:"value_as_string"`
}

// NewEnricher initiates a new Enricher
func NewEnricher(identProvider IdentityProvider, backendVersion string, esClientProvider ESClientProvider) *Enricher {
	return &Enricher{
		identityProvider:      identProvider,
		DSName:                Pipermail,
		ElasticSearchProvider: esClientProvider,
		BackendVersion:        backendVersion,
	}
}

// EnrichMessage enriches raw message
func (e *Enricher) EnrichMessage(rawMessage *RawMessage, now time.Time) (*EnrichMessage, error) {

	now = now.UTC()
	enriched := EnrichMessage{
		ID:                   rawMessage.Data.MessageID,
		ProjectTS:            0,
		FromUserName:         "",
		TZ:                   rawMessage.Data.DateTZ,
		MessageID:            rawMessage.Data.MessageID,
		UUID:                 rawMessage.UUID,
		AuthorName:           "",
		Root:                 false,
		FromUUID:             "",
		AuthorGenderACC:      0,
		FromName:             "",
		AuthorOrgName:        "",
		AuthorUserName:       "",
		AuthorBot:            false,
		BodyExtract:          "",
		AuthorID:             "",
		SubjectAnalyzed:      rawMessage.Data.Subject,
		FromBot:              false,
		Project:              "",
		MboxAuthorDomain:     "",
		Date:                 rawMessage.Data.Date,
		IsPipermailMessage:   1,
		FromGender:           "",
		FromMultipleOrgNames: nil,
		FromOrgName:          "",
		FromDomain:           "",
		List:                 rawMessage.Origin,
		AuthorUUID:           "",
		AuthorMultiOrgNames:  nil,
		Origin:               rawMessage.Origin,
		Size:                 rawMessage.Data.MboxByteLength,
		Tag:                  rawMessage.Origin,
		Subject:              rawMessage.Data.Subject,
		FromID:               "",
		AuthorGender:         "",
		FromGenderAcc:        "",
		EmailDate:            rawMessage.Data.Date,
		MetadataTimestamp:    rawMessage.MetadataTimestamp,
		MetadataBackendName:  fmt.Sprintf("%sEnrich", strings.Title(e.DSName)),
		MetadataUpdatedOn:    rawMessage.MetadataUpdatedOn,
		MetadataEnrichedOn:   now,
		BackendVersion:       rawMessage.BackendVersion,
	}

	var emailObfuscationPatterns = []string{" at ", "_at_", " en "}
	for _, pattern := range emailObfuscationPatterns {
		trimBraces := strings.Split(rawMessage.Data.From, " (")
		obfuscatedEmail := strings.TrimSpace(trimBraces[0])
		if strings.Contains(obfuscatedEmail, pattern) {
			email := strings.Replace(obfuscatedEmail, pattern, "@", 1)
			fmt.Println(email)
			os.Exit(1)
		}
	}

	return &enriched, nil
}

// EnrichAffiliation gets author Affiliations identity data
func (e *Enricher) EnrichAffiliation(key string, val string) (*affiliation.Identity, error) {
	return e.identityProvider.GetIdentity(key, val)
}

// HandleMapping creates rich mapping
func (e *Enricher) HandleMapping(index string) error {
	_, err := e.ElasticSearchProvider.CreateIndex(index, PiperRichMapping)
	return err
}

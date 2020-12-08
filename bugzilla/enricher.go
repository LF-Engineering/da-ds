package bugzilla

import (
	"fmt"
	"time"

	"github.com/LF-Engineering/da-ds/utils"
)

// EnrichedItem ...
type EnrichedItem struct {
	UUID                    string    `json:"uuid"`
	Labels                  []string  `json:"labels"`
	Changes                 int       `json:"changes"`
	Priority                string    `json:"priority"`
	Severity                string    `json:"severity"`
	OpSys                   string    `json:"op_sys"`
	ChangedAt               string    `json:"changed_at"`
	Product                 string    `json:"product"`
	Component               string    `json:"component"`
	Platform                string    `json:"platform"`
	BugId                   int       `json:"bug_id"`
	Status                  string    `json:"status"`
	TimeOpenDays            float64   `json:"timeopen_days"`
	Category                string    `json:"category"`
	ChangedDate             time.Time `json:"changed_date"`
	Tag                     string    `json:"tag"`
	IsBugzillaBug           int       `json:"is_bugzilla_bug"`
	Url                     string    `json:"url"`
	ResolutionDays          float64   `json:"resolution_days"`
	CreationDate            time.Time `json:"creation_date"`
	DeltaTs                 time.Time `json:"delta_ts"`
	Whiteboard              string    `json:"whiteboard"`
	Resolution              string    `json:"resolution"`
	Assigned                string    `json:"assigned"`
	ReporterName            string    `json:"reporter_name"`
	AuthorName              string    `json:"author_name"`
	MainDescription         string    `json:"main_description"`
	MainDescriptionAnalyzed string    `json:"main_description_analyzed"`
	Summary                 string    `json:"summary"`
	SummaryAnalyzed         string    `json:"summary_analyzed"`
	Comments int `json:"comments"`

	MetadataUpdatedOn  time.Time `json:"metadata__updated_on"`
	MetadataTimestamp  time.Time `json:"metadata__timestamp"`
	MetadataEnrichedOn time.Time `json:"metadata__enriched_on"`
}

func EnrichItem(rawItem BugRaw, now time.Time) (*EnrichedItem, error) {
	enriched := &EnrichedItem{}

	enriched.Category = "bug"
	enriched.ChangedDate = rawItem.ChangedAt
	fmt.Println("111111")
	fmt.Println(rawItem.DeltaTs)
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

	return enriched, nil
}

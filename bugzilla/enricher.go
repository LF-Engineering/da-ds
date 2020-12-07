package bugzilla

import (
	"fmt"
	"github.com/LF-Engineering/da-ds/utils"
	"time"
)

type EnItem struct {
	UUID string `json:"uuid"`
	Labels []string `json:"labels"`
	Changes int `json:"changes"`
	Priority string `json:"priority"`
	Severity string `json:"severity"`
	OpSys    string    `json:"op_sys"`
	ChangedAt string `json:"changed_at"`
	Product string `json:"product"`
	Component string `json:"component"`
	Platform string `json:"platform"`
	BugId int `json:"bug_id"`
	Status string `json:"status"`
	TimeopenDays float64 `json:"timeopen_days"`
	Category string `json:"'category'"`
	ChangeddateDate time.Time `json:"changeddate_date"`
	Tag string `json:"tag"`
	IsBugzillaBug int `json:"is_bugzilla_bug"`
	Url string `json:"url"`
	ResolutionDays float64 `json:"resolution_days"`
	CreationDate time.Time `json:"creation_date"`
	DeltaTs time.Time `json:"delta_ts"`
	Whiteboard string `json:"whiteboard"`
	Resolution string `json:"resolution"`
	Assigned string `json:"assigned"`
	ReporterName string `json:"reporter_name"`
	AuthorName string `json:"author_name"`
	MainDescription string `json:"main_description"`
	MainDescriptionAnalyzed string `json:"main_description_analyzed"`
	Summary string `json:"summary"`
	SummaryAnalyzed string `json:"summary_analyzed"`

	MetadataUpdatedOn time.Time `json:"metadata__updated_on"`
	MetadataTimestamp time.Time `json:"metadata__timestamp"`
	MetadataEnrichedOn time.Time `json:"metadata__enriched_on"`
}

func EnrichItem(rawItem BugRaw, now time.Time) ( *EnItem, error)  {
	enriched := &EnItem{}

	enriched.Category = "bug"
	enriched.ChangeddateDate = rawItem.DeltaTs.UTC()
	enriched.Changes = rawItem.ActivityCount
	enriched.Labels = rawItem.Keywords
	enriched.Priority = rawItem.Priority
	enriched.Severity = rawItem.Severity
	enriched.OpSys = rawItem.OpSys
	enriched.Product = rawItem.Product
	enriched.Component = rawItem.Component
	enriched.Platform = rawItem.RepPlatform
	tnow := time.Now()
	tnow = now.UTC()

	enriched.Tag = rawItem.Tag
	enriched.UUID = rawItem.UUID
	enriched.MetadataUpdatedOn = rawItem.MetadataUpdatedOn
	enriched.MetadataTimestamp = rawItem.MetadataTimestamp
	enriched.MetadataEnrichedOn = tnow
	enriched.IsBugzillaBug = 1
	enriched.Url = rawItem.Origin + "/show_bug.cgi?id=" + fmt.Sprint(rawItem.BugID)
	//
	//from, err := time.Parse("2006-01-02 15:04:05", rawItem.CreationTS )
	//if err != nil {
	//	return nil, err
	//}
	enriched.CreationDate = rawItem.CreationTS

	enriched.ResolutionDays = utils.GetDaysbetweenDates(enriched.CreationDate, enriched.DeltaTs)
	enriched.TimeopenDays = utils.GetDaysbetweenDates(enriched.CreationDate, enriched.DeltaTs)
	if rawItem.StatusWhiteboard != "" {
		enriched.Whiteboard = rawItem.StatusWhiteboard
	}
	if rawItem.AssignedTo != ""{
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
		enriched.SummaryAnalyzed = rawItem.Summary
	}

	enriched.Status = rawItem.BugStatus
	enriched.BugId = rawItem.BugID







	return enriched,nil
}

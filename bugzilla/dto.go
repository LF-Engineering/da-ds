package bugzilla

import "time"

// BugResponse data model represents Bugzilla get bugs results
type BugResponse struct {
	ID               int       `json:"id"`
	Product          string    `json:"product"`
	Component        string    `json:"component"`
	AssignedTo       string    `json:"assigned_to"`
	Status           string    `json:"status"`
	Resolution       string    `json:"resolution"`
	ShortDescription string    `json:"short_description"`
	ChangedDate      time.Time `json:"changed_date"`
}

// SearchFields ...
type SearchFields struct {
	Component string `json:"component"`
	Product   string `json:"product"`
	ItemID    string `json:"item_id"`
}

// BugRaw data model represents es schema
type BugRaw struct {
	BackendVersion           string        `json:"backend_version"`
	BackendName              string        `json:"backend_name"`
	UUID                     string        `json:"uuid"`
	Origin                   string        `json:"origin"`
	Tag                      string        `json:"tag"`
	Product                  string        `json:"product"`
	Data                     *BugResponse  `json:"data"`
	UpdatedOn                int64         `json:"updated_on"`
	MetadataUpdatedOn        time.Time     `json:"metadata__updated_on"`
	MetadataTimestamp        time.Time     `json:"metadata__timestamp"`
	Timestamp                int64         `json:"timestamp"`
	Category                 string        `json:"category"`
	ClassifiedFieldsFiltered *string       `json:"classified_fields_filtered"`
	SearchFields             *SearchFields `json:"search_fields"`
}

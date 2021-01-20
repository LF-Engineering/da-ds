package googlegroups

import "time"

// GoogleGroupMessages ...
type GoogleGroupMessages struct {
	Messages []*GoogleGroupMessageThread
}

// GoogleGroupMessageThread ...
type GoogleGroupMessageThread struct {
	Topic    string                `json:"topic"`
	ID       string                `json:"id"`
	Messages []*GoogleGroupMessage `json:"messages"`
}

// GoogleGroupMessage ...
type GoogleGroupMessage struct {
	ID     string `json:"id"`
	Author string `json:"author"`
	Date   string `json:"date"`
	File   string `json:"file"`
}

// EnrichedMessage ...
type EnrichedMessage struct {
	Topic               string    `json:"topic"`
	TopicID             string    `json:"topic_id"`
	Message             string    `json:"message"`
	ID                  string    `json:"id"`
	Author              string    `json:"author"`
	Date                string    `json:"date"`
	File                string    `json:"file"`
	UUID                string    `json:"uuid"`
	MetadataTimestamp   time.Time `json:"metadata__timestamp"`
	MetadataBackendName string    `json:"metadata__backend_name"`
	MetadataUpdatedOn   time.Time `json:"metadata__updated_on"`
	MetadataEnrichedOn  time.Time `json:"metadata__enriched_on"`
	ChangedDate         time.Time `json:"changed_date"`
}

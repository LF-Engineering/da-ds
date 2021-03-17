package jenkins

import "time"

// JobResponse struct represent the response of the
// Jenkins api to get all the jobs
type JobResponse struct {
	Jobs []struct {
		Class string `json:"_class"`
		Name  string `json:"name"`
		URL   string `json:"url"`
		Color string `json:"color,omitempty"`
	} `json:"jobs"`
	URL   string `json:"url"`
	Views []struct {
		Class string `json:"_class"`
		Name  string `json:"name"`
		URL   string `json:"url"`
	} `json:"views"`
}

// BuildResponse struct represent the response of
// the jenkins api to get all the builds
type BuildResponse struct {
	Description       string      `json:"description"`
	DisplayName       string      `json:"displayName"`
	DisplayNameOrNull interface{} `json:"displayNameOrNull"`
	FullDisplayName   string      `json:"fullDisplayName"`
	FullName          string      `json:"fullName"`
	Name              string      `json:"name"`
	URL               string      `json:"url"`
	Builds            []Build     `json:"builds"`
}

// BuildsRaw struct represent the schema of
// the raw documents in ES
type BuildsRaw struct {
	BackendName              string      `json:"backend_name"`
	BackendVersion           string      `json:"backend_version"`
	PercevalVersion          string      `json:"perceval_version"`
	Timestamp                float64     `json:"timestamp"`
	Origin                   string      `json:"origin"`
	UUID                     string      `json:"uuid"`
	UpdatedOn                float64     `json:"updated_on"`
	ClassifiedFieldsFiltered interface{} `json:"classified_fields_filtered"`
	Category                 string      `json:"category"`
	SearchFields             struct {
		ItemID string `json:"item_id"`
		Number int    `json:"number"`
	} `json:"search_fields"`
	Tag               string    `json:"tag"`
	Data              Build     `json:"data"`
	MetadataUpdatedOn time.Time `json:"metadata__updated_on"`
	MetadataTimestamp time.Time `json:"metadata__timestamp"`
	Installer         string    `json:"installer"`
}

// Build is the single build in the response of
// the jenkins api to get the builds
type Build struct {
	Class   string `json:"_class"`
	Actions []struct {
		Class  string `json:"_class,omitempty"`
		Causes []struct {
			Class            string `json:"_class"`
			ShortDescription string `json:"shortDescription"`
		} `json:"causes,omitempty"`
		Parameters []struct {
			Class string `json:"_class"`
			Name  string `json:"name"`
			Value string `json:"value"`
		} `json:"parameters,omitempty"`
		BuildsByBranchName struct {
			RefsRemotesOriginMaster struct {
				Class       string      `json:"_class"`
				BuildNumber int         `json:"buildNumber"`
				BuildResult interface{} `json:"buildResult"`
				Marked      struct {
					SHA1   string `json:"SHA1"`
					Branch []struct {
						SHA1 string `json:"SHA1"`
						Name string `json:"name"`
					} `json:"branch"`
				} `json:"marked"`
				Revision struct {
					SHA1   string `json:"SHA1"`
					Branch []struct {
						SHA1 string `json:"SHA1"`
						Name string `json:"name"`
					} `json:"branch"`
				} `json:"revision"`
			} `json:"refs/remotes/origin/master"`
		} `json:"buildsByBranchName,omitempty"`
		LastBuiltRevision struct {
			SHA1   string `json:"SHA1"`
			Branch []struct {
				SHA1 string `json:"SHA1"`
				Name string `json:"name"`
			} `json:"branch"`
		} `json:"lastBuiltRevision,omitempty"`
		RemoteUrls []string `json:"remoteUrls,omitempty"`
		ScmName    string   `json:"scmName,omitempty"`
	} `json:"actions"`
	Artifacts []struct {
		DisplayPath  string `json:"displayPath"`
		FileName     string `json:"fileName"`
		RelativePath string `json:"relativePath"`
	} `json:"artifacts"`
	Building          bool        `json:"building"`
	Description       interface{} `json:"description"`
	DisplayName       string      `json:"displayName"`
	Duration          int         `json:"duration"`
	EstimatedDuration int         `json:"estimatedDuration"`
	Executor          interface{} `json:"executor"`
	FullDisplayName   string      `json:"fullDisplayName"`
	ID                string      `json:"id"`
	KeepLog           bool        `json:"keepLog"`
	Number            int         `json:"number"`
	QueueID           int         `json:"queueId"`
	Result            *string     `json:"result"`
	Timestamp         int64       `json:"timestamp"`
	URL               string      `json:"url"`
	BuiltOn           string      `json:"builtOn"`
	ChangeSet         struct {
		Class string        `json:"_class"`
		Items []interface{} `json:"items"`
		Kind  string        `json:"kind"`
	} `json:"changeSet"`
	Culprits []interface{} `json:"culprits"`
	Runs     []struct {
		Number int    `json:"number"`
		URL    string `json:"url"`
	} `json:"runs"`
}

// BuildsEnrich represents the schema for the
// enriched documents in ES
type BuildsEnrich struct {
	MetadataUpdatedOn       time.Time   `json:"metadata__updated_on"`
	MetadataTimestamp       time.Time   `json:"metadata__timestamp"`
	Offset                  interface{} `json:"offset"`
	Origin                  string      `json:"origin"`
	Tag                     string      `json:"tag"`
	UUID                    string      `json:"uuid"`
	FullDisplayName         string      `json:"fullDisplayName"`
	URL                     string      `json:"url"`
	Result                  *string     `json:"result"`
	Duration                int         `json:"duration"`
	BuiltOn                 string      `json:"builtOn"`
	FullDisplayNameAnalyzed string      `json:"fullDisplayName_analyzed"`
	Build                   int         `json:"build"`
	JobURL                  string      `json:"job_url"`
	JobName                 string      `json:"job_name"`
	JobBuild                string      `json:"job_build"`
	BuildDate               time.Time   `json:"build_date"`
	DurationDays            float64     `json:"duration_days"`
	Category                string      `json:"category"`
	Installer               string      `json:"installer"`
	Scenario                interface{} `json:"scenario"`
	Testproject             string      `json:"testproject"`
	Pod                     string      `json:"pod"`
	Loop                    string      `json:"loop"`
	Branch                  string      `json:"branch"`
	GrimoireCreationDate    time.Time   `json:"grimoire_creation_date"`
	IsJenkinsJob            int         `json:"is_jenkins_job"`
	RepositoryLabels        interface{} `json:"repository_labels"`
	MetadataFilterRaw       interface{} `json:"metadata__filter_raw"`
	MetadataBackendVersion  string      `json:"metadata__backend_version"`
	MetadataBackendName     string      `json:"metadata__backend_name"`
	Project                 string      `json:"project"`
	MetadataEnrichedOn      time.Time   `json:"metadata__enriched_on"`
}

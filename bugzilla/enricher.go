package bugzilla

type EnItem struct {
	Labels []string `json:"labels"`
	Changes int `json:"changes"`
	Priority string `json:"priority"`
	Severity string `json:"severity"`
	OpSys    string    `json:"op_sys"`
	ChangedAt string `json:"changed_at"`
	Product string `json:"product"`
	Component string `json:"component"`
	Platform string `json:"platform"`
	BugId string `json:"bug_id"`
	BugStatus string `json:"bug_status"`
	TimeopenDays float32 `json:"timeopen_days"`
}

func EnrichItem() ( *EnItem, error)  {


	return nil,nil
}

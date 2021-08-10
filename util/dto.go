package util

// ElasticResponse ...
type ElasticResponse struct {
	Took   int
	Errors bool
	Items  []ElasticResponseItem
}

// ElasticResponseItem ...
type ElasticResponseItem struct {
	Index ESResponseIndex
}

// ESResponseIndex ...
type ESResponseIndex struct {
	ID     string `json:"_id"`
	Status int
}

// EnrollmentOrgs ...
type EnrollmentOrgs struct {
	Org  string
	Orgs []string
}

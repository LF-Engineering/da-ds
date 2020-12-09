package affiliation

// Identity contains sortingHat user Identity
type Identity struct {
	ID            string
	UUID          string
	Name          string
	Username      string
	Email         string
	Domain        string
	Gender        *string
	GenderACC     *int `db:"gender_acc"`
	OrgName       *string
	IsBot         bool `db:"is_bot"`
	MultiOrgNames []string
}

package util

import "time"

// IdentityData ...
type IdentityData struct {
	Email        *string    `json:"email,omitempty"`
	ID           string     `json:"id,omitempty"`
	LastModified *time.Time `json:"last_modified,omitempty"`
	Name         *string    `json:"name,omitempty"`
	Source       string     `json:"source,omitempty"`
	Username     *string    `json:"username,omitempty"`
	UUID         *string    `json:"uuid,omitempty"`
}

type UniqueIdentityFullProfile struct {
	Enrollments []*Enrollments  `json:"enrollments"`
	Identities  []*IdentityData `json:"identities"`
	Profile     *Profile        `json:"profile,omitempty"`
	UUID        string          `json:"uuid,omitempty"`
}

// Enrollments ...
type Enrollments struct {
	Organization *Organization `json:"organization,omitempty"`
}

//  Organization ...
type Organization struct {
	Name string `json:"name,omitempty"`
}

// Profile ...
type Profile struct {
	Email     *string `json:"email,omitempty"`
	Gender    *string `json:"gender,omitempty"`
	GenderAcc *int64  `json:"gender_acc,omitempty"`
	IsBot     *int64  `json:"is_bot,omitempty"`
	Name      *string `json:"name,omitempty"`
	UUID      string  `json:"uuid,omitempty"`
}

// Identity contains affiliation user Identity
type AffIdentity struct {
	ID            *string `json:"id"`
	UUID          *string
	Name          string
	Username      string
	Email         string
	Domain        string
	Gender        *string  `json:"gender"`
	GenderACC     *int64   `json:"gender_acc"`
	OrgName       *string  `json:"org_name"`
	IsBot         *int64   `json:"is_bot"`
	MultiOrgNames []string `json:"multi_org_names"`
}

// Person describe affiliation person data
type Person struct {
	Name     string `json:"name"`
	Username string `json:"username"`
}

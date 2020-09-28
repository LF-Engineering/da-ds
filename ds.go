package dads

import "time"

// DS - interface for all data source types
type DS interface {
	ParseArgs(*Ctx) error
	Name() string
	Info() string
	FetchRaw(*Ctx) (*time.Time, error)
	Enrich(*Ctx, *time.Time) error
}

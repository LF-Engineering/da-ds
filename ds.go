package dads

// DS - interface for all data source types
type DS interface {
	ParseArgs(*Ctx) error
	Name() string
}

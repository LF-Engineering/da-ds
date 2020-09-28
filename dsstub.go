package dads

import (
	"fmt"
	"time"
)

// DSStub - DS implementation for stub - does nothing at all, just presents a skeleton code
type DSStub struct {
	DS string
}

// ParseArgs - parse stub specific environment variables
func (j *DSStub) ParseArgs(ctx *Ctx) (err error) {
	j.DS = Stub
	fmt.Printf("DSStub.ParseArgs\n")
	return
}

// Name - return data source name
func (j *DSStub) Name() string {
	return j.DS
}

// Info - return DS configuration in a human readable form
func (j DSStub) Info() string {
	return fmt.Sprintf("%+v", j)
}

// CustomFetchRaw - is this datasource using custom fetch raw implementation?
func (j *DSStub) CustomFetchRaw() bool {
	return false
}

// FetchRaw - implement fetch raw data for stub datasource
func (j *DSStub) FetchRaw(ctx *Ctx) (lastData *time.Time, err error) {
	fmt.Printf("DSStub.FetchRaw\n")
	return
}

// CustomEnrich - is this datasource using custom enrich implementation?
func (j *DSStub) CustomEnrich() bool {
	return false
}

// Enrich - implement enrich data for stub datasource
func (j *DSStub) Enrich(ctx *Ctx, startFrom *time.Time) (err error) {
	fmt.Printf("DSStub.Enrich\n")
	return
}

// SupportDateFrom - does DS support resuming from date?
func (j *DSStub) SupportDateFrom() bool {
	return false
}

// SupportOffsetFrom - does DS support resuming from offset?
func (j *DSStub) SupportOffsetFrom() bool {
	return false
}

// DateField - return date field used to detect where to restart from
func (j *DSStub) DateField(*Ctx) string {
	return DefaultDateField
}

// OffsetField - return offset field used to detect where to restart from
func (j *DSStub) OffsetField(*Ctx) string {
	return DefaultOffsetField
}

// Categories - return a set of configured categories
func (j *DSStub) Categories() map[string]struct{} {
	return map[string]struct{}{}
}

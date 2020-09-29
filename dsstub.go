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

// Validate - is current DS configuration OK?
func (j *DSStub) Validate() (err error) {
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
func (j *DSStub) FetchRaw(ctx *Ctx) (err error) {
	fmt.Printf("DSStub.FetchRaw\n")
	return
}

// CustomEnrich - is this datasource using custom enrich implementation?
func (j *DSStub) CustomEnrich() bool {
	return false
}

// Enrich - implement enrich data for stub datasource
func (j *DSStub) Enrich(ctx *Ctx) (err error) {
	fmt.Printf("DSStub.Enrich\n")
	return
}

// FetchItems - implement enrich data for stub datasource
func (j *DSStub) FetchItems(ctx *Ctx) (err error) {
	fmt.Printf("DSStub.FetchItems\n")
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

// ResumeNeedsOrigin - is origin field needed when resuming
// Origin should be needed when multiple configurations save to the same index
func (j *DSStub) ResumeNeedsOrigin() bool {
	return false
}

// Origin - return current origin
func (j *DSStub) Origin() string {
	return ""
}

// ItemID - return unique identifier for an item
func (j *DSStub) ItemID(item interface{}) string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// ItemUpdatedOn - return updated on date for an item
func (j *DSStub) ItemUpdatedOn(item interface{}) time.Time {
	return time.Now()
}

// ItemCategory - return unique identifier for an item
func (j *DSStub) ItemCategory(item interface{}) string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// SearchFields - define (optional) search fields to be returned
func (j *DSStub) SearchFields() map[string][]string {
	return map[string][]string{}
}

// ElasticRawMapping - Raw index mapping definition
func (j *DSStub) ElasticRawMapping() string {
	return ""
}

// ElasticRichMapping - Rich index mapping definition
func (j *DSStub) ElasticRichMapping() string {
	return ""
}

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

// FetchRaw - implement fetch raw data for stub datasource
func (j *DSStub) FetchRaw(ctx *Ctx) (lastData *time.Time, err error) {
	fmt.Printf("DSStub.FetchRaw\n")
	return
}

// Enrich - implement enrich data for stub datasource
func (j *DSStub) Enrich(ctx *Ctx, startFrom *time.Time) (err error) {
	fmt.Printf("DSStub.Enrich\n")
	return
}

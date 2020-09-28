package dads

// DSStub - DS implementation for stub - does nothing at all, just presents a skeleton code
type DSStub struct {
	DS string
}

// ParseArgs - parse stub specific environment variables
func (j *DSStub) ParseArgs(ctx *Ctx) (err error) {
	j.DS = Stub
	return
}

// Name - return data source name
func (j *DSStub) Name() string {
	return j.DS
}

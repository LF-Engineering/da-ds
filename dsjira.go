package dads

import "fmt"

// DSJira - DS implementation for Jira
type DSJira struct {
	DS string
}

// ParseArgs - parse jira specific environment variables
func (j *DSJira) ParseArgs() (err error) {
	j.DS = Jira
	fmt.Printf("Jira's ParseArgs\n")
	return
}

// Name - return data source name
func (j *DSJira) Name() string {
	return j.DS
}

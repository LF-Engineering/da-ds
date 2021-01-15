// Code generated by mockery v2.3.0. DO NOT EDIT.

package mocks

import (
	affiliation "github.com/LF-Engineering/dev-analytics-libraries/affiliation"

	mock "github.com/stretchr/testify/mock"
)

// Affiliation is an autogenerated mock type for the Affiliation type
type Affiliation struct {
	mock.Mock
}

// AddIdentity provides a mock function with given fields: identity
func (_m *Affiliation) AddIdentity(identity *affiliation.Identity) bool {
	ret := _m.Called(identity)

	var r0 bool
	if rf, ok := ret.Get(0).(func(*affiliation.Identity) bool); ok {
		r0 = rf(identity)
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// GetIdentityByUser provides a mock function with given fields: key, value
func (_m *Affiliation) GetIdentityByUser(key string, value string) (*affiliation.AffIdentity, error) {
	ret := _m.Called(key, value)

	var r0 *affiliation.AffIdentity
	if rf, ok := ret.Get(0).(func(string, string) *affiliation.AffIdentity); ok {
		r0 = rf(key, value)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*affiliation.AffIdentity)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, string) error); ok {
		r1 = rf(key, value)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

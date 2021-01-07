// Code generated by mockery v2.3.0. DO NOT EDIT.

package mocks

import (
	affiliation "github.com/LF-Engineering/da-ds/affiliation"

	mock "github.com/stretchr/testify/mock"

	time "time"
)

// IdentityProvider is an autogenerated mock type for the IdentityProvider type
type IdentityProvider struct {
	mock.Mock
}

// CreateIdentity provides a mock function with given fields: ident, source
func (_m *IdentityProvider) CreateIdentity(ident affiliation.Identity, source string) {
	_m.Called(ident, source)
}

// GetIdentity provides a mock function with given fields: key, val
func (_m *IdentityProvider) GetIdentity(key string, val string) (*affiliation.Identity, error) {
	ret := _m.Called(key, val)

	var r0 *affiliation.Identity
	if rf, ok := ret.Get(0).(func(string, string) *affiliation.Identity); ok {
		r0 = rf(key, val)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*affiliation.Identity)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, string) error); ok {
		r1 = rf(key, val)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetOrganizations provides a mock function with given fields: uuid, date
func (_m *IdentityProvider) GetOrganizations(uuid string, date time.Time) ([]string, error) {
	ret := _m.Called(uuid, date)

	var r0 []string
	if rf, ok := ret.Get(0).(func(string, time.Time) []string); ok {
		r0 = rf(uuid, date)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, time.Time) error); ok {
		r1 = rf(uuid, date)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

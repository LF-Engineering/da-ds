// Code generated by mockery v0.0.0-dev. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// HTTPClientProvider is an autogenerated mock type for the HTTPClientProvider type
type HTTPClientProvider struct {
	mock.Mock
}

// Request provides a mock function with given fields: url, method, header, body, params
func (_m *HTTPClientProvider) Request(url string, method string, header map[string]string, body []byte, params map[string]string) (int, []byte, error) {
	ret := _m.Called(url, method, header, body, params)

	var r0 int
	if rf, ok := ret.Get(0).(func(string, string, map[string]string, []byte, map[string]string) int); ok {
		r0 = rf(url, method, header, body, params)
	} else {
		r0 = ret.Get(0).(int)
	}

	var r1 []byte
	if rf, ok := ret.Get(1).(func(string, string, map[string]string, []byte, map[string]string) []byte); ok {
		r1 = rf(url, method, header, body, params)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).([]byte)
		}
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(string, string, map[string]string, []byte, map[string]string) error); ok {
		r2 = rf(url, method, header, body, params)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

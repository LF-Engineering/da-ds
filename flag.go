package dads

import (
	"strconv"
	"time"
)

type Flag string

func (f *Flag) String() string {
	if f != nil {
		return string(*f)
	}

	return ""
}

func (f *Flag) Set(val string) error {
	*f = Flag(val)
	return nil
}

func (f *Flag) Bool() bool {

	if f != nil {
		val, err:=strconv.ParseBool( f.String() )
		if err != nil {
			return false
		}
		return val
	}

	return false
}


func (f *Flag) Int() int {
	if f != nil {
		val, err := strconv.Atoi(f.String())
		if err!= nil {
			return 0
		}
		return val
	}

	return 0
}


func (f *Flag) Date() *time.Time {
	if f != nil {
		date, err := time.Parse("2006-01-02 15:04:05", f.String())
		if err != nil {
			return nil
		}
		return  &date
	}
	return nil
}

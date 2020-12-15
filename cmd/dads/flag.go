package main

type Flag struct {
	Value string
}

func (f *Flag) String() string {
	return f.Value
}

func (f *Flag) Set(val string) error {
	f.Value = val
	return nil
}


func NewFlag() *Flag {
	s := NewFlag()
	return s
}
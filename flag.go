package dads

type StringFlag string

/*type Flag struct {
	Value string
}*/

func (f *StringFlag) String() string {
	if f != nil {
		return string(*f)
	}

	return ""
}

func (f *StringFlag) Set(val string) error {
	*f = StringFlag(val)
	return nil
}

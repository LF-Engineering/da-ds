package uuid

import "testing"

func TestGenerate(t *testing.T) {
	args := []string{" abc ", "123"}


	id, err := Generate(args...)
	if err != nil {
		t.Errorf("could not generate %v, error: %v", args, err)
		return
	}

	t.Logf("uuid: %s\nargs: %v", id, args)
}

package dockerhub

import (
	"testing"
)

func TestFetchItems(t *testing.T) {
	srv := &DSDockerhub{}

	ctx := &Ctx{}
	srv.ParseArgs(ctx)
	srv.FetchItems(ctx)
}

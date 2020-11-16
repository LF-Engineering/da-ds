package dockerhub

import (
	"fmt"
	dads "github.com/LF-Engineering/da-ds"
	"testing"
	"time"
)

func prepareManagerObject() *Manager {
	repos := []*Repository{
		{"envoyproxy", "envoy", "sds-cncf-envoy-dockerhub"},
		{"hyperledger", "explorer", "sds-hyperledger-explorer-dockerhub"},
	}

	manager := NewManager(
		"",
		"",
		"0.0.1",
		"0.0.1",
		true,
		false,
		"http://localhost:9200",
		"elastic",
		"changeme",
		60*time.Second,
		repos,
		"",
		false,
	)

	return manager
}

func TestSync(t *testing.T) {
	manager := prepareManagerObject()

	err := manager.Sync()
	if err != nil {
		t.Logf("error: %v", err)
	}
}

func TestUUID(t *testing.T) {
	ctx := &dads.Ctx{}
	ctx.LegacyUUID = true
	origin := fmt.Sprintf("%s/%s/%s", APIUrl, "envoyproxy", "envoy")

	uuid := dads.UUIDNonEmpty(ctx, origin, "1.60162948062991E9")

	t.Logf("UUID: %v", uuid)
}

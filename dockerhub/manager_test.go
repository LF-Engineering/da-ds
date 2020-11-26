package dockerhub

import (
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
		"http://elastic:changeme@localhost:9200",
		60*time.Second,
		repos,
		nil,
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

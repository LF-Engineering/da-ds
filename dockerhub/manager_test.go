package dockerhub

import (
	"testing"
	"time"
)

func prepareManagerObject() *Manager {
	repos := []*Repository{
		{"envoyproxy", "envoy"},
		{"hyperledger", "explorer"},
	}

	manager := NewManager(
		"",
		"",
		"0.0.1",
		"0.0.1",
		false,
		true,
		"http://localhost:9200",
		"elastic",
		"changeme",
		60*time.Second,
		repos,
		"",
		0,
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

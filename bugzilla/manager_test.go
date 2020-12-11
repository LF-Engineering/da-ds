package bugzilla

import (
	"github.com/LF-Engineering/da-ds/bugzilla/mocks"
	"testing"
)

// ManagerProvider...
type ManagerProvider interface {
	Sync() error
	buildServices(m *Manager) (*Fetcher, *Enricher, ESClientProvider, error)
}

func TestManagerSync(t *testing.T) {

	mocks.ManagerProvider.On("Sync", "").Run(func(){

	}).Return(nil)
}

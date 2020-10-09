package dads

import (
	"fmt"
	"sync"
	"time"
)

// DSStub - DS implementation for stub - does nothing at all, just presents a skeleton code
type DSStub struct {
	DS          string
	MultiOrigin bool // can we store multiple endpoints in a single index?
}

// ParseArgs - parse stub specific environment variables
func (j *DSStub) ParseArgs(ctx *Ctx) (err error) {
	// IMPL:
	j.DS = Stub
	return
}

// Validate - is current DS configuration OK?
func (j *DSStub) Validate() (err error) {
	// IMPL:
	return
}

// Name - return data source name
func (j *DSStub) Name() string {
	return j.DS
}

// Info - return DS configuration in a human readable form
func (j DSStub) Info() string {
	return fmt.Sprintf("%+v", j)
}

// CustomFetchRaw - is this datasource using custom fetch raw implementation?
func (j *DSStub) CustomFetchRaw() bool {
	return false
}

// FetchRaw - implement fetch raw data for stub datasource
func (j *DSStub) FetchRaw(ctx *Ctx) (err error) {
	Printf("%s should use generic FetchRaw()\n", j.DS)
	return
}

// CustomEnrich - is this datasource using custom enrich implementation?
func (j *DSStub) CustomEnrich() bool {
	return false
}

// Enrich - implement enrich data for stub datasource
func (j *DSStub) Enrich(ctx *Ctx) (err error) {
	Printf("%s should use generic Enrich()\n", j.DS)
	return
}

// FetchItems - implement enrich data for stub datasource
func (j *DSStub) FetchItems(ctx *Ctx) (err error) {
	// IMPL:
	var messages [][]byte
	// Process messages (possibly in threads)
	var (
		ch         chan error
		allMsgs    []interface{}
		allMsgsMtx *sync.Mutex
		escha      []chan error
		eschaMtx   *sync.Mutex
	)
	thrN := GetThreadsNum(ctx)
	if thrN > 1 {
		ch = make(chan error)
		allMsgsMtx = &sync.Mutex{}
		eschaMtx = &sync.Mutex{}
	}
	nThreads := 0
	processMsg := func(c chan error, msg []byte) (wch chan error, e error) {
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		// FIXME: Real data processing here
		if allMsgsMtx != nil {
			allMsgsMtx.Lock()
		}
		// FIXME: add item
		allMsgs = append(allMsgs, map[string]interface{}{"id": time.Now().UnixNano(), "name": "xyz"})
		nMsgs := len(allMsgs)
		if nMsgs >= ctx.ESBulkSize {
			sendToElastic := func(c chan error) (ee error) {
				defer func() {
					if c != nil {
						c <- ee
					}
				}()
				// FIXME: item ID column name
				ee = SendToElastic(ctx, j, true, "FIXME", allMsgs)
				if ee != nil {
					Printf("error %v sending %d messages to ElasticSearch\n", ee, len(allMsgs))
				}
				allMsgs = []interface{}{}
				if allMsgsMtx != nil {
					allMsgsMtx.Unlock()
				}
				return
			}
			if thrN > 1 {
				wch = make(chan error)
				go func() {
					_ = sendToElastic(wch)
				}()
			} else {
				e = sendToElastic(nil)
				if e != nil {
					return
				}
			}
		} else {
			if allMsgsMtx != nil {
				allMsgsMtx.Unlock()
			}
		}
		return
	}
	if thrN > 1 {
		for _, message := range messages {
			go func(msg []byte) {
				var esch chan error
				esch, err = processMsg(ch, msg)
				if err != nil {
					return
				}
				if esch != nil {
					if eschaMtx != nil {
						eschaMtx.Lock()
					}
					escha = append(escha, esch)
					if eschaMtx != nil {
						eschaMtx.Unlock()
					}
				}
			}(message)
			nThreads++
			if nThreads == thrN {
				err = <-ch
				if err != nil {
					return
				}
				nThreads--
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				return
			}
		}
	} else {
		for _, message := range messages {
			_, err = processMsg(nil, message)
			if err != nil {
				return
			}
		}
	}
	for _, esch := range escha {
		err = <-esch
		if err != nil {
			return
		}
	}
	nMsgs := len(allMsgs)
	if ctx.Debug > 0 {
		Printf("%d remaining messages to send to ES\n", nMsgs)
	}
	if nMsgs > 0 {
		// FIXME: id item name
		err = SendToElastic(ctx, j, true, "FIXME", allMsgs)
		if err != nil {
			Printf("Error %v sending %d messages to ES\n", err, len(allMsgs))
		}
	}
	return
}

// SupportDateFrom - does DS support resuming from date?
func (j *DSStub) SupportDateFrom() bool {
	// IMPL:
	return false
}

// SupportOffsetFrom - does DS support resuming from offset?
func (j *DSStub) SupportOffsetFrom() bool {
	// IMPL:
	return false
}

// DateField - return date field used to detect where to restart from
func (j *DSStub) DateField(*Ctx) string {
	return DefaultDateField
}

// RichIDField - return rich ID field name
func (j *DSStub) RichIDField(*Ctx) string {
	return DefaultIDField
}

// RichAuthorField - return rich ID field name
func (j *DSStub) RichAuthorField(*Ctx) string {
	return DefaultAuthorField
}

// OffsetField - return offset field used to detect where to restart from
func (j *DSStub) OffsetField(*Ctx) string {
	return DefaultOffsetField
}

// OriginField - return origin field used to detect where to restart from
func (j *DSStub) OriginField(ctx *Ctx) string {
	if ctx.Tag != "" {
		return DefaultTagField
	}
	return DefaultOriginField
}

// Categories - return a set of configured categories
func (j *DSStub) Categories() map[string]struct{} {
	// IMPL:
	return map[string]struct{}{}
}

// ResumeNeedsOrigin - is origin field needed when resuming
// Origin should be needed when multiple configurations save to the same index
func (j *DSStub) ResumeNeedsOrigin(ctx *Ctx) bool {
	return j.MultiOrigin
}

// Origin - return current origin
func (j *DSStub) Origin(ctx *Ctx) string {
	// IMPL: you must change this, for example to j.URL/j.GroupName or somethign like this
	return ctx.Tag
}

// ItemID - return unique identifier for an item
func (j *DSStub) ItemID(item interface{}) string {
	// IMPL:
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// ItemUpdatedOn - return updated on date for an item
func (j *DSStub) ItemUpdatedOn(item interface{}) time.Time {
	// IMPL:
	return time.Now()
}

// ItemCategory - return unique identifier for an item
func (j *DSStub) ItemCategory(item interface{}) string {
	// IMPL:
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// SearchFields - define (optional) search fields to be returned
func (j *DSStub) SearchFields() map[string][]string {
	return map[string][]string{}
}

// ElasticRawMapping - Raw index mapping definition
func (j *DSStub) ElasticRawMapping() []byte {
	// IMPL:
	return []byte{}
}

// ElasticRichMapping - Rich index mapping definition
func (j *DSStub) ElasticRichMapping() []byte {
	// IMPL:
	return []byte{}
}

// GetItemIdentities return list of item's identities, each one is [3]string
// (name, username, email) tripples, special value Nil "<nil>" means null
// we use string and not *string which allows nil to allow usage as a map key
func (j *DSStub) GetItemIdentities(ctx *Ctx, doc interface{}) (map[[3]string]struct{}, error) {
	// IMPL:
	return map[[3]string]struct{}{}, nil
}

// EnrichItems - perform the enrichment
func (j *DSStub) EnrichItems(ctx *Ctx) (err error) {
	// IMPL:
	return
}

// EnrichItem - return rich item from raw item for a given author type
func (j *DSStub) EnrichItem(ctx *Ctx, item map[string]interface{}, author string, affs bool) (rich map[string]interface{}, err error) {
	// IMPL:
	rich = item
	return
}

// AffsItems - return affiliations data items for given roles and date
func (j *DSStub) AffsItems(ctx *Ctx, rawItem map[string]interface{}, roles []string, date interface{}) (affsItems map[string]interface{}, err error) {
	// IMPL:
	return
}

// GetRoleIdentity - return identity data for a given role
func (j *DSStub) GetRoleIdentity(ctx *Ctx, item map[string]interface{}, role string) map[string]interface{} {
	// IMPL:
	return map[string]interface{}{"name": nil, "username": nil, "email": nil}
}

// AllRoles - return all roles defined for Jira backend
func (j *DSStub) AllRoles(ctx *Ctx) []string {
	// IMPL:
	return []string{"author"}
}

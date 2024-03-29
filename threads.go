package dads

import (
	"runtime"
	"sync"
)

var (
	// MT - are we running in multiple threading mode?
	MT      = false
	thrN    = 0
	thrNMtx = &sync.Mutex{}
)

// SetMT - we're in multithreaded mode, setup global caches mutexes
func SetMT() {
	if uuidsNonEmptyCacheMtx == nil {
		uuidsNonEmptyCacheMtx = &sync.RWMutex{}
	}
	if uuidsAffsCacheMtx == nil {
		uuidsAffsCacheMtx = &sync.RWMutex{}
	}
	if identityCacheMtx == nil {
		identityCacheMtx = &sync.RWMutex{}
	}
	if rollsCacheMtx == nil {
		rollsCacheMtx = &sync.RWMutex{}
	}
	if i2uCacheMtx == nil {
		i2uCacheMtx = &sync.RWMutex{}
	}
	if emailsCacheMtx == nil {
		emailsCacheMtx = &sync.RWMutex{}
	}
	if esCacheMtx == nil {
		esCacheMtx = &sync.RWMutex{}
	}
	if memCacheMtx == nil {
		memCacheMtx = &sync.RWMutex{}
	}
	if parseDateCacheMtx == nil {
		parseDateCacheMtx = &sync.RWMutex{}
	}
	if gTokenEnvMtx == nil {
		gTokenEnvMtx = &sync.Mutex{}
	}
	if gTokenMtx == nil {
		gTokenMtx = &sync.Mutex{}
	}
	MT = true
}

// ResetThreadsNum - allows clearing current setting so the new one can be applied
func ResetThreadsNum(ctx *Ctx) {
	thrNMtx.Lock()
	defer thrNMtx.Unlock()
	thrN = 0
	MT = false
	uuidsNonEmptyCacheMtx = nil
	uuidsAffsCacheMtx = nil
	identityCacheMtx = nil
	rollsCacheMtx = nil
	i2uCacheMtx = nil
	emailsCacheMtx = nil
	esCacheMtx = nil
	memCacheMtx = nil
	parseDateCacheMtx = nil
	gTokenEnvMtx = nil
	gTokenMtx = nil
}

// GetThreadsNum returns the number of available CPUs
// If environment variable DA_DS_ST is set it retuns 1
// It can be used to debug single threaded verion
func GetThreadsNum(ctx *Ctx) int {
	thrNMtx.Lock()
	defer thrNMtx.Unlock()
	if thrN > 0 {
		return thrN
	}
	defer func() {
		if ctx.Debug > 0 {
			Printf("using %d threads\n", thrN)
		}
	}()
	if ctx.ST {
		thrN = 1
		return thrN
	}
	// Use environment variable to have singlethreaded version
	if ctx.NCPUs > 0 {
		n := int(float64(runtime.NumCPU()) * ctx.NCPUsScale)
		if ctx.NCPUs > n {
			ctx.NCPUs = n
		}
		runtime.GOMAXPROCS(ctx.NCPUs)
		thrN = ctx.NCPUs
		if thrN > 1 {
			SetMT()
		}
		return thrN
	}
	thrN = int(float64(runtime.NumCPU()) * ctx.NCPUsScale)
	runtime.GOMAXPROCS(thrN)
	if thrN > 1 {
		SetMT()
	}
	return thrN
}

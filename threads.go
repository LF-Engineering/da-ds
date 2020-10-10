package dads

import (
	"runtime"
	"sync"
)

var (
	// MT -are we running in multiple threading mode?
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
	MT = true
}

// GetThreadsNum returns the number of available CPUs
// If environment variable GHA_ST is set it retuns 1
// It can be used to debug single threaded verion
func GetThreadsNum(ctx *Ctx) int {
	thrNMtx.Lock()
	defer thrNMtx.Unlock()
	if thrN > 0 {
		return thrN
	}
	defer func() { Printf("using %d threads\n", thrN) }()
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
	if ctx.ST {
		thrN = 1
		return thrN
	}
	thrN = int(float64(runtime.NumCPU()) * ctx.NCPUsScale)
	runtime.GOMAXPROCS(thrN)
	if thrN > 1 {
		SetMT()
	}
	return thrN
}

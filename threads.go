package dads

import (
	"runtime"
	"sync"
)

var (
	// MT -are we running in multiple threading mode?
	MT = false
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
	// Use environment variable to have singlethreaded version
	if ctx.NCPUs > 0 {
		n := int(float64(runtime.NumCPU()) * ctx.NCPUsScale)
		if ctx.NCPUs > n {
			ctx.NCPUs = n
		}
		runtime.GOMAXPROCS(ctx.NCPUs)
		if ctx.NCPUs > 1 {
			SetMT()
		}
		return ctx.NCPUs
	}
	if ctx.ST {
		return 1
	}
	thrN := int(float64(runtime.NumCPU()) * ctx.NCPUsScale)
	runtime.GOMAXPROCS(thrN)
	if thrN > 1 {
		SetMT()
	}
	return thrN
}

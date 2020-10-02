package dads

import (
	"runtime"
	"sync"
)

// SetMT - we're in multithreaded mode, setup global caches mutexes
func SetMT() {
	if uuidsCacheNonEmptyMtx == nil {
		uuidsCacheNonEmptyMtx = &sync.RWMutex{}
	}
	if uuidsCacheAffsMtx == nil {
		uuidsCacheAffsMtx = &sync.RWMutex{}
	}
	if identityCacheMtx == nil {
		identityCacheMtx = &sync.RWMutex{}
	}
	if rollsCacheMtx == nil {
		rollsCacheMtx = &sync.RWMutex{}
	}
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

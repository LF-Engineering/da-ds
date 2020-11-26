package utils

import (
	"math"
	"time"
)

// ConvertTimeToFloat ...
func ConvertTimeToFloat(t time.Time) float64 {
	return math.Floor(float64(t.UnixNano())/float64(time.Second)*1e6) / 1e6
}

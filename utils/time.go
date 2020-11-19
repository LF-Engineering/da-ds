package utils

import (
	"math"
	"time"
)

func ConvertTimeToFloat(t time.Time) float64 {
	return math.Floor(float64(t.UnixNano()) / float64(time.Second)*1E6)/1E6
}

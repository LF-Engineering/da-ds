package utils

import (
	"math"
	"time"
)

// ConvertTimeToFloat ...
func ConvertTimeToFloat(t time.Time) float64 {
	return math.Round(float64(t.UnixNano())/float64(time.Second)*1e6) / 1e6
}

// GetDaysbetweenDates calculate days between two dates
func GetDaysbetweenDates(t1 time.Time, t2 time.Time) float64 {
	var res float64
	if t1.IsZero() || t2.IsZero() {
		res = 0.0
	}else{
		res = t1.Sub(t2).Hours()
	}
	return res
}

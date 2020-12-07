package utils

import (
	"fmt"
	"math"
	"time"
)

// ConvertTimeToFloat ...
func ConvertTimeToFloat(t time.Time) float64 {
	return math.Round(float64(t.UnixNano())/float64(time.Second)*1e6) / 1e6
}

// GetDaysbetweenDates calculate days between two dates
func GetDaysbetweenDates(t1 time.Time, t2 time.Time) float64 {
	res := t1.Sub(t2).Hours() / 24
	fmt.Println("jjjjjjj")
	fmt.Println(t1)
	fmt.Println(t2)
	fmt.Println(res)
	return res
}

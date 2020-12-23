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
	res := t1.Sub(t2).Hours() / 24
	return res
}

// GetOldestDate get the older date between two nullable dates
func GetOldestDate(t1 *time.Time, t2 *time.Time) *time.Time {
	from, err := time.Parse("2006-01-02 15:04:05", "1970-01-01 00:00:00")
	if err != nil {
		return nil
	}

	isT1Empty := t1 == nil || t1.IsZero()
	isT2Empty := t2 == nil || t2.IsZero()

	if isT1Empty && !isT2Empty {
		from = *t2
	} else if !isT1Empty && isT2Empty {
		from = *t1
	} else if !isT1Empty && !isT2Empty {
		from = *t2
		if t1.Before(*t2) {
			from = *t1
		}
	}

	return &from
}

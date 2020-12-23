package utils

import (
	"fmt"
	"log"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFloatTime(t *testing.T) {
	x := ConvertTimeToFloat(time.Now())
	sec, dec := math.Modf(x)
	fmt.Printf("%v\n", time.Unix(int64(sec), int64(dec)))
	fmt.Printf("%v\n", x)
}

func TestGetOldestDate(t *testing.T) {

	// Arrange
	from, err := time.Parse("2006-01-02 15:04:05", "1970-01-01 00:00:00")
	if err != nil {
		fmt.Println(err)
	}
	var d1 time.Time
	d1, _ = time.Parse("2006-01-02 15:04:05", "2020-01-01 00:00:00")

	var d2 time.Time
	d2, _ = time.Parse("2006-01-02 15:04:05", "2020-05-01 00:00:00")

	// Act
	case2nil := GetOldestDate(nil, nil)
	case1 := GetOldestDate(&d1, nil)
	case2 := GetOldestDate(nil, &d1)
	case1after2 := GetOldestDate(&d2, &d1)
	case1before2 := GetOldestDate(&d1, &d2)

	// Assert
	assert.Equal(t, case2nil, &from)
	assert.Equal(t, case1, &d1)
	assert.Equal(t, case2, &d1)
	assert.Equal(t, case1after2, &d1)
	assert.Equal(t, case1before2, &d1)

}

func TestFloatTime2(t *testing.T) {
	type item struct {
		name   string
		result float64
		input  string
	}

	items := []item{
		{
			"hyperledger-aries",
			1.605760484366669e9,
			"2020-11-19T04:34:44.366669+00:00",
		},
		{
			"hyperledger-aries_old",
			1.578593623000436e9,
			"2020-01-09T18:13:43.000436+00:00",
		},
		{
			"hyperledger-explorer-db",
			1.605760867976719e9,
			"2020-11-19T04:41:07.976719+00:00",
		},
		{
			"hyperledger-explorer-db_old",
			1.578590780539522e9,
			"2020-01-09T17:26:20.539522+00:00",
		},
		{
			"yocto-eol",
			1.605787742136769e9,
			"2020-11-19T12:09:02.136769+00:00",
		},
		{
			"yocto-eol_old",
			1.596145933258305e9,
			"2020-07-30T21:52:13.258305+00:00",
		},
		{
			"fluentd-kubernetes-daemonset",
			1.605748553332666e9,
			"2020-11-19T01:15:53.332666+00:00",
		},
		{
			"fluentd-kubernetes-daemonset_old",
			1.596835322901541e9,
			"2020-08-07T21:22:02.901541+00:00",
		},
		{
			"envoy",
			1.605748149761947e9,
			"2020-11-19T01:09:09.761947+00:00",
		},
		{
			"envoy_old",
			1.596835145490981e9,
			"2020-08-07T21:19:05.490981+00:00",
		},
	}

	for _, i := range items {
		t.Run(i.name, func(t *testing.T) {
			tm, err := time.Parse(time.RFC3339Nano, i.input)
			if err != nil {
				log.Fatal(err)
			}

			result := ConvertTimeToFloat(tm)
			fmt.Println(fmt.Sprintf("%f", i.result), fmt.Sprintf("%f", result))
			assert.Equal(t, i.result, result)
		})
	}
}

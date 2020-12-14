package pipermail

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func TrimFirstDot(s string) string {
	st := strings.Split(s, ".")
	return st[0]
}

func TrimFirstDash(s string) (year, month string) {
	m := strings.Split(s, "-")
	return m[0], m[1]
}

func ParseDateFromFilePath(path string) time.Time {
	layoutISO := "2006-1-2"
	baseName := filepath.Base(path)
	year, month := TrimFirstDash(TrimFirstDot(baseName))
	monthVal := MONTHS[month]
	date := fmt.Sprintf("%s-%+v-1", year, monthVal)
	t, err := time.Parse(layoutISO, date)
	if err != nil {
		fmt.Println(baseName)
		fmt.Println(err)
	}
	return t
}

func DateTimeToUTC(date string) time.Time {
	layout := "2006-01-02T15:04:05.000Z"
	t, err := time.Parse(layout, date)
	if err != nil {
		fmt.Println(err)
	}
	return t
}

// DownloadFile will download a url to a local file. It's efficient because it will
// write as it downloads and not load the whole file into memory.
func DownloadFile(url, filepath string) error {
	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	return err
}

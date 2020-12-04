package bugzilla

import (
	"bytes"

	"github.com/PuerkitoBio/goquery"
)

// GetLen gets count of searched items
func GetActivityLen(query string, body []byte) (int, error) {
	r := bytes.NewReader(body)
	doc, err := goquery.NewDocumentFromReader(r)
	if err != nil {
		return 0, err
	}

	activityCount := 0
	doc.Find(query).Each(func(i int, selection *goquery.Selection) {
		if len(selection.Find("td").Nodes) == 5 {
			activityCount++
		}
	})

	return activityCount, nil
}

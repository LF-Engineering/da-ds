package bugzilla

import (
	"bytes"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

// GetActivityLen gets count of searched items
func GetActivityLen(query string, body []byte) (int, []Activity, error) {
	r := bytes.NewReader(body)
	doc, err := goquery.NewDocumentFromReader(r)
	act := make([]Activity, 0)
	if err != nil {
		return 0, act, err
	}
	activityCount := 0
	doc.Find(query).Each(func(i int, selection *goquery.Selection) {
		if len(selection.Find("td").Nodes) == 5 {
			var ac Activity
			selection.Find("td").Each(func(x int, selection *goquery.Selection) {
				val := strings.TrimPrefix(strings.TrimSuffix(strings.TrimSpace(selection.Text()), "\""), "\"")
				switch x {
				case 0:
					ac.Who = val
					break
				case 1:
					ac.When = val
					break
				case 2:
					ac.What = val
					break
				case 3:
					ac.Removed = val
					break
				case 4:
					ac.Added = val
					break
				}
			})
			act = append(act, ac)
			activityCount++
		}
	})

	return activityCount, act, nil
}

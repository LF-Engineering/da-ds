package utils

import (
	"bytes"

	"github.com/PuerkitoBio/goquery"
)

// GetLen gets count of searched items
func GetLen(query string, body []byte) (int, error) {
	r := bytes.NewReader(body)
	doc, err := goquery.NewDocumentFromReader(r)
	if err != nil {
		return 0, err
	}

	count := len(doc.Find(query).Nodes)

	return count, nil
}

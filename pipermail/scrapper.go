package pipermail

import (
	"bytes"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"net/http"
	"path/filepath"
	"strings"
)

// ParseArchiveLinks scraps the contents of a given url to extract compressed files
// download links
func (f *Fetcher) ParseArchiveLinks(archivesURL string) ([]string, error) {
	// get all accepted & compressed types into one list
	CombinedTypes = append(CombinedTypes, CompressedTypes...)
	CombinedTypes = append(CombinedTypes, AcceptedTypes...)

	headers := map[string]string{}
	headers["pragma"] = "no-cache"
	headers["cache-control"] = "no-cache"
	headers["dnt"] = "1"
	headers["upgrade-insecure-requests"] = "1"
	headers["referer"] = archivesURL

	var links []string

	statusCode, resBody, err := f.HTTPClientProvider.Request(archivesURL, "GET", headers, nil, nil)
	if err != nil || statusCode != http.StatusOK {
		return nil, err
	}
	if statusCode == http.StatusOK {
		doc, err := goquery.NewDocumentFromReader(bytes.NewReader(resBody))

		if err != nil {
			fmt.Println(err)
		}

		doc.Find("a[href]").Each(func(i int, s *goquery.Selection) {
			link, _ := s.Attr("href")

			link = archivesURL + link

			// Make sure you we only fetch correct URL with corresponding title
			if strings.Contains(link, ".") {
				text := s.Text()
				// filter out unnecessary links
				if text != "" && text != "more" {
					links = append(links, link)
				}
			}

		})
	}

	var sortedLinks []string
	for _, link := range links {
		// Links from Apache's 'mod_mbox' plugin contain
		// trailing "/thread" substrings. Remove them to get
		// the links where mbox files are stored.
		if strings.HasSuffix(link, ModMboxThreadStr) {
			link = strings.TrimSuffix(link, ModMboxThreadStr)
		}

		// inspect the first extension for any accepted compressed types
		// ie ".gz", ".bz2", ".zip", ".tar", ".tar.gz", ".tar.bz2", ".tgz", ".tbz"
		_, ext1 := f.Find(CombinedTypes, filepath.Ext(link))

		// get the second extension. piper mail extensions are in the format https://mails.dpdk.org/archives/users/2016-March.txt.gz
		// thus have two extensions.
		secondExtension := strings.TrimSuffix(link, ext1)

		// inspect the second extension for any accepted types
		// ie ".mbox", ".txt"
		_, ext2 := f.Find(CombinedTypes, filepath.Ext(secondExtension))

		if ext1 != "" || ext2 != "" {
			sortedLinks = append(sortedLinks, link)
		}
	}
	return sortedLinks, nil
}

// Find takes a slice and looks for an element in it. If found it will
// return it's true, otherwise it will return a bool of false.
func (f *Fetcher) Find(slice []string, val string) (bool, string) {
	for _, item := range slice {
		if item == val {
			return true, item
		}
	}
	return false, ""
}

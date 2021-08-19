package util

import (
	"encoding/json"
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"
)

var emailRegex = regexp.MustCompile(`^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,4}$`)

// IsEmailValid  validate email address
func IsEmailValid(e string) bool {
	if len(e) < 3 && len(e) > 254 {
		return false
	}

	if !emailRegex.MatchString(e) {
		return false
	}

	parts := strings.Split(e, "@")
	mx, err := net.LookupMX(parts[1])
	if err != nil || len(mx) == 0 {
		return false
	}

	return true
}

// GetEnrollments get identity single and multi organization
func GetEnrollments(auth0ClientProvider Auth0Client, httpClientProvider HTTPClientProvider, AffBaseURL string, projectSlug string, uuid string, date time.Time) (string, []string, error) {
	// space in projectSlug is to handle empty slug which encounter invalid url, it is optional and whatever you send it will return the same enrollment result
	if projectSlug == "" {
		projectSlug = " "
	}
	URL := fmt.Sprintf("%s/affiliation/%s/both/%s/%s", AffBaseURL, projectSlug, uuid, date.Format("2006-02-01 15:04:05"))
	token, err := auth0ClientProvider.GetToken()
	if err != nil {
		return "", []string{}, err
	}

	headers := make(map[string]string)
	headers["Authorization"] = "Bearer " + token

	_, body, err := httpClientProvider.Request(URL, "GET", headers, nil, nil)
	if err != nil {
		return "", []string{}, err
	}

	var res EnrollmentOrgs
	err = json.Unmarshal(body, &res)
	if err != nil {
		return "", []string{}, err
	}
	return res.Org, res.Orgs, nil

}

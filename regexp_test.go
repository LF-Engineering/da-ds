package dads

import (
	"regexp"
	"strings"
	"testing"

	lib "github.com/LF-Engineering/da-ds"
)

func TestREs(t *testing.T) {
	var testCases = []struct {
		res      string
		str      string
		expected map[string]string
	}{
		{
			res: lib.GitCommitPattern.String(),
			str: "commit a8ea74789413b83a44a3edaf064c6be976968d9f",
			expected: map[string]string{
				"commit":  "a8ea74789413b83a44a3edaf064c6be976968d9f",
				"parents": "",
				"refs":    "",
			},
		},
		{
			res: lib.GitCommitPattern.String(),
			str: "commit 0979a920f7b2a5278fccea0d71c875d60bb8e3a3 363a8f133dc3adcb149e1c9e9ad801840ef39538",
			expected: map[string]string{
				"commit":  "0979a920f7b2a5278fccea0d71c875d60bb8e3a3",
				"parents": "363a8f133dc3adcb149e1c9e9ad801840ef39538",
				"refs":    "",
			},
		},
		{
			res: lib.GitCommitPattern.String(),
			str: "commit 294c2079398417d6b046f4a11eccd9a21a7e51a6 0979a920f7b2a5278fccea0d71c875d60bb8e3a3 (HEAD -> refs/heads/master, refs/remotes/origin/master, refs/remotes/origin/HEAD)",
			expected: map[string]string{
				"commit":  "294c2079398417d6b046f4a11eccd9a21a7e51a6",
				"parents": "0979a920f7b2a5278fccea0d71c875d60bb8e3a3",
				"refs":    "HEAD -> refs/heads/master, refs/remotes/origin/master, refs/remotes/origin/HEAD",
			},
		},
		{
			res: lib.GitHeaderPattern.String(),
			str: "Author:     Łukasz Gryglicki <lgryglicki@cncf.io>",
			expected: map[string]string{
				"name":  "Author",
				"value": "Łukasz Gryglicki <lgryglicki@cncf.io>",
			},
		},
		{
			res: lib.GitMessagePattern.String(),
			str: "    this is a message ",
			expected: map[string]string{
				"msg": "this is a message ",
			},
		},
		{
			res: lib.GitTrailerPattern.String(),
			str: "Signed-off-by:\tŁukasz Gryglicki <lukaszgryglicki@o2.pl>",
			expected: map[string]string{
				"name":  "Signed-off-by",
				"value": "Łukasz Gryglicki <lukaszgryglicki@o2.pl>",
			},
		},
		{
			res: lib.GitActionPattern.String(),
			str: "::100644 100644 100644 aaf94db0 e82bf6fa e82bf6fa MC\t\t.circleci/deployments/develop/api.deployment.yml.erb",
			expected: map[string]string{
				"sc":      "::",
				"modes":   "100644 100644 100644 ",
				"indexes": "aaf94db0 e82bf6fa e82bf6fa ",
				"action":  "MC",
				"file":    ".circleci/deployments/develop/api.deployment.yml.erb",
				"newfile": "",
			},
		},
		{
			res: lib.GitActionPattern.String(),
			str: ":000000 100644 0000000 66fd13c A\t.gitignore",
			expected: map[string]string{
				"sc":      ":",
				"modes":   "000000 100644 ",
				"indexes": "0000000 66fd13c ",
				"action":  "A",
				"file":    ".gitignore",
				"newfile": "",
			},
		},
		{
			res: lib.GitStatsPattern.String(),
			str: "2 4 .circleci/deployments/{production => prod}/api.deployment.yml.erb",
			expected: map[string]string{
				"added":   "2",
				"removed": "4",
				"file":    ".circleci/deployments/{production => prod}/api.deployment.yml.erb",
			},
		},
		{
			res: lib.GitAuthorsPattern.String(),
			str: "Lukasz Gryglicki and Justyna Gryglicka <us@cncf.io>",
			expected: map[string]string{
				"first_authors": "Lukasz Gryglicki",
				"last_author":   "Justyna Gryglicka",
				"email":         "<us@cncf.io>",
			},
		},
		{
			res: lib.GitAuthorsPattern.String(),
			str: "Lukasz Gryglicki, Alicja Gryglicka, Krzysztof Gryglicki and Justyna Gryglicka <family@cncf.io>",
			expected: map[string]string{
				"first_authors": "Lukasz Gryglicki, Alicja Gryglicka, Krzysztof Gryglicki",
				"last_author":   "Justyna Gryglicka",
				"email":         "<family@cncf.io>",
			},
		},
		{
			res: lib.GitAuthorsPattern.String(),
			str: "Lukasz Gryglicki <lgryglicki@cncf.io> and Justyna Gryglicka <jgryglicka@cncf.io> <us@cncf.io>",
			expected: map[string]string{
				"first_authors": "Lukasz Gryglicki <lgryglicki@cncf.io>",
				"last_author":   "Justyna Gryglicka <jgryglicka@cncf.io>",
				"email":         "<us@cncf.io>",
			},
		},
		{
			res: lib.GitCoAuthorsPattern.String(),
			str: "Co-authored-by:Lukasz Gryglicki<lgryglicki@cncf.io>",
			expected: map[string]string{
				"first_authors": "Lukasz Gryglicki",
				"email":         "lgryglicki@cncf.io",
			},
		},
		{
			res: lib.GitCoAuthorsPattern.String(),
			str: "Co-authored-by:Lukasz Gryglicki<lgryglicki@cncf.io>\nCo-authored-by:Justyna Gryglicka<jgryglicka@cncf.io>",
			expected: map[string]string{
				"first_authors": "Lukasz Gryglicki",
				"email":         "lgryglicki@cncf.io",
			},
		},
	}
	sameResult := func(a1, a2 map[string]string) bool {
		m1 := make(map[[2]string]struct{})
		m2 := make(map[[2]string]struct{})
		for k, v := range a1 {
			m1[[2]string{k, v}] = struct{}{}
		}
		for k, v := range a2 {
			m2[[2]string{k, v}] = struct{}{}
		}
		for k := range m1 {
			_, ok := m2[k]
			if !ok {
				return false
			}
		}
		for k := range m2 {
			_, ok := m1[k]
			if !ok {
				return false
			}
		}
		return true
	}
	for index, test := range testCases {
		re := regexp.MustCompile(test.res)
		got := lib.MatchGroups(re, test.str)
		if !sameResult(got, test.expected) {
			t.Errorf("test number %d, expected '%s' matching '%s' result %v, got %v", index+1, test.str, test.res, test.expected, got)
		}
	}
}

func TestArrayREs(t *testing.T) {
	var testCases = []struct {
		res      string
		str      string
		expected map[string][]string
	}{
		{
			res: lib.GitCoAuthorsPattern.String(),
			str: "Co-authored-by:Lukasz Gryglicki<lgryglicki@cncf.io>\nCo-authored-by:Justyna Gryglicka<jgryglicka@cncf.io>\n",
			expected: map[string][]string{
				"first_authors": {"Lukasz Gryglicki", "Justyna Gryglicka"},
				"email":         {"lgryglicki@cncf.io", "jgryglicka@cncf.io"},
			},
		},
	}
	sameResult := func(a1, a2 map[string][]string) bool {
		m1 := make(map[[2]string]struct{})
		m2 := make(map[[2]string]struct{})
		for k, va := range a1 {
			v := strings.Join(va, ",")
			m1[[2]string{k, v}] = struct{}{}
		}
		for k, va := range a2 {
			v := strings.Join(va, ",")
			m2[[2]string{k, v}] = struct{}{}
		}
		for k := range m1 {
			_, ok := m2[k]
			if !ok {
				return false
			}
		}
		for k := range m2 {
			_, ok := m1[k]
			if !ok {
				return false
			}
		}
		return true
	}
	for index, test := range testCases {
		re := regexp.MustCompile(test.res)
		got := lib.MatchGroupsArray(re, test.str)
		if !sameResult(got, test.expected) {
			t.Errorf("test number %d, expected '%s' matching '%s' result %v, got %v", index+1, test.str, test.res, test.expected, got)
		}
	}
}

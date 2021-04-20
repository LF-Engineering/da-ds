package dads

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/go-github/v35/github"
	jsoniter "github.com/json-iterator/go"
	"golang.org/x/oauth2"
)

const (
	// GitHubBackendVersion - backend version
	GitHubBackendVersion = "0.1.0"
	// cMaxGitHubUsersFileCacheAge 90 days (in seconds) - file is considered too old anywhere between 90-180 days
	cMaxGitHubUsersFileCacheAge = 7776000
)

var (
	// GitHubRawMapping - GitHub raw index mapping
	GitHubRawMapping = []byte(`{"properties":{"metadata__updated_on":{"type":"date"}}}`)
	// GitHubRichMapping - GitHub rich index mapping
	GitHubRichMapping = []byte(`{"properties":{"metadata__updated_on":{"type":"date"},"merge_author_geolocation":{"type":"geo_point"},"assignee_geolocation":{"type":"geo_point"},"state":{"type":"keyword"},"user_geolocation":{"type":"geo_point"},"title_analyzed":{"type":"text","index":true}}}`)
	// GitHubCategories - categories defined for GitHub
	GitHubCategories = map[string]struct{}{"issue": {}, "pull_request": {}, "repository": {}}
)

// DSGitHub - DS implementation for GitHub
type DSGitHub struct {
	DS                        string // From DA_DS - data source type "github"
	Org                       string // From DA_GITHUB_ORG - github org
	Repo                      string // From DA_GITHUB_REPO - github repo
	Category                  string // From DA_GITHUB_CATEGORY - issue, pull_request, repository
	Tokens                    string // From DA_GITHUB_TOKENS - "," separated list of OAuth tokens
	URL                       string
	Clients                   []*github.Client
	Context                   context.Context
	OAuthKeys                 []string
	ThrN                      int
	Hint                      int
	CacheDir                  string
	GitHubMtx                 *sync.RWMutex
	GitHubReposMtx            *sync.RWMutex
	GitHubRepos               map[string]map[string]interface{}
	GitHubIssuesMtx           *sync.RWMutex
	GitHubIssues              map[string][]map[string]interface{}
	GitHubUserMtx             *sync.RWMutex
	GitHubUser                map[string]map[string]interface{}
	GitHubIssueCommentsMtx    *sync.RWMutex
	GitHubIssueComments       map[string][]map[string]interface{}
	GitHubCommentReactionsMtx *sync.RWMutex
	GitHubCommentReactions    map[string][]map[string]interface{}
}

func (j *DSGitHub) getRateLimits(gctx context.Context, ctx *Ctx, gcs []*github.Client, core bool) (int, []int, []int, []time.Duration) {
	var (
		limits     []int
		remainings []int
		durations  []time.Duration
	)
	display := false
	for idx, gc := range gcs {
		rl, _, err := gc.RateLimits(gctx)
		if err != nil {
			rem, ok := PeriodParse(err.Error())
			if ok {
				Printf("Parsed wait time from error message: %v\n", rem)
				limits = append(limits, -1)
				remainings = append(remainings, -1)
				durations = append(durations, rem)
				display = true
				continue
			}
			Printf("GetRateLimit(%d): %v\n", idx, err)
		}
		if rl == nil {
			limits = append(limits, -1)
			remainings = append(remainings, -1)
			durations = append(durations, time.Duration(5)*time.Second)
			continue
		}
		if core {
			limits = append(limits, rl.Core.Limit)
			remainings = append(remainings, rl.Core.Remaining)
			durations = append(durations, rl.Core.Reset.Time.Sub(time.Now())+time.Duration(1)*time.Second)
			continue
		}
		limits = append(limits, rl.Search.Limit)
		remainings = append(remainings, rl.Search.Remaining)
		durations = append(durations, rl.Search.Reset.Time.Sub(time.Now())+time.Duration(1)*time.Second)
	}
	hint := 0
	for idx := range limits {
		if remainings[idx] > remainings[hint] {
			hint = idx
		} else if idx != hint && remainings[idx] == remainings[hint] && durations[idx] < durations[hint] {
			hint = idx
		}
	}
	if display || ctx.Debug > 0 {
		Printf("GetRateLimits: hint: %d, limits: %+v, remaining: %+v, reset: %+v\n", hint, limits, remainings, durations)
	}
	return hint, limits, remainings, durations
}

func (j *DSGitHub) handleRate(ctx *Ctx) (aHint int, canCache bool) {
	h, _, rem, wait := j.getRateLimits(j.Context, ctx, j.Clients, true)
	for {
		// Printf("Checking token %d %+v %+v\n", h, rem, wait)
		if rem[h] <= 5 {
			Printf("All GH API tokens are overloaded, maximum points %d, waiting %+v\n", rem[h], wait[h])
			time.Sleep(time.Duration(1) * time.Second)
			time.Sleep(wait[h])
			h, _, rem, wait = j.getRateLimits(j.Context, ctx, j.Clients, true)
			continue
		}
		if rem[h] >= 500 {
			canCache = true
		}
		break
	}
	aHint = h
	j.Hint = aHint
	// Printf("Found usable token %d/%d/%v, cache enabled: %v\n", aHint, rem[h], wait[h], canCache)
	return
}

func (j *DSGitHub) isAbuse(e error) bool {
	if e == nil {
		return false
	}
	errStr := e.Error()
	return strings.Contains(errStr, "403 You have triggered an abuse detection mechanism") || strings.Contains(errStr, "403 API rate limit")
}

func (j *DSGitHub) githubRepos(ctx *Ctx, org, repo string) (repoData map[string]interface{}, err error) {
	origin := org + "/" + repo
	// Try memory cache 1st
	if j.GitHubReposMtx != nil {
		j.GitHubReposMtx.RLock()
	}
	repoData, found := j.GitHubRepos[origin]
	if j.GitHubReposMtx != nil {
		j.GitHubReposMtx.RUnlock()
	}
	if found {
		// Printf("repos found in cache: %+v\n", repoData)
		return
	}
	var c *github.Client
	if j.GitHubMtx != nil {
		j.GitHubMtx.RLock()
	}
	c = j.Clients[j.Hint]
	if j.GitHubMtx != nil {
		j.GitHubMtx.RUnlock()
	}
	retry := false
	for {
		var (
			response *github.Response
			rep      *github.Repository
			e        error
		)
		rep, response, e = c.Repositories.Get(j.Context, org, repo)
		// Printf("GET %s/%s -> {%+v, %+v, %+v}\n", org, repo, rep, response, e)
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if j.GitHubReposMtx != nil {
				j.GitHubReposMtx.Lock()
			}
			j.GitHubRepos[origin] = nil
			if j.GitHubReposMtx != nil {
				j.GitHubReposMtx.Unlock()
			}
			if ctx.Debug > 1 {
				Printf("githubRepos: repo not found %s: %v\n", origin, e)
			}
			return
		}
		if e != nil && !retry {
			Printf("Error getting %s repo: response: %+v, error: %+v, retrying rate\n", origin, response, e)
			Printf("githubRepos: handle rate\n")
			abuse := j.isAbuse(e)
			if abuse {
				sleepFor := 10 + rand.Intn(10)
				Printf("GitHub detected abuse (get repo %s), waiting for %ds\n", origin, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse {
				retry = true
			}
			continue
		}
		if e != nil {
			err = e
			return
		}
		jm, _ := jsoniter.Marshal(rep)
		_ = jsoniter.Unmarshal(jm, &repoData)
		// Printf("repos got from API: %+v\n", repoData)
		break
	}
	if j.GitHubReposMtx != nil {
		j.GitHubReposMtx.Lock()
	}
	j.GitHubRepos[origin] = repoData
	if j.GitHubReposMtx != nil {
		j.GitHubReposMtx.Unlock()
	}
	return
}

func (j *DSGitHub) githubUser(ctx *Ctx, login string) (user map[string]interface{}, found bool, err error) {
	// Try memory cache 1st
	if j.GitHubUserMtx != nil {
		j.GitHubUserMtx.RLock()
	}
	user, ok := j.GitHubUser[login]
	if j.GitHubUserMtx != nil {
		j.GitHubUserMtx.RUnlock()
	}
	if ok {
		found = len(user) > 0
		// Printf("user found in memory cache: %+v\n", user)
		return
	}
	// Try file cache 2nd
	// IMPL: make sure EC2test & EC2prod both have j.cacheDir directory created
	path := j.CacheDir + login + ".json"
	lockPath := path + ".lock"
	file, e := os.Stat(path)
	if e == nil {
		for {
			_, e := os.Stat(lockPath)
			if e == nil {
				// Printf("user %s lock file %s present, waitng 1s\n", user, lockPath)
				time.Sleep(time.Duration(1) * time.Second)
				continue
			}
			file, _ = os.Stat(path)
			break
		}
		modified := file.ModTime()
		age := int(time.Now().Sub(modified).Seconds())
		allowedAge := cMaxGitHubUsersFileCacheAge + rand.Intn(cMaxGitHubUsersFileCacheAge)
		if age <= allowedAge {
			bts, e := ioutil.ReadFile(path)
			if e == nil {
				e = jsoniter.Unmarshal(bts, &user)
				bts = nil
				if e == nil {
					found = len(user) > 0
					if found {
						if j.GitHubUserMtx != nil {
							j.GitHubUserMtx.Lock()
						}
						j.GitHubUser[login] = user
						if j.GitHubUserMtx != nil {
							j.GitHubUserMtx.Unlock()
						}
						// Printf("user found in files cache: %+v\n", user)
						return
					}
					Printf("githubUser: unmarshaled %s cache file is empty\n", path)
				}
				Printf("githubUser: cannot unmarshal %s cache file: %v\n", path, e)
			} else {
				Printf("githubUser: cannot read %s user cache file: %v\n", path, e)
			}
		} else {
			Printf("githubUser: %s user cache file is too old: %v (allowed %v)\n", path, time.Duration(age)*time.Second, time.Duration(allowedAge)*time.Second)
		}
	} else {
		if ctx.Debug > 0 {
			// Printf("githubUser: no %s user cache file: %v\n", path, e)
		}
	}
	lockFile, _ := os.Create(lockPath)
	defer func() {
		if lockFile != nil {
			defer func() {
				// Printf("remove lock file %s\n", lockPath)
				_ = os.Remove(lockPath)
			}()
		}
		if err != nil {
			return
		}
		// path := j.CacheDir + login + ".json"
		bts, err := jsoniter.Marshal(user)
		if err != nil {
			Printf("githubUser: cannot marshal user %s to file %s\n", login, path)
			return
		}
		err = ioutil.WriteFile(path, bts, 0644)
		if err != nil {
			Printf("githubUser: cannot write file %s, %d bytes\n", path, len(bts))
			return
		}
		if ctx.Debug > 0 {
			Printf("githubUser: saved %s user file\n", path)
		}
	}()
	// Try GitHub API 3rd
	var c *github.Client
	if j.GitHubMtx != nil {
		j.GitHubMtx.RLock()
	}
	c = j.Clients[j.Hint]
	if j.GitHubMtx != nil {
		j.GitHubMtx.RUnlock()
	}
	retry := false
	for {
		var (
			response *github.Response
			usr      *github.User
			e        error
		)
		usr, response, e = c.Users.Get(j.Context, login)
		// Printf("GET %s -> {%+v, %+v, %+v}\n", login, usr, response, e)
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if j.GitHubUserMtx != nil {
				j.GitHubUserMtx.Lock()
			}
			// Printf("user not found using API: %s\n", login)
			j.GitHubUser[login] = map[string]interface{}{}
			if j.GitHubUserMtx != nil {
				j.GitHubUserMtx.Unlock()
			}
			return
		}
		if e != nil && !retry {
			Printf("Error getting %s user: response: %+v, error: %+v, retrying rate\n", login, response, e)
			Printf("githubUser: handle rate\n")
			abuse := j.isAbuse(e)
			if abuse {
				sleepFor := 10 + rand.Intn(10)
				Printf("GitHub detected abuse (get user %s), waiting for %ds\n", login, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse {
				retry = true
			}
			continue
		}
		if e != nil {
			err = e
			return
		}
		if usr != nil {
			jm, _ := jsoniter.Marshal(usr)
			_ = jsoniter.Unmarshal(jm, &user)
			// Printf("user found using API: %+v\n", user)
			found = true
		}
		break
	}
	if j.GitHubUserMtx != nil {
		j.GitHubUserMtx.Lock()
	}
	j.GitHubUser[login] = user
	if j.GitHubUserMtx != nil {
		j.GitHubUserMtx.Unlock()
	}
	return
}

func (j *DSGitHub) githubIssues(ctx *Ctx, org, repo string, since *time.Time) (issuesData []map[string]interface{}, err error) {
	origin := org + "/" + repo
	// Try memory cache 1st
	if j.GitHubIssuesMtx != nil {
		j.GitHubIssuesMtx.RLock()
	}
	issuesData, found := j.GitHubIssues[origin]
	if j.GitHubIssuesMtx != nil {
		j.GitHubIssuesMtx.RUnlock()
	}
	if found {
		// Printf("issues found in cache: %+v\n", issuesData)
		return
	}
	var c *github.Client
	if j.GitHubMtx != nil {
		j.GitHubMtx.RLock()
	}
	c = j.Clients[j.Hint]
	if j.GitHubMtx != nil {
		j.GitHubMtx.RUnlock()
	}
	opt := &github.IssueListByRepoOptions{
		State:     "all",
		Sort:      "updated",
		Direction: "asc",
	}
	opt.PerPage = 100
	if since != nil {
		opt.Since = *since
	}
	retry := false
	for {
		var (
			response *github.Response
			issues   []*github.Issue
			e        error
		)
		issues, response, e = c.Issues.ListByRepo(j.Context, org, repo, opt)
		// Printf("GET %s/%s -> {%+v, %+v, %+v}\n", org, repo, issues, response, e)
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if j.GitHubIssuesMtx != nil {
				j.GitHubIssuesMtx.Lock()
			}
			j.GitHubIssues[origin] = []map[string]interface{}{}
			if j.GitHubIssuesMtx != nil {
				j.GitHubIssuesMtx.Unlock()
			}
			if ctx.Debug > 1 {
				Printf("githubIssues: issues not found %s: %v\n", origin, e)
			}
			return
		}
		if e != nil && !retry {
			Printf("Error getting %s issues: response: %+v, error: %+v, retrying rate\n", origin, response, e)
			Printf("githubIssues: handle rate\n")
			abuse := j.isAbuse(e)
			if abuse {
				sleepFor := 10 + rand.Intn(10)
				Printf("GitHub detected abuse (get issues %s), waiting for %ds\n", origin, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse {
				retry = true
			}
			continue
		}
		if e != nil {
			err = e
			return
		}
		for _, issue := range issues {
			iss := map[string]interface{}{}
			jm, _ := jsoniter.Marshal(issue)
			_ = jsoniter.Unmarshal(jm, &iss)
			issuesData = append(issuesData, iss)
		}
		if response.NextPage == 0 {
			break
		}
		opt.Page = response.NextPage
		retry = false
		// Printf("issues got from API: %+v\n", issuesData)
	}
	if j.GitHubIssuesMtx != nil {
		j.GitHubIssuesMtx.Lock()
	}
	j.GitHubIssues[origin] = issuesData
	if j.GitHubIssuesMtx != nil {
		j.GitHubIssuesMtx.Unlock()
	}
	return
}

func (j *DSGitHub) githubIssueComments(ctx *Ctx, org, repo string, number int) (comments []map[string]interface{}, err error) {
	key := fmt.Sprintf("%s/%s/%d", org, repo, number)
	// Try memory cache 1st
	if j.GitHubIssueCommentsMtx != nil {
		j.GitHubIssueCommentsMtx.RLock()
	}
	comments, found := j.GitHubIssueComments[key]
	if j.GitHubIssueCommentsMtx != nil {
		j.GitHubIssueCommentsMtx.RUnlock()
	}
	if found {
		// Printf("issue comments found in cache: %+v\n", comments)
		return
	}
	var c *github.Client
	if j.GitHubMtx != nil {
		j.GitHubMtx.RLock()
	}
	c = j.Clients[j.Hint]
	if j.GitHubMtx != nil {
		j.GitHubMtx.RUnlock()
	}
	opt := &github.IssueListCommentsOptions{}
	opt.PerPage = 100
	retry := false
	for {
		var (
			response *github.Response
			comms    []*github.IssueComment
			e        error
		)
		comms, response, e = c.Issues.ListComments(j.Context, org, repo, number, opt)
		// Printf("GET %s/%s -> {%+v, %+v, %+v}\n", org, repo, comms, response, e)
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if j.GitHubIssueCommentsMtx != nil {
				j.GitHubIssueCommentsMtx.Lock()
			}
			j.GitHubIssueComments[key] = []map[string]interface{}{}
			if j.GitHubIssueCommentsMtx != nil {
				j.GitHubIssueCommentsMtx.Unlock()
			}
			if ctx.Debug > 1 {
				Printf("githubIssueComments: comments not found %s: %v\n", key, e)
			}
			return
		}
		if e != nil && !retry {
			Printf("Error getting %s issue comments: response: %+v, error: %+v, retrying rate\n", key, response, e)
			Printf("githubIssueComments: handle rate\n")
			abuse := j.isAbuse(e)
			if abuse {
				sleepFor := 10 + rand.Intn(10)
				Printf("GitHub detected abuse (get issue comments %s), waiting for %ds\n", key, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse {
				retry = true
			}
			continue
		}
		if e != nil {
			err = e
			return
		}
		for _, comment := range comms {
			com := map[string]interface{}{}
			jm, _ := jsoniter.Marshal(comment)
			_ = jsoniter.Unmarshal(jm, &com)
			userLogin, ok := Dig(com, []string{"user", "login"}, false, true)
			if ok {
				com["user_data"], _, err = j.githubUser(ctx, userLogin.(string))
				if err != nil {
					return
				}
			}
			iCnt, ok := Dig(com, []string{"reactions", "total_count"}, false, true)
			if ok {
				com["reactions_data"] = []interface{}{}
				cnt := int(iCnt.(float64))
				if cnt > 0 {
					cid, ok := Dig(com, []string{"id"}, false, true)
					if ok {
						com["reactions_data"], err = j.githubCommentReactions(ctx, org, repo, int64(cid.(float64)))
						if err != nil {
							return
						}
					}
				}
			}
			comments = append(comments, com)
		}
		if response.NextPage == 0 {
			break
		}
		opt.Page = response.NextPage
		retry = false
		// Printf("issue comments got from API: %+v\n", comments)
	}
	if j.GitHubIssueCommentsMtx != nil {
		j.GitHubIssueCommentsMtx.Lock()
	}
	j.GitHubIssueComments[key] = comments
	if j.GitHubIssueCommentsMtx != nil {
		j.GitHubIssueCommentsMtx.Unlock()
	}
	return
}

func (j *DSGitHub) githubCommentReactions(ctx *Ctx, org, repo string, cid int64) (reactions []map[string]interface{}, err error) {
	key := fmt.Sprintf("%s/%s/%d", org, repo, cid)
	// fmt.Printf("githubCommentReactions %s\n", key)
	// Try memory cache 1st
	if j.GitHubCommentReactionsMtx != nil {
		j.GitHubCommentReactionsMtx.RLock()
	}
	reactions, found := j.GitHubCommentReactions[key]
	if j.GitHubCommentReactionsMtx != nil {
		j.GitHubCommentReactionsMtx.RUnlock()
	}
	if found {
		// Printf("comment reactions found in cache: %+v\n", reactions)
		return
	}
	var c *github.Client
	if j.GitHubMtx != nil {
		j.GitHubMtx.RLock()
	}
	c = j.Clients[j.Hint]
	if j.GitHubMtx != nil {
		j.GitHubMtx.RUnlock()
	}
	opt := &github.ListOptions{}
	opt.PerPage = 100
	retry := false
	for {
		var (
			response *github.Response
			reacts   []*github.Reaction
			e        error
		)
		reacts, response, e = c.Reactions.ListIssueCommentReactions(j.Context, org, repo, cid, opt)
		// Printf("GET %s/%s/%d -> {%+v, %+v, %+v}\n", org, repo, cid, reacts, response, e)
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if j.GitHubCommentReactionsMtx != nil {
				j.GitHubCommentReactionsMtx.Lock()
			}
			j.GitHubCommentReactions[key] = []map[string]interface{}{}
			if j.GitHubCommentReactionsMtx != nil {
				j.GitHubCommentReactionsMtx.Unlock()
			}
			if ctx.Debug > 1 {
				Printf("githubCommentreactions: reactions not found %s: %v\n", key, e)
			}
			return
		}
		if e != nil && !retry {
			Printf("Error getting %s comment reactions: response: %+v, error: %+v, retrying rate\n", key, response, e)
			Printf("githubCommentreactions: handle rate\n")
			abuse := j.isAbuse(e)
			if abuse {
				sleepFor := 10 + rand.Intn(10)
				Printf("GitHub detected abuse (get comment reactions %s), waiting for %ds\n", key, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse {
				retry = true
			}
			continue
		}
		if e != nil {
			err = e
			return
		}
		for _, reaction := range reacts {
			react := map[string]interface{}{}
			jm, _ := jsoniter.Marshal(reaction)
			_ = jsoniter.Unmarshal(jm, &react)
			userLogin, ok := Dig(react, []string{"user", "login"}, false, true)
			if ok {
				react["user_data"], _, err = j.githubUser(ctx, userLogin.(string))
				if err != nil {
					return
				}
			}
			reactions = append(reactions, react)
		}
		if response.NextPage == 0 {
			break
		}
		opt.Page = response.NextPage
		retry = false
		// Printf("comment reactions got from API: %+v\n", reactions)
	}
	if j.GitHubCommentReactionsMtx != nil {
		j.GitHubCommentReactionsMtx.Lock()
	}
	j.GitHubCommentReactions[key] = reactions
	if j.GitHubCommentReactionsMtx != nil {
		j.GitHubCommentReactionsMtx.Unlock()
	}
	return
}

// ParseArgs - parse GitHub specific environment variables
func (j *DSGitHub) ParseArgs(ctx *Ctx) (err error) {
	j.DS = GitHub
	prefix := "DA_GITHUB_"
	j.Org = os.Getenv(prefix + "ORG")
	j.Repo = os.Getenv(prefix + "REPO")
	j.Category = os.Getenv(prefix + "CATEGORY")
	j.Tokens = os.Getenv(prefix + "TOKENS")
	return
}

// Validate - is current DS configuration OK?
func (j *DSGitHub) Validate(ctx *Ctx) (err error) {
	j.Org = strings.TrimSpace(j.Org)
	if j.Org == "" {
		err = fmt.Errorf("github org must be set")
		return
	}
	j.Repo = strings.TrimSpace(j.Repo)
	if strings.Contains(j.Repo, ".git") {
		j.Repo = strings.Replace(j.Repo, ".git", "", -1)
	}
	if j.Repo == "" {
		err = fmt.Errorf("github repo must be set")
		return
	}
	j.Category = strings.TrimSpace(j.Category)
	if j.Category == "" {
		err = fmt.Errorf("github category must be set")
		return
	}
	j.URL = "https://github.com/" + j.Org + "/" + j.Repo
	defer func() {
		Printf("configured %d GitHub OAuth clients\n", len(j.Clients))
	}()
	j.Tokens = strings.TrimSpace(j.Tokens)
	// Get GitHub OAuth from env or from file
	oAuth := j.Tokens
	if strings.Contains(oAuth, "/") {
		bytes, err := ioutil.ReadFile(oAuth)
		FatalOnError(err)
		oAuth = strings.TrimSpace(string(bytes))
	}
	// GitHub authentication or use public access
	j.Context = context.Background()
	if oAuth == "" {
		client := github.NewClient(nil)
		j.Clients = append(j.Clients, client)
	} else {
		oAuths := strings.Split(oAuth, ",")
		for _, auth := range oAuths {
			j.OAuthKeys = append(j.OAuthKeys, auth)
			ts := oauth2.StaticTokenSource(
				&oauth2.Token{AccessToken: auth},
			)
			tc := oauth2.NewClient(j.Context, ts)
			client := github.NewClient(tc)
			j.Clients = append(j.Clients, client)
		}
	}
	j.GitHubRepos = make(map[string]map[string]interface{})
	j.GitHubIssues = make(map[string][]map[string]interface{})
	j.GitHubUser = make(map[string]map[string]interface{})
	j.GitHubIssueComments = make(map[string][]map[string]interface{})
	j.GitHubCommentReactions = make(map[string][]map[string]interface{})
	j.ThrN = GetThreadsNum(ctx)
	if j.ThrN > 1 {
		j.GitHubMtx = &sync.RWMutex{}
		j.GitHubReposMtx = &sync.RWMutex{}
		j.GitHubIssuesMtx = &sync.RWMutex{}
		j.GitHubUserMtx = &sync.RWMutex{}
		j.GitHubIssueCommentsMtx = &sync.RWMutex{}
		j.GitHubCommentReactionsMtx = &sync.RWMutex{}
	}
	j.Hint, _ = j.handleRate(ctx)
	j.CacheDir = os.Getenv("HOME") + "/.perceval/github-users-cache/"
	_ = os.MkdirAll(j.CacheDir, 0777)
	return
}

// Name - return data source name
func (j *DSGitHub) Name() string {
	return j.DS
}

// Info - return DS configuration in a human readable form
func (j DSGitHub) Info() string {
	return fmt.Sprintf("%+v", j)
}

// CustomFetchRaw - is this datasource using custom fetch raw implementation?
func (j *DSGitHub) CustomFetchRaw() bool {
	return false
}

// FetchRaw - implement fetch raw data for GitHub datasource
func (j *DSGitHub) FetchRaw(ctx *Ctx) (err error) {
	Printf("%s should use generic FetchRaw()\n", j.DS)
	return
}

// CustomEnrich - is this datasource using custom enrich implementation?
func (j *DSGitHub) CustomEnrich() bool {
	return false
}

// Enrich - implement enrich data for GitHub datasource
func (j *DSGitHub) Enrich(ctx *Ctx) (err error) {
	Printf("%s should use generic Enrich()\n", j.DS)
	return
}

// FetchItems - implement raw data for GitHub datasource
func (j *DSGitHub) FetchItems(ctx *Ctx) (err error) {
	switch j.Category {
	case "repository":
		return j.FetchItemsRepository(ctx)
	case "issue":
		return j.FetchItemsIssue(ctx)
	case "pull_request":
		return j.FetchItemsIssue(ctx)
	default:
		err = fmt.Errorf("FetchItems: unknown category %s", j.Category)
	}
	return
}

// FetchItemsRepository - implement raw repository data for GitHub datasource
func (j *DSGitHub) FetchItemsRepository(ctx *Ctx) (err error) {
	items := []interface{}{}
	item, err := j.githubRepos(ctx, j.Org, j.Repo)
	FatalOnError(err)
	item["fetched_on"] = fmt.Sprintf("%.6f", float64(time.Now().UnixNano())/1.0e9)
	esItem := j.AddMetadata(ctx, item)
	if ctx.Project != "" {
		item["project"] = ctx.Project
	}
	esItem["data"] = item
	items = append(items, esItem)
	err = SendToElastic(ctx, j, true, UUID, items)
	if err != nil {
		Printf("Error %v sending %d messages to ES\n", err, len(items))
	}
	return
}

// ProcessIssue - add issues sub items
func (j *DSGitHub) ProcessIssue(ctx *Ctx, inIssue map[string]interface{}) (issue map[string]interface{}, err error) {
	issue = inIssue
	issue["user_data"] = map[string]interface{}{}
	issue["assignee_data"] = map[string]interface{}{}
	issue["assignees_data"] = []interface{}{}
	issue["comments_data"] = []interface{}{}
	issue["reactions_data"] = []interface{}{}
	// ['user', 'assignee', 'assignees', 'comments', 'reactions']
	userLogin, ok := Dig(issue, []string{"user", "login"}, false, true)
	if ok {
		issue["user_data"], _, err = j.githubUser(ctx, userLogin.(string))
		if err != nil {
			return
		}
	}
	assigneeLogin, ok := Dig(issue, []string{"assignee", "login"}, false, true)
	if ok {
		issue["assignee_data"], _, err = j.githubUser(ctx, assigneeLogin.(string))
		if err != nil {
			return
		}
	}
	iAssignees, ok := Dig(issue, []string{"assignees"}, false, true)
	if ok {
		assignees, _ := iAssignees.([]interface{})
		assigneesAry := []map[string]interface{}{}
		for _, assignee := range assignees {
			aLogin, ok := Dig(assignee, []string{"login"}, false, true)
			if ok {
				assigneeData, _, e := j.githubUser(ctx, aLogin.(string))
				if e != nil {
					err = e
					return
				}
				assigneesAry = append(assigneesAry, assigneeData)
			}
		}
		issue["assignees_data"] = assigneesAry
	}
	number, ok := Dig(issue, []string{"number"}, false, true)
	if ok {
		issue["comments_data"], err = j.githubIssueComments(ctx, j.Org, j.Repo, int(number.(float64)))
		if err != nil {
			return
		}
	}
	// xxx
	return
}

// FetchItemsIssue - implement raw issue data for GitHub datasource
func (j *DSGitHub) FetchItemsIssue(ctx *Ctx) (err error) {
	// Process issues (possibly in threads)
	var (
		ch           chan error
		allIssues    []interface{}
		allIssuesMtx *sync.Mutex
		escha        []chan error
		eschaMtx     *sync.Mutex
	)
	if j.ThrN > 1 {
		ch = make(chan error)
		allIssuesMtx = &sync.Mutex{}
		eschaMtx = &sync.Mutex{}
	}
	nThreads := 0
	processIssue := func(c chan error, issue map[string]interface{}) (wch chan error, e error) {
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		item, err := j.ProcessIssue(ctx, issue)
		FatalOnError(err)
		esItem := j.AddMetadata(ctx, item)
		if ctx.Project != "" {
			item["project"] = ctx.Project
		}
		esItem["data"] = item
		if allIssuesMtx != nil {
			allIssuesMtx.Lock()
		}
		allIssues = append(allIssues, esItem)
		nIssues := len(allIssues)
		if nIssues >= ctx.ESBulkSize {
			sendToElastic := func(c chan error) (ee error) {
				defer func() {
					if c != nil {
						c <- ee
					}
				}()
				ee = SendToElastic(ctx, j, true, UUID, allIssues)
				if ee != nil {
					Printf("error %v sending %d issues to ElasticSearch\n", ee, len(allIssues))
				}
				allIssues = []interface{}{}
				if allIssuesMtx != nil {
					allIssuesMtx.Unlock()
				}
				return
			}
			if j.ThrN > 1 {
				wch = make(chan error)
				go func() {
					_ = sendToElastic(wch)
				}()
			} else {
				e = sendToElastic(nil)
				if e != nil {
					return
				}
			}
		} else {
			if allIssuesMtx != nil {
				allIssuesMtx.Unlock()
			}
		}
		return
	}
	issues, err := j.githubIssues(ctx, j.Org, j.Repo, ctx.DateFrom)
	FatalOnError(err)
	Printf("got %d issues\n", len(issues))
	if j.ThrN > 1 {
		for _, issue := range issues {
			go func(iss map[string]interface{}) {
				var (
					e    error
					esch chan error
				)
				esch, e = processIssue(ch, iss)
				if e != nil {
					Printf("process error: %v\n", e)
					return
				}
				if esch != nil {
					if eschaMtx != nil {
						eschaMtx.Lock()
					}
					escha = append(escha, esch)
					if eschaMtx != nil {
						eschaMtx.Unlock()
					}
				}
			}(issue)
			nThreads++
			if nThreads == j.ThrN {
				err = <-ch
				if err != nil {
					return
				}
				nThreads--
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				return
			}
		}
	} else {
		for _, issue := range issues {
			_, err = processIssue(nil, issue)
			if err != nil {
				return
			}
		}
	}
	for _, esch := range escha {
		err = <-esch
		if err != nil {
			return
		}
	}
	nIssues := len(allIssues)
	if ctx.Debug > 0 {
		Printf("%d remaining issues to send to ES\n", nIssues)
	}
	if nIssues > 0 {
		err = SendToElastic(ctx, j, true, UUID, allIssues)
		if err != nil {
			Printf("Error %v sending %d issues to ES\n", err, len(allIssues))
		}
	}
	return
}

// FetchItemsPullRequest - implement raw issue data for GitHub datasource
func (j *DSGitHub) FetchItemsPullRequest(ctx *Ctx) (err error) {
	// IMPL:
	var messages [][]byte
	// Process messages (possibly in threads)
	var (
		ch         chan error
		allMsgs    []interface{}
		allMsgsMtx *sync.Mutex
		escha      []chan error
		eschaMtx   *sync.Mutex
	)
	thrN := GetThreadsNum(ctx)
	if thrN > 1 {
		ch = make(chan error)
		allMsgsMtx = &sync.Mutex{}
		eschaMtx = &sync.Mutex{}
	}
	nThreads := 0
	processMsg := func(c chan error, msg []byte) (wch chan error, e error) {
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		// FIXME: Real data processing here
		item := map[string]interface{}{"id": time.Now().UnixNano(), "name": "xyz"}
		esItem := j.AddMetadata(ctx, item)
		if ctx.Project != "" {
			item["project"] = ctx.Project
		}
		esItem["data"] = item
		if allMsgsMtx != nil {
			allMsgsMtx.Lock()
		}
		allMsgs = append(allMsgs, esItem)
		nMsgs := len(allMsgs)
		if nMsgs >= ctx.ESBulkSize {
			sendToElastic := func(c chan error) (ee error) {
				defer func() {
					if c != nil {
						c <- ee
					}
				}()
				ee = SendToElastic(ctx, j, true, UUID, allMsgs)
				if ee != nil {
					Printf("error %v sending %d messages to ElasticSearch\n", ee, len(allMsgs))
				}
				allMsgs = []interface{}{}
				if allMsgsMtx != nil {
					allMsgsMtx.Unlock()
				}
				return
			}
			if thrN > 1 {
				wch = make(chan error)
				go func() {
					_ = sendToElastic(wch)
				}()
			} else {
				e = sendToElastic(nil)
				if e != nil {
					return
				}
			}
		} else {
			if allMsgsMtx != nil {
				allMsgsMtx.Unlock()
			}
		}
		return
	}
	if thrN > 1 {
		for _, message := range messages {
			go func(msg []byte) {
				var (
					e    error
					esch chan error
				)
				esch, e = processMsg(ch, msg)
				if e != nil {
					Printf("process error: %v\n", e)
					return
				}
				if esch != nil {
					if eschaMtx != nil {
						eschaMtx.Lock()
					}
					escha = append(escha, esch)
					if eschaMtx != nil {
						eschaMtx.Unlock()
					}
				}
			}(message)
			nThreads++
			if nThreads == thrN {
				err = <-ch
				if err != nil {
					return
				}
				nThreads--
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				return
			}
		}
	} else {
		for _, message := range messages {
			_, err = processMsg(nil, message)
			if err != nil {
				return
			}
		}
	}
	for _, esch := range escha {
		err = <-esch
		if err != nil {
			return
		}
	}
	nMsgs := len(allMsgs)
	if ctx.Debug > 0 {
		Printf("%d remaining messages to send to ES\n", nMsgs)
	}
	if nMsgs > 0 {
		err = SendToElastic(ctx, j, true, UUID, allMsgs)
		if err != nil {
			Printf("Error %v sending %d messages to ES\n", err, len(allMsgs))
		}
	}
	return
}

// SupportDateFrom - does DS support resuming from date?
func (j *DSGitHub) SupportDateFrom() bool {
	return true
}

// SupportOffsetFrom - does DS support resuming from offset?
func (j *DSGitHub) SupportOffsetFrom() bool {
	return false
}

// DateField - return date field used to detect where to restart from
func (j *DSGitHub) DateField(*Ctx) string {
	return DefaultDateField // metadata__updated_on
}

// RichIDField - return rich ID field name
func (j *DSGitHub) RichIDField(*Ctx) string {
	return UUID
}

// RichAuthorField - return rich author field name
func (j *DSGitHub) RichAuthorField(*Ctx) string {
	return DefaultAuthorField
}

// OffsetField - return offset field used to detect where to restart from
func (j *DSGitHub) OffsetField(*Ctx) string {
	return DefaultOffsetField
}

// OriginField - return origin field used to detect where to restart from
func (j *DSGitHub) OriginField(ctx *Ctx) string {
	if ctx.Tag != "" {
		return DefaultTagField
	}
	return DefaultOriginField
}

// Categories - return a set of configured categories
func (j *DSGitHub) Categories() map[string]struct{} {
	return GitHubCategories
}

// ResumeNeedsOrigin - is origin field needed when resuming
// Origin should be needed when multiple configurations save to the same index
func (j *DSGitHub) ResumeNeedsOrigin(ctx *Ctx, raw bool) bool {
	return true
}

// ResumeNeedsCategory - is category field needed when resuming
// Category should be needed when multiple types of categories save to the same index
// or there are multiple types of documents within the same category
func (j *DSGitHub) ResumeNeedsCategory(ctx *Ctx, raw bool) bool {
	return j.Category != "repository"
}

// Origin - return current origin
func (j *DSGitHub) Origin(ctx *Ctx) string {
	return j.URL
}

// ItemID - return unique identifier for an item
func (j *DSGitHub) ItemID(item interface{}) string {
	if j.Category == "repository" {
		id, ok := item.(map[string]interface{})["fetched_on"]
		if !ok {
			Fatalf("%s: ItemID() - cannot extract fetched_on from %+v", j.DS, DumpKeys(item))
		}
		return fmt.Sprintf("%v", id)
	}
	// IMPL:
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// AddMetadata - add metadata to the item
func (j *DSGitHub) AddMetadata(ctx *Ctx, item interface{}) (mItem map[string]interface{}) {
	// IMPL:
	mItem = make(map[string]interface{})
	origin := j.URL
	tag := ctx.Tag
	if tag == "" {
		tag = origin
	}
	itemID := j.ItemID(item)
	updatedOn := j.ItemUpdatedOn(item)
	uuid := UUIDNonEmpty(ctx, origin, itemID)
	timestamp := time.Now()
	mItem["backend_name"] = GitHub
	mItem["backend_version"] = GitHubBackendVersion
	mItem["timestamp"] = fmt.Sprintf("%.06f", float64(timestamp.UnixNano())/1.0e9)
	mItem[UUID] = uuid
	mItem[DefaultOriginField] = origin
	mItem[DefaultTagField] = tag
	mItem[DefaultOffsetField] = float64(updatedOn.Unix())
	mItem["category"] = j.ItemCategory(item)
	mItem["search_fields"] = make(map[string]interface{})
	FatalOnError(DeepSet(mItem, []string{"search_fields", "owner"}, j.Org, false))
	FatalOnError(DeepSet(mItem, []string{"search_fields", "repo"}, j.Repo, false))
	//mItem["search_fields"] = j.GenSearchFields(ctx, issue, uuid)
	//mItem["search_fields"] = make(map[string]interface{})
	mItem[DefaultDateField] = ToESDate(updatedOn)
	mItem[DefaultTimestampField] = ToESDate(timestamp)
	mItem[ProjectSlug] = ctx.ProjectSlug
	return
}

// ItemUpdatedOn - return updated on date for an item
func (j *DSGitHub) ItemUpdatedOn(item interface{}) time.Time {
	if j.Category == "repository" {
		epochNS, ok := item.(map[string]interface{})["fetched_on"].(float64)
		if ok {
			epochNS *= 1.0e9
			return time.Unix(0, int64(epochNS))
		}
		epochS, ok := item.(map[string]interface{})["fetched_on"].(string)
		if !ok {
			Fatalf("%s: ItemUpdatedOn() - cannot extract fetched_on from %+v", j.DS, DumpKeys(item))
		}
		epochNS, err := strconv.ParseFloat(epochS, 64)
		FatalOnError(err)
		epochNS *= 1.0e9
		return time.Unix(0, int64(epochNS))
	}
	iWhen, _ := Dig(item, []string{"updated_at"}, true, false)
	when, err := TimeParseInterfaceString(iWhen)
	FatalOnError(err)
	return when
}

// ItemCategory - return unique identifier for an item
func (j *DSGitHub) ItemCategory(item interface{}) string {
	return j.Category
}

// ElasticRawMapping - Raw index mapping definition
func (j *DSGitHub) ElasticRawMapping() []byte {
	return GitHubRawMapping
}

// ElasticRichMapping - Rich index mapping definition
func (j *DSGitHub) ElasticRichMapping() []byte {
	return GitHubRichMapping
}

// GetItemIdentities return list of item's identities, each one is [3]string
// (name, username, email) tripples, special value Nil "none" means null
// we use string and not *string which allows nil to allow usage as a map key
func (j *DSGitHub) GetItemIdentities(ctx *Ctx, doc interface{}) (map[[3]string]struct{}, error) {
	if j.Category == "repository" {
		return map[[3]string]struct{}{}, nil
	}
	// IMPL:
	return map[[3]string]struct{}{}, nil
}

// GitHubEnrichItemsFunc - iterate items and enrich them
// items is a current pack of input items
// docs is a pointer to where extracted identities will be stored
func GitHubEnrichItemsFunc(ctx *Ctx, ds DS, thrN int, items []interface{}, docs *[]interface{}) (err error) {
	j, _ := ds.(*DSGitHub)
	switch j.Category {
	case "repository":
		return j.GitHubRepositoryEnrichItemsFunc(ctx, thrN, items, docs)
	default:
		err = fmt.Errorf("GitHubEnrichItemsFunc: unknown category %s", j.Category)
	}
	return
}

// GitHubRepositoryEnrichItemsFunc - iterate items and enrich them
// items is a current pack of input items
// docs is a pointer to where extracted identities will be stored
func (j *DSGitHub) GitHubRepositoryEnrichItemsFunc(ctx *Ctx, thrN int, items []interface{}, docs *[]interface{}) (err error) {
	if ctx.Debug > 0 {
		Printf("github enrich repository items %d/%d func\n", len(items), len(*docs))
	}
	var (
		mtx *sync.RWMutex
		ch  chan error
	)
	if thrN > 1 {
		mtx = &sync.RWMutex{}
		ch = make(chan error)
	}
	nThreads := 0
	procItem := func(c chan error, idx int) (e error) {
		if thrN > 1 {
			mtx.RLock()
		}
		item := items[idx]
		if thrN > 1 {
			mtx.RUnlock()
		}
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		src, ok := item.(map[string]interface{})["_source"]
		if !ok {
			e = fmt.Errorf("Missing _source in item %+v", DumpKeys(item))
			return
		}
		doc, ok := src.(map[string]interface{})
		if !ok {
			e = fmt.Errorf("Failed to parse document %+v", doc)
			return
		}
		var rich map[string]interface{}
		rich, e = j.EnrichItem(ctx, doc, "", false, nil)
		if e != nil {
			return
		}
		e = EnrichItem(ctx, j, rich)
		if e != nil {
			return
		}
		if thrN > 1 {
			mtx.Lock()
		}
		*docs = append(*docs, rich)
		if thrN > 1 {
			mtx.Unlock()
		}
		return
	}
	if thrN > 1 {
		for i := range items {
			go func(i int) {
				_ = procItem(ch, i)
			}(i)
			nThreads++
			if nThreads == thrN {
				err = <-ch
				if err != nil {
					return
				}
				nThreads--
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				return
			}
		}
		return
	}
	for i := range items {
		err = procItem(nil, i)
		if err != nil {
			return
		}
	}
	return
}

// GitHubEnrichIssueItemsFunc - iterate items and enrich them
// items is a current pack of input items
// docs is a pointer to where extracted identities will be stored
func (j *DSGitHub) GitHubEnrichIssueItemsFunc(ctx *Ctx, thrN int, items []interface{}, docs *[]interface{}) (err error) {
	// IMPL:
	if ctx.Debug > 0 {
		Printf("github enrich issue items %d/%d func\n", len(items), len(*docs))
	}
	var (
		mtx *sync.RWMutex
		ch  chan error
	)
	if thrN > 1 {
		mtx = &sync.RWMutex{}
		ch = make(chan error)
	}
	dbConfigured := ctx.AffsDBConfigured()
	nThreads := 0
	procItem := func(c chan error, idx int) (e error) {
		if thrN > 1 {
			mtx.RLock()
		}
		item := items[idx]
		if thrN > 1 {
			mtx.RUnlock()
		}
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		src, ok := item.(map[string]interface{})["_source"]
		if !ok {
			e = fmt.Errorf("Missing _source in item %+v", DumpKeys(item))
			return
		}
		doc, ok := src.(map[string]interface{})
		if !ok {
			e = fmt.Errorf("Failed to parse document %+v", doc)
			return
		}
		if 1 == 0 {
			Printf("%v\n", dbConfigured)
		}
		// Actual item enrichment
		/*
			    var rich map[string]interface{}
					if thrN > 1 {
						mtx.Lock()
					}
					*docs = append(*docs, rich)
					if thrN > 1 {
						mtx.Unlock()
					}
		*/
		return
	}
	if thrN > 1 {
		for i := range items {
			go func(i int) {
				_ = procItem(ch, i)
			}(i)
			nThreads++
			if nThreads == thrN {
				err = <-ch
				if err != nil {
					return
				}
				nThreads--
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				return
			}
		}
		return
	}
	for i := range items {
		err = procItem(nil, i)
		if err != nil {
			return
		}
	}
	return
}

// EnrichItems - perform the enrichment
func (j *DSGitHub) EnrichItems(ctx *Ctx) (err error) {
	Printf("enriching items\n")
	err = ForEachESItem(ctx, j, true, ESBulkUploadFunc, GitHubEnrichItemsFunc, nil, true)
	return
}

// EnrichItem - return rich item from raw item for a given author type
func (j *DSGitHub) EnrichItem(ctx *Ctx, item map[string]interface{}, author string, affs bool, extra interface{}) (rich map[string]interface{}, err error) {
	switch j.Category {
	case "repository":
		return j.EnrichRepositoryItem(ctx, item, author, affs, extra)
	default:
		err = fmt.Errorf("EnrichItem: unknown category %s", j.Category)
	}
	return
}

// EnrichRepositoryItem - return rich item from raw item for a given author type
func (j *DSGitHub) EnrichRepositoryItem(ctx *Ctx, item map[string]interface{}, author string, affs bool, extra interface{}) (rich map[string]interface{}, err error) {
	rich = make(map[string]interface{})
	repo, ok := item["data"].(map[string]interface{})
	if !ok {
		err = fmt.Errorf("missing data field in item %+v", DumpKeys(item))
		return
	}
	for _, field := range RawFields {
		v, _ := item[field]
		rich[field] = v
	}
	repoFields := []string{"forks_count", "subscribers_count", "stargazers_count", "fetched_on"}
	for _, field := range repoFields {
		v, _ := repo[field]
		rich[field] = v
	}
	v, _ := repo["html_url"]
	rich["url"] = v
	rich["repo_name"] = j.URL
	updatedOn, _ := Dig(item, []string{j.DateField(ctx)}, true, false)
	for prop, value := range CommonFields(j, updatedOn, "repository") {
		rich[prop] = value
	}
	rich["type"] = "repository"
	return
}

// AffsItems - return affiliations data items for given roles and date
func (j *DSGitHub) AffsItems(ctx *Ctx, rawItem map[string]interface{}, roles []string, date interface{}) (affsItems map[string]interface{}, err error) {
	// IMPL:
	return
}

// GetRoleIdentity - return identity data for a given role
func (j *DSGitHub) GetRoleIdentity(ctx *Ctx, item map[string]interface{}, role string) map[string]interface{} {
	// IMPL:
	return map[string]interface{}{"name": nil, "username": nil, "email": nil}
}

// AllRoles - return all roles defined for the backend
// roles can be static (always the same) or dynamic (per item)
// second return parameter is static mode (true/false)
// dynamic roles will use item to get its roles
func (j *DSGitHub) AllRoles(ctx *Ctx, item map[string]interface{}) ([]string, bool) {
	if j.Category == "repository" {
		return []string{}, false
	}
	// IMPL:
	return []string{Author}, true
}

// CalculateTimeToReset - calculate time to reset rate limits based on rate limit value and rate limit reset value
func (j *DSGitHub) CalculateTimeToReset(ctx *Ctx, rateLimit, rateLimitReset int) (seconds int) {
	seconds = rateLimitReset
	return
}

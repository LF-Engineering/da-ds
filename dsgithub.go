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
	// MaxGitHubUsersFileCacheAge 90 days (in seconds) - file is considered too old anywhere between 90-180 days
	MaxGitHubUsersFileCacheAge = 7776000
	// MaxCommentBodyLength - max comment body length
	MaxCommentBodyLength = 4096
	// MaxIssueBodyLength - max issue body length
	MaxIssueBodyLength = 4096
	// MaxPullBodyLength - max pull request body length
	MaxPullBodyLength = 4096
	// MaxReviewBodyLength - max review body length
	MaxReviewBodyLength = 4096
	// MaxReviewCommentBodyLength - max review comment body length
	MaxReviewCommentBodyLength = 4096
	// ItemsPerPage - how many items in a page
	ItemsPerPage = 100
	// CacheGitHubRepo - cache this?
	CacheGitHubRepo = true
	// CacheGitHubIssues - cache this?
	CacheGitHubIssues = false
	// CacheGitHubUser - cache this?
	CacheGitHubUser = true
	// CacheGitHubUserFiles - cache this in files?
	CacheGitHubUserFiles = true
	// CacheGitHubIssueComments - cache this?
	CacheGitHubIssueComments = false
	// CacheGitHubCommentReactions - cache this?
	CacheGitHubCommentReactions = false
	// CacheGitHubIssueReactions - cache this?
	CacheGitHubIssueReactions = false
	// CacheGitHubPull - cache this?
	CacheGitHubPull = false
	// CacheGitHubPulls - cache this?
	CacheGitHubPulls = false
	// CacheGitHubPullReviews - cache this?
	CacheGitHubPullReviews = false
	// CacheGitHubPullReviewComments - cache this?
	CacheGitHubPullReviewComments = false
	// CacheGitHubReviewCommentReactions - cache this?
	CacheGitHubReviewCommentReactions = false
	// CacheGitHubPullRequestedReviewers - cache this?
	CacheGitHubPullRequestedReviewers = false
	// CacheGitHubPullCommits - cache this?
	CacheGitHubPullCommits = false
	// CacheGitHubUserOrgs - cache this?
	CacheGitHubUserOrgs = true
)

var (
	// GitHubRawMapping - GitHub raw index mapping
	// GitHubRawMapping = []byte(`{"properties":{"metadata__updated_on":{"type":"date"}}}`)
	GitHubRawMapping = []byte(`{"dynamic":true,"properties":{"metadata__updated_on":{"type":"date","format":"strict_date_optional_time||epoch_millis"},"data":{"properties":{"comments_data":{"dynamic":false,"properties":{"body":{"type":"text","index":true}}},"review_comments_data":{"dynamic":false,"properties":{"body":{"type":"text","index":true},"diff_hunk":{"type":"text","index":true}}},"reviews_data":{"dynamic":false,"properties":{"body":{"type":"text","index":true}}},"body":{"type":"text","index":true}}}}}`)
	// GitHubRichMapping - GitHub rich index mapping
	// GitHubRichMapping = []byte(`{"properties":{"metadata__updated_on":{"type":"date"},"merge_author_geolocation":{"type":"geo_point"},"assignee_geolocation":{"type":"geo_point"},"state":{"type":"keyword"},"user_geolocation":{"type":"geo_point"},"title_analyzed":{"type":"text","index":true}}}`)
	GitHubRichMapping = []byte(`{"dynamic":true,"properties":{"metadata__updated_on":{"type":"date","format":"strict_date_optional_time||epoch_millis"},"merge_author_geolocation":{"type":"geo_point"},"assignee_geolocation":{"type":"geo_point"},"state":{"type":"keyword"},"user_geolocation":{"type":"geo_point"},"title_analyzed":{"type":"text","index":true}},"dynamic_templates":[{"notanalyzed":{"match":"*","unmatch":"body","match_mapping_type":"string","mapping":{"type":"keyword"}}},{"formatdate":{"match":"*","match_mapping_type":"date","mapping":{"format":"strict_date_optional_time||epoch_millis","type":"date"}}}]}`)
	// GitHubCategories - categories defined for GitHub
	GitHubCategories = map[string]struct{}{"issue": {}, "pull_request": {}, "repository": {}}
)

// DSGitHub - DS implementation for GitHub
type DSGitHub struct {
	DS                              string // From DA_DS - data source type "github"
	Org                             string // From DA_GITHUB_ORG - github org
	Repo                            string // From DA_GITHUB_REPO - github repo
	Category                        string // From DA_GITHUB_CATEGORY - issue, pull_request, repository
	Tokens                          string // From DA_GITHUB_TOKENS - "," separated list of OAuth tokens
	URL                             string
	Clients                         []*github.Client
	Context                         context.Context
	OAuthKeys                       []string
	ThrN                            int
	Hint                            int
	CacheDir                        string
	GitHubMtx                       *sync.RWMutex
	GitHubRepoMtx                   *sync.RWMutex
	GitHubIssuesMtx                 *sync.RWMutex
	GitHubUserMtx                   *sync.RWMutex
	GitHubIssueCommentsMtx          *sync.RWMutex
	GitHubCommentReactionsMtx       *sync.RWMutex
	GitHubIssueReactionsMtx         *sync.RWMutex
	GitHubPullMtx                   *sync.RWMutex
	GitHubPullsMtx                  *sync.RWMutex
	GitHubPullReviewsMtx            *sync.RWMutex
	GitHubPullReviewCommentsMtx     *sync.RWMutex
	GitHubReviewCommentReactionsMtx *sync.RWMutex
	GitHubPullRequestedReviewersMtx *sync.RWMutex
	GitHubPullCommitsMtx            *sync.RWMutex
	GitHubUserOrgsMtx               *sync.RWMutex
	GitHubRepo                      map[string]map[string]interface{}
	GitHubIssues                    map[string][]map[string]interface{}
	GitHubUser                      map[string]map[string]interface{}
	GitHubIssueComments             map[string][]map[string]interface{}
	GitHubCommentReactions          map[string][]map[string]interface{}
	GitHubIssueReactions            map[string][]map[string]interface{}
	GitHubPull                      map[string]map[string]interface{}
	GitHubPulls                     map[string][]map[string]interface{}
	GitHubPullReviews               map[string][]map[string]interface{}
	GitHubPullReviewComments        map[string][]map[string]interface{}
	GitHubReviewCommentReactions    map[string][]map[string]interface{}
	GitHubPullRequestedReviewers    map[string][]map[string]interface{}
	GitHubPullCommits               map[string][]map[string]interface{}
	GitHubUserOrgs                  map[string][]map[string]interface{}
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

func (j *DSGitHub) githubRepo(ctx *Ctx, org, repo string) (repoData map[string]interface{}, err error) {
	var found bool
	origin := org + "/" + repo
	// Try memory cache 1st
	if CacheGitHubRepo {
		if j.GitHubRepoMtx != nil {
			j.GitHubRepoMtx.RLock()
		}
		repoData, found = j.GitHubRepo[origin]
		if j.GitHubRepoMtx != nil {
			j.GitHubRepoMtx.RUnlock()
		}
		if found {
			// Printf("repos found in cache: %+v\n", repoData)
			return
		}
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
			if CacheGitHubRepo {
				if j.GitHubRepoMtx != nil {
					j.GitHubRepoMtx.Lock()
				}
				j.GitHubRepo[origin] = nil
				if j.GitHubRepoMtx != nil {
					j.GitHubRepoMtx.Unlock()
				}
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
	if CacheGitHubRepo {
		if j.GitHubRepoMtx != nil {
			j.GitHubRepoMtx.Lock()
		}
		j.GitHubRepo[origin] = repoData
		if j.GitHubRepoMtx != nil {
			j.GitHubRepoMtx.Unlock()
		}
	}
	return
}

func (j *DSGitHub) githubUser(ctx *Ctx, login string) (user map[string]interface{}, found bool, err error) {
	var ok bool
	// Try memory cache 1st
	if CacheGitHubUser {
		if j.GitHubUserMtx != nil {
			j.GitHubUserMtx.RLock()
		}
		user, ok = j.GitHubUser[login]
		if j.GitHubUserMtx != nil {
			j.GitHubUserMtx.RUnlock()
		}
		if ok {
			found = len(user) > 0
			// Printf("user found in memory cache: %+v\n", user)
			return
		}
		// Try file cache 2nd
		if CacheGitHubUserFiles {
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
				allowedAge := MaxGitHubUsersFileCacheAge + rand.Intn(MaxGitHubUsersFileCacheAge)
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
		}
	}
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
			if CacheGitHubUser {
				if j.GitHubUserMtx != nil {
					j.GitHubUserMtx.Lock()
				}
				// Printf("user not found using API: %s\n", login)
				j.GitHubUser[login] = map[string]interface{}{}
				if j.GitHubUserMtx != nil {
					j.GitHubUserMtx.Unlock()
				}
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
			user["organizations"], err = j.githubUserOrgs(ctx, login)
			if err != nil {
				return
			}
			// Printf("user found using API: %+v\n", user)
			found = true
		}
		break
	}
	if CacheGitHubUser {
		if j.GitHubUserMtx != nil {
			j.GitHubUserMtx.Lock()
		}
		j.GitHubUser[login] = user
		if j.GitHubUserMtx != nil {
			j.GitHubUserMtx.Unlock()
		}
	}
	return
}

func (j *DSGitHub) githubIssues(ctx *Ctx, org, repo string, since *time.Time) (issuesData []map[string]interface{}, err error) {
	var found bool
	origin := org + "/" + repo
	// Try memory cache 1st
	if CacheGitHubIssues {
		if j.GitHubIssuesMtx != nil {
			j.GitHubIssuesMtx.RLock()
		}
		issuesData, found = j.GitHubIssues[origin]
		if j.GitHubIssuesMtx != nil {
			j.GitHubIssuesMtx.RUnlock()
		}
		if found {
			// Printf("issues found in cache: %+v\n", issuesData)
			return
		}
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
	opt.PerPage = ItemsPerPage
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
			if CacheGitHubIssues {
				if j.GitHubIssuesMtx != nil {
					j.GitHubIssuesMtx.Lock()
				}
				j.GitHubIssues[origin] = []map[string]interface{}{}
				if j.GitHubIssuesMtx != nil {
					j.GitHubIssuesMtx.Unlock()
				}
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
			body, ok := Dig(iss, []string{"body"}, false, true)
			if ok {
				nBody := len(body.(string))
				if nBody > MaxIssueBodyLength {
					iss["body"] = body.(string)[:MaxIssueBodyLength]
				}
			}
			iss["is_pull"] = issue.IsPullRequest()
			issuesData = append(issuesData, iss)
		}
		if response.NextPage == 0 {
			break
		}
		opt.Page = response.NextPage
		retry = false
		// Printf("issues got from API: %+v\n", issuesData)
	}
	if CacheGitHubIssues {
		if j.GitHubIssuesMtx != nil {
			j.GitHubIssuesMtx.Lock()
		}
		j.GitHubIssues[origin] = issuesData
		if j.GitHubIssuesMtx != nil {
			j.GitHubIssuesMtx.Unlock()
		}
	}
	return
}

func (j *DSGitHub) githubIssueComments(ctx *Ctx, org, repo string, number int) (comments []map[string]interface{}, err error) {
	var found bool
	key := fmt.Sprintf("%s/%s/%d", org, repo, number)
	// Try memory cache 1st
	if CacheGitHubIssueComments {
		if j.GitHubIssueCommentsMtx != nil {
			j.GitHubIssueCommentsMtx.RLock()
		}
		comments, found = j.GitHubIssueComments[key]
		if j.GitHubIssueCommentsMtx != nil {
			j.GitHubIssueCommentsMtx.RUnlock()
		}
		if found {
			// Printf("issue comments found in cache: %+v\n", comments)
			return
		}
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
	opt.PerPage = ItemsPerPage
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
			if CacheGitHubIssueComments {
				if j.GitHubIssueCommentsMtx != nil {
					j.GitHubIssueCommentsMtx.Lock()
				}
				j.GitHubIssueComments[key] = []map[string]interface{}{}
				if j.GitHubIssueCommentsMtx != nil {
					j.GitHubIssueCommentsMtx.Unlock()
				}
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
			body, ok := Dig(com, []string{"body"}, false, true)
			if ok {
				nBody := len(body.(string))
				if nBody > MaxCommentBodyLength {
					com["body"] = body.(string)[:MaxCommentBodyLength]
				}
			}
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
	if CacheGitHubIssueComments {
		if j.GitHubIssueCommentsMtx != nil {
			j.GitHubIssueCommentsMtx.Lock()
		}
		j.GitHubIssueComments[key] = comments
		if j.GitHubIssueCommentsMtx != nil {
			j.GitHubIssueCommentsMtx.Unlock()
		}
	}
	return
}

func (j *DSGitHub) githubCommentReactions(ctx *Ctx, org, repo string, cid int64) (reactions []map[string]interface{}, err error) {
	var found bool
	key := fmt.Sprintf("%s/%s/%d", org, repo, cid)
	// fmt.Printf("githubCommentReactions %s\n", key)
	// Try memory cache 1st
	if CacheGitHubCommentReactions {
		if j.GitHubCommentReactionsMtx != nil {
			j.GitHubCommentReactionsMtx.RLock()
		}
		reactions, found = j.GitHubCommentReactions[key]
		if j.GitHubCommentReactionsMtx != nil {
			j.GitHubCommentReactionsMtx.RUnlock()
		}
		if found {
			// Printf("comment reactions found in cache: %+v\n", reactions)
			return
		}
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
	opt.PerPage = ItemsPerPage
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
			if CacheGitHubCommentReactions {
				if j.GitHubCommentReactionsMtx != nil {
					j.GitHubCommentReactionsMtx.Lock()
				}
				j.GitHubCommentReactions[key] = []map[string]interface{}{}
				if j.GitHubCommentReactionsMtx != nil {
					j.GitHubCommentReactionsMtx.Unlock()
				}
			}
			if ctx.Debug > 1 {
				Printf("githubCommentReactions: reactions not found %s: %v\n", key, e)
			}
			return
		}
		if e != nil && !retry {
			Printf("Error getting %s comment reactions: response: %+v, error: %+v, retrying rate\n", key, response, e)
			Printf("githubCommentReactions: handle rate\n")
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
	if CacheGitHubCommentReactions {
		if j.GitHubCommentReactionsMtx != nil {
			j.GitHubCommentReactionsMtx.Lock()
		}
		j.GitHubCommentReactions[key] = reactions
		if j.GitHubCommentReactionsMtx != nil {
			j.GitHubCommentReactionsMtx.Unlock()
		}
	}
	return
}

func (j *DSGitHub) githubIssueReactions(ctx *Ctx, org, repo string, number int) (reactions []map[string]interface{}, err error) {
	var found bool
	key := fmt.Sprintf("%s/%s/%d", org, repo, number)
	// fmt.Printf("githubIssueReactions %s\n", key)
	// Try memory cache 1st
	if CacheGitHubIssueReactions {
		if j.GitHubIssueReactionsMtx != nil {
			j.GitHubIssueReactionsMtx.RLock()
		}
		reactions, found = j.GitHubIssueReactions[key]
		if j.GitHubIssueReactionsMtx != nil {
			j.GitHubIssueReactionsMtx.RUnlock()
		}
		if found {
			// Printf("issue reactions found in cache: %+v\n", reactions)
			return
		}
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
	opt.PerPage = ItemsPerPage
	retry := false
	for {
		var (
			response *github.Response
			reacts   []*github.Reaction
			e        error
		)
		reacts, response, e = c.Reactions.ListIssueReactions(j.Context, org, repo, number, opt)
		// Printf("GET %s/%s/%d -> {%+v, %+v, %+v}\n", org, repo, number, reacts, response, e)
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if CacheGitHubIssueReactions {
				if j.GitHubIssueReactionsMtx != nil {
					j.GitHubIssueReactionsMtx.Lock()
				}
				j.GitHubIssueReactions[key] = []map[string]interface{}{}
				if j.GitHubIssueReactionsMtx != nil {
					j.GitHubIssueReactionsMtx.Unlock()
				}
			}
			if ctx.Debug > 1 {
				Printf("githubIssueReactions: reactions not found %s: %v\n", key, e)
			}
			return
		}
		if e != nil && !retry {
			Printf("Error getting %s issue reactions: response: %+v, error: %+v, retrying rate\n", key, response, e)
			Printf("githubIssueReactions: handle rate\n")
			abuse := j.isAbuse(e)
			if abuse {
				sleepFor := 10 + rand.Intn(10)
				Printf("GitHub detected abuse (get issue reactions %s), waiting for %ds\n", key, sleepFor)
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
		// Printf("issue reactions got from API: %+v\n", reactions)
	}
	if CacheGitHubIssueReactions {
		if j.GitHubIssueReactionsMtx != nil {
			j.GitHubIssueReactionsMtx.Lock()
		}
		j.GitHubIssueReactions[key] = reactions
		if j.GitHubIssueReactionsMtx != nil {
			j.GitHubIssueReactionsMtx.Unlock()
		}
	}
	return
}

func (j *DSGitHub) githubPull(ctx *Ctx, org, repo string, number int) (pullData map[string]interface{}, err error) {
	var found bool
	key := fmt.Sprintf("%s/%s/%d", org, repo, number)
	// Try memory cache 1st
	if CacheGitHubPull {
		if j.GitHubPullMtx != nil {
			j.GitHubPullMtx.RLock()
		}
		pullData, found = j.GitHubPull[key]
		if j.GitHubPullMtx != nil {
			j.GitHubPullMtx.RUnlock()
		}
		if found {
			// Printf("pull found in cache: %+v\n", pullData)
			return
		}
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
			pull     *github.PullRequest
			e        error
		)
		pull, response, e = c.PullRequests.Get(j.Context, org, repo, number)
		// Printf("GET %s/%s/%d -> {%+v, %+v, %+v}\n", org, repo, number, pull, response, e)
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if CacheGitHubPull {
				if j.GitHubPullMtx != nil {
					j.GitHubPullMtx.Lock()
				}
				j.GitHubPull[key] = nil
				if j.GitHubPullMtx != nil {
					j.GitHubPullMtx.Unlock()
				}
			}
			if ctx.Debug > 1 {
				Printf("githubPull: pull not found %s: %v\n", key, e)
			}
			return
		}
		if e != nil && !retry {
			Printf("Error getting %s pull: response: %+v, error: %+v, retrying rate\n", key, response, e)
			Printf("githubPulls: handle rate\n")
			abuse := j.isAbuse(e)
			if abuse {
				sleepFor := 10 + rand.Intn(10)
				Printf("GitHub detected abuse (get pull %s), waiting for %ds\n", key, sleepFor)
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
		jm, _ := jsoniter.Marshal(pull)
		_ = jsoniter.Unmarshal(jm, &pullData)
		body, ok := Dig(pullData, []string{"body"}, false, true)
		if ok {
			nBody := len(body.(string))
			if nBody > MaxPullBodyLength {
				pullData["body"] = body.(string)[:MaxPullBodyLength]
			}
		}
		// Printf("pull got from API: %+v\n", pullData)
		break
	}
	if CacheGitHubPull {
		if j.GitHubPullMtx != nil {
			j.GitHubPullMtx.Lock()
		}
		j.GitHubPull[key] = pullData
		if j.GitHubPullMtx != nil {
			j.GitHubPullMtx.Unlock()
		}
	}
	return
}

func (j *DSGitHub) githubPullsFromIssues(ctx *Ctx, org, repo string, since *time.Time) (pullsData []map[string]interface{}, err error) {
	var (
		issues []map[string]interface{}
		pull   map[string]interface{}
	)
	issues, err = j.githubIssues(ctx, org, repo, ctx.DateFrom)
	if err != nil {
		return
	}
	for _, issue := range issues {
		isPR, _ := issue["is_pull"]
		if !isPR.(bool) {
			continue
		}
		number, _ := issue["number"]
		pull, err = j.githubPull(ctx, org, repo, int(number.(float64)))
		if err != nil {
			return
		}
		pullsData = append(pullsData, pull)
	}
	return
}

func (j *DSGitHub) githubPulls(ctx *Ctx, org, repo string) (pullsData []map[string]interface{}, err error) {
	// WARNING: this is not returning all possible Pull sub fields, recommend to use githubPullsFromIssues instead.
	var found bool
	origin := org + "/" + repo
	// Try memory cache 1st
	if CacheGitHubPulls {
		if j.GitHubPullsMtx != nil {
			j.GitHubPullsMtx.RLock()
		}
		pullsData, found = j.GitHubPulls[origin]
		if j.GitHubPullsMtx != nil {
			j.GitHubPullsMtx.RUnlock()
		}
		if found {
			// Printf("pulls found in cache: %+v\n", pullsData)
			return
		}
	}
	var c *github.Client
	if j.GitHubMtx != nil {
		j.GitHubMtx.RLock()
	}
	c = j.Clients[j.Hint]
	if j.GitHubMtx != nil {
		j.GitHubMtx.RUnlock()
	}
	opt := &github.PullRequestListOptions{
		State:     "all",
		Sort:      "updated",
		Direction: "asc",
	}
	opt.PerPage = ItemsPerPage
	retry := false
	for {
		var (
			response *github.Response
			pulls    []*github.PullRequest
			e        error
		)
		pulls, response, e = c.PullRequests.List(j.Context, org, repo, opt)
		// Printf("GET %s/%s -> {%+v, %+v, %+v}\n", org, repo, pulls, response, e)
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if CacheGitHubPulls {
				if j.GitHubPullsMtx != nil {
					j.GitHubPullsMtx.Lock()
				}
				j.GitHubPulls[origin] = []map[string]interface{}{}
				if j.GitHubPullsMtx != nil {
					j.GitHubPullsMtx.Unlock()
				}
			}
			if ctx.Debug > 1 {
				Printf("githubPulls: pulls not found %s: %v\n", origin, e)
			}
			return
		}
		if e != nil && !retry {
			Printf("Error getting %s pulls: response: %+v, error: %+v, retrying rate\n", origin, response, e)
			Printf("githubPulls: handle rate\n")
			abuse := j.isAbuse(e)
			if abuse {
				sleepFor := 10 + rand.Intn(10)
				Printf("GitHub detected abuse (get pulls %s), waiting for %ds\n", origin, sleepFor)
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
		for _, pull := range pulls {
			pr := map[string]interface{}{}
			jm, _ := jsoniter.Marshal(pull)
			_ = jsoniter.Unmarshal(jm, &pr)
			body, ok := Dig(pr, []string{"body"}, false, true)
			if ok {
				nBody := len(body.(string))
				if nBody > MaxPullBodyLength {
					pr["body"] = body.(string)[:MaxPullBodyLength]
				}
			}
			pullsData = append(pullsData, pr)
		}
		if response.NextPage == 0 {
			break
		}
		opt.Page = response.NextPage
		retry = false
		// Printf("pulls got from API: %+v\n", pullsData)
	}
	if CacheGitHubPulls {
		if j.GitHubPullsMtx != nil {
			j.GitHubPullsMtx.Lock()
		}
		j.GitHubPulls[origin] = pullsData
		if j.GitHubPullsMtx != nil {
			j.GitHubPullsMtx.Unlock()
		}
	}
	return
}

func (j *DSGitHub) githubPullReviews(ctx *Ctx, org, repo string, number int) (reviews []map[string]interface{}, err error) {
	var found bool
	key := fmt.Sprintf("%s/%s/%d", org, repo, number)
	// Try memory cache 1st
	if CacheGitHubPullReviews {
		if j.GitHubPullReviewsMtx != nil {
			j.GitHubPullReviewsMtx.RLock()
		}
		reviews, found = j.GitHubPullReviews[key]
		if j.GitHubPullReviewsMtx != nil {
			j.GitHubPullReviewsMtx.RUnlock()
		}
		if found {
			// Printf("pull reviews found in cache: %+v\n", reviews)
			return
		}
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
	opt.PerPage = ItemsPerPage
	retry := false
	for {
		var (
			response *github.Response
			revs     []*github.PullRequestReview
			e        error
		)
		revs, response, e = c.PullRequests.ListReviews(j.Context, org, repo, number, opt)
		// Printf("GET %s/%s/%s -> {%+v, %+v, %+v}\n", org, repo, number, revs, response, e)
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if CacheGitHubPullReviews {
				if j.GitHubPullReviewsMtx != nil {
					j.GitHubPullReviewsMtx.Lock()
				}
				j.GitHubPullReviews[key] = []map[string]interface{}{}
				if j.GitHubPullReviewsMtx != nil {
					j.GitHubPullReviewsMtx.Unlock()
				}
			}
			if ctx.Debug > 1 {
				Printf("githubPullReviews: reviews not found %s: %v\n", key, e)
			}
			return
		}
		if e != nil && !retry {
			Printf("Error getting %s pull reviews: response: %+v, error: %+v, retrying rate\n", key, response, e)
			Printf("githubPullReviews: handle rate\n")
			abuse := j.isAbuse(e)
			if abuse {
				sleepFor := 10 + rand.Intn(10)
				Printf("GitHub detected abuse (get pull reviews %s), waiting for %ds\n", key, sleepFor)
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
		for _, review := range revs {
			rev := map[string]interface{}{}
			jm, _ := jsoniter.Marshal(review)
			_ = jsoniter.Unmarshal(jm, &rev)
			body, ok := Dig(rev, []string{"body"}, false, true)
			if ok {
				nBody := len(body.(string))
				if nBody > MaxReviewBodyLength {
					rev["body"] = body.(string)[:MaxReviewBodyLength]
				}
			}
			userLogin, ok := Dig(rev, []string{"user", "login"}, false, true)
			if ok {
				rev["user_data"], _, err = j.githubUser(ctx, userLogin.(string))
				if err != nil {
					return
				}
			}
			reviews = append(reviews, rev)
		}
		if response.NextPage == 0 {
			break
		}
		opt.Page = response.NextPage
		retry = false
		// Printf("pull reviews got from API: %+v\n", reviews)
	}
	if CacheGitHubPullReviews {
		if j.GitHubPullReviewsMtx != nil {
			j.GitHubPullReviewsMtx.Lock()
		}
		j.GitHubPullReviews[key] = reviews
		if j.GitHubPullReviewsMtx != nil {
			j.GitHubPullReviewsMtx.Unlock()
		}
	}
	return
}

func (j *DSGitHub) githubPullReviewComments(ctx *Ctx, org, repo string, number int) (reviewComments []map[string]interface{}, err error) {
	var found bool
	key := fmt.Sprintf("%s/%s/%d", org, repo, number)
	// Try memory cache 1st
	if CacheGitHubPullReviewComments {
		if j.GitHubPullReviewCommentsMtx != nil {
			j.GitHubPullReviewCommentsMtx.RLock()
		}
		reviewComments, found = j.GitHubPullReviewComments[key]
		if j.GitHubPullReviewCommentsMtx != nil {
			j.GitHubPullReviewCommentsMtx.RUnlock()
		}
		if found {
			// Printf("pull review comments found in cache: %+v\n", reviewComments)
			return
		}
	}
	var c *github.Client
	if j.GitHubMtx != nil {
		j.GitHubMtx.RLock()
	}
	c = j.Clients[j.Hint]
	if j.GitHubMtx != nil {
		j.GitHubMtx.RUnlock()
	}
	opt := &github.PullRequestListCommentsOptions{
		Sort:      "updated",
		Direction: "asc",
	}
	opt.PerPage = ItemsPerPage
	retry := false
	for {
		var (
			response *github.Response
			revComms []*github.PullRequestComment
			e        error
		)
		revComms, response, e = c.PullRequests.ListComments(j.Context, org, repo, number, opt)
		// Printf("GET %s/%s/%s -> {%+v, %+v, %+v}\n", org, repo, number, revComms, response, e)
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if CacheGitHubPullReviewComments {
				if j.GitHubPullReviewCommentsMtx != nil {
					j.GitHubPullReviewCommentsMtx.Lock()
				}
				j.GitHubPullReviewComments[key] = []map[string]interface{}{}
				if j.GitHubPullReviewCommentsMtx != nil {
					j.GitHubPullReviewCommentsMtx.Unlock()
				}
			}
			if ctx.Debug > 1 {
				Printf("githubPullReviewComments: review comments not found %s: %v\n", key, e)
			}
			return
		}
		if e != nil && !retry {
			Printf("Error getting %s pull review comments: response: %+v, error: %+v, retrying rate\n", key, response, e)
			Printf("githubPullReviewComments: handle rate\n")
			abuse := j.isAbuse(e)
			if abuse {
				sleepFor := 10 + rand.Intn(10)
				Printf("GitHub detected abuse (get pull review comments %s), waiting for %ds\n", key, sleepFor)
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
		for _, reviewComment := range revComms {
			revComm := map[string]interface{}{}
			jm, _ := jsoniter.Marshal(reviewComment)
			_ = jsoniter.Unmarshal(jm, &revComm)
			body, ok := Dig(revComm, []string{"body"}, false, true)
			if ok {
				nBody := len(body.(string))
				if nBody > MaxReviewCommentBodyLength {
					revComm["body"] = body.(string)[:MaxReviewCommentBodyLength]
				}
			}
			userLogin, ok := Dig(revComm, []string{"user", "login"}, false, true)
			if ok {
				revComm["user_data"], _, err = j.githubUser(ctx, userLogin.(string))
				if err != nil {
					return
				}
			}
			iCnt, ok := Dig(revComm, []string{"reactions", "total_count"}, false, true)
			if ok {
				revComm["reactions_data"] = []interface{}{}
				cnt := int(iCnt.(float64))
				if cnt > 0 {
					cid, ok := Dig(revComm, []string{"id"}, false, true)
					if ok {
						revComm["reactions_data"], err = j.githubReviewCommentReactions(ctx, org, repo, int64(cid.(float64)))
						if err != nil {
							return
						}
					}
				}
			}
			reviewComments = append(reviewComments, revComm)
		}
		if response.NextPage == 0 {
			break
		}
		opt.Page = response.NextPage
		retry = false
		// Printf("pull review comments got from API: %+v\n", reviewComments)
	}
	if CacheGitHubPullReviewComments {
		if j.GitHubPullReviewCommentsMtx != nil {
			j.GitHubPullReviewCommentsMtx.Lock()
		}
		j.GitHubPullReviewComments[key] = reviewComments
		if j.GitHubPullReviewCommentsMtx != nil {
			j.GitHubPullReviewCommentsMtx.Unlock()
		}
	}
	return
}

func (j *DSGitHub) githubReviewCommentReactions(ctx *Ctx, org, repo string, cid int64) (reactions []map[string]interface{}, err error) {
	var found bool
	key := fmt.Sprintf("%s/%s/%d", org, repo, cid)
	// fmt.Printf("githubReviewCommentReactions %s\n", key)
	// Try memory cache 1st
	if CacheGitHubReviewCommentReactions {
		if j.GitHubReviewCommentReactionsMtx != nil {
			j.GitHubReviewCommentReactionsMtx.RLock()
		}
		reactions, found = j.GitHubReviewCommentReactions[key]
		if j.GitHubReviewCommentReactionsMtx != nil {
			j.GitHubReviewCommentReactionsMtx.RUnlock()
		}
		if found {
			// Printf("comment reactions found in cache: %+v\n", reactions)
			return
		}
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
	opt.PerPage = ItemsPerPage
	retry := false
	for {
		var (
			response *github.Response
			reacts   []*github.Reaction
			e        error
		)
		reacts, response, e = c.Reactions.ListPullRequestCommentReactions(j.Context, org, repo, cid, opt)
		// Printf("GET %s/%s/%d -> {%+v, %+v, %+v}\n", org, repo, cid, reacts, response, e)
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if CacheGitHubReviewCommentReactions {
				if j.GitHubReviewCommentReactionsMtx != nil {
					j.GitHubReviewCommentReactionsMtx.Lock()
				}
				j.GitHubReviewCommentReactions[key] = []map[string]interface{}{}
				if j.GitHubReviewCommentReactionsMtx != nil {
					j.GitHubReviewCommentReactionsMtx.Unlock()
				}
			}
			if ctx.Debug > 1 {
				Printf("githubReviewCommentReactions: reactions not found %s: %v\n", key, e)
			}
			return
		}
		if e != nil && !retry {
			Printf("Error getting %s comment reactions: response: %+v, error: %+v, retrying rate\n", key, response, e)
			Printf("githubReviewCommentReactions: handle rate\n")
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
	if CacheGitHubReviewCommentReactions {
		if j.GitHubReviewCommentReactionsMtx != nil {
			j.GitHubReviewCommentReactionsMtx.Lock()
		}
		j.GitHubReviewCommentReactions[key] = reactions
		if j.GitHubReviewCommentReactionsMtx != nil {
			j.GitHubReviewCommentReactionsMtx.Unlock()
		}
	}
	return
}

func (j *DSGitHub) githubPullRequestedReviewers(ctx *Ctx, org, repo string, number int) (reviewers []map[string]interface{}, err error) {
	var found bool
	key := fmt.Sprintf("%s/%s/%d", org, repo, number)
	// Try memory cache 1st
	if CacheGitHubPullRequestedReviewers {
		if j.GitHubPullRequestedReviewersMtx != nil {
			j.GitHubPullRequestedReviewersMtx.RLock()
		}
		reviewers, found = j.GitHubPullRequestedReviewers[key]
		if j.GitHubPullRequestedReviewersMtx != nil {
			j.GitHubPullRequestedReviewersMtx.RUnlock()
		}
		if found {
			// Printf("pull requested reviewers found in cache: %+v\n", reviewers)
			return
		}
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
	opt.PerPage = ItemsPerPage
	retry := false
	for {
		var (
			response *github.Response
			revsObj  *github.Reviewers
			e        error
		)
		revsObj, response, e = c.PullRequests.ListReviewers(j.Context, org, repo, number, opt)
		// Printf("GET %s/%s/%s -> {%+v, %+v, %+v}\n", org, repo, number, revsObj, response, e)
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if CacheGitHubPullRequestedReviewers {
				if j.GitHubPullRequestedReviewersMtx != nil {
					j.GitHubPullRequestedReviewersMtx.Lock()
				}
				j.GitHubPullRequestedReviewers[key] = []map[string]interface{}{}
				if j.GitHubPullRequestedReviewersMtx != nil {
					j.GitHubPullRequestedReviewersMtx.Unlock()
				}
			}
			if ctx.Debug > 1 {
				Printf("githubPullRequestedReviewers: reviewers not found %s: %v\n", key, e)
			}
			return
		}
		if e != nil && !retry {
			Printf("Error getting %s pull requested reviewers: response: %+v, error: %+v, retrying rate\n", key, response, e)
			Printf("githubPullRequestedReviewers: handle rate\n")
			abuse := j.isAbuse(e)
			if abuse {
				sleepFor := 10 + rand.Intn(10)
				Printf("GitHub detected abuse (get pull requested reviewers %s), waiting for %ds\n", key, sleepFor)
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
		users := revsObj.Users
		for _, reviewer := range users {
			if reviewer.Login == nil {
				continue
			}
			var userData map[string]interface{}
			userData, _, err = j.githubUser(ctx, *reviewer.Login)
			if err != nil {
				return
			}
			reviewers = append(reviewers, userData)
		}
		if response.NextPage == 0 {
			break
		}
		opt.Page = response.NextPage
		retry = false
		// Printf("pull requested reviewers got from API: %+v\n", reviewers)
	}
	if CacheGitHubPullRequestedReviewers {
		if j.GitHubPullRequestedReviewersMtx != nil {
			j.GitHubPullRequestedReviewersMtx.Lock()
		}
		j.GitHubPullRequestedReviewers[key] = reviewers
		if j.GitHubPullRequestedReviewersMtx != nil {
			j.GitHubPullRequestedReviewersMtx.Unlock()
		}
	}
	return
}

func (j *DSGitHub) githubPullCommits(ctx *Ctx, org, repo string, number int, deep bool) (commits []map[string]interface{}, err error) {
	var found bool
	key := fmt.Sprintf("%s/%s/%d", org, repo, number)
	// Try memory cache 1st
	if CacheGitHubPullCommits {
		if j.GitHubPullCommitsMtx != nil {
			j.GitHubPullCommitsMtx.RLock()
		}
		commits, found = j.GitHubPullCommits[key]
		if j.GitHubPullCommitsMtx != nil {
			j.GitHubPullCommitsMtx.RUnlock()
		}
		if found {
			// Printf("pull commits found in cache: %+v\n", commits)
			return
		}
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
	opt.PerPage = ItemsPerPage
	retry := false
	for {
		var (
			response *github.Response
			comms    []*github.RepositoryCommit
			e        error
		)
		comms, response, e = c.PullRequests.ListCommits(j.Context, org, repo, number, opt)
		// Printf("GET %s/%s/%s -> {%+v, %+v, %+v}\n", org, repo, number, comms, response, e)
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if CacheGitHubPullCommits {
				if j.GitHubPullCommitsMtx != nil {
					j.GitHubPullCommitsMtx.Lock()
				}
				j.GitHubPullCommits[key] = []map[string]interface{}{}
				if j.GitHubPullCommitsMtx != nil {
					j.GitHubPullCommitsMtx.Unlock()
				}
			}
			if ctx.Debug > 1 {
				Printf("githubPullCommits: commits not found %s: %v\n", key, e)
			}
			return
		}
		if e != nil && !retry {
			Printf("Error getting %s pull commits: response: %+v, error: %+v, retrying rate\n", key, response, e)
			Printf("githubPullCommits: handle rate\n")
			abuse := j.isAbuse(e)
			if abuse {
				sleepFor := 10 + rand.Intn(10)
				Printf("GitHub detected abuse (get pull commits %s), waiting for %ds\n", key, sleepFor)
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
		for _, commit := range comms {
			com := map[string]interface{}{}
			jm, _ := jsoniter.Marshal(commit)
			_ = jsoniter.Unmarshal(jm, &com)
			if deep {
				userLogin, ok := Dig(com, []string{"author", "login"}, false, true)
				if ok {
					com["author_data"], _, err = j.githubUser(ctx, userLogin.(string))
					if err != nil {
						return
					}
				}
				userLogin, ok = Dig(com, []string{"committer", "login"}, false, true)
				if ok {
					com["committer_data"], _, err = j.githubUser(ctx, userLogin.(string))
					if err != nil {
						return
					}
				}
			}
			commits = append(commits, com)
		}
		if response.NextPage == 0 {
			break
		}
		opt.Page = response.NextPage
		retry = false
		// Printf("pull commits got from API: %+v\n", commits)
	}
	if CacheGitHubPullCommits {
		if j.GitHubPullCommitsMtx != nil {
			j.GitHubPullCommitsMtx.Lock()
		}
		j.GitHubPullCommits[key] = commits
		if j.GitHubPullCommitsMtx != nil {
			j.GitHubPullCommitsMtx.Unlock()
		}
	}
	return
}

func (j *DSGitHub) githubUserOrgs(ctx *Ctx, login string) (orgsData []map[string]interface{}, err error) {
	var found bool
	// Try memory cache 1st
	if CacheGitHubUserOrgs {
		if j.GitHubUserOrgsMtx != nil {
			j.GitHubUserOrgsMtx.RLock()
		}
		orgsData, found = j.GitHubUserOrgs[login]
		if j.GitHubUserOrgsMtx != nil {
			j.GitHubUserOrgsMtx.RUnlock()
		}
		if found {
			// Printf("user orgs found in cache: %+v\n", orgsData)
			return
		}
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
	opt.PerPage = ItemsPerPage
	retry := false
	for {
		var (
			response      *github.Response
			organizations []*github.Organization
			e             error
		)
		organizations, response, e = c.Organizations.List(j.Context, login, opt)
		// Printf("GET %s -> {%+v, %+v, %+v}\n", login, organizations, response, e)
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if CacheGitHubUserOrgs {
				if j.GitHubUserOrgsMtx != nil {
					j.GitHubUserOrgsMtx.Lock()
				}
				j.GitHubUserOrgs[login] = []map[string]interface{}{}
				if j.GitHubUserOrgsMtx != nil {
					j.GitHubUserOrgsMtx.Unlock()
				}
			}
			if ctx.Debug > 1 {
				Printf("githubUserOrgs: orgs not found %s: %v\n", login, e)
			}
			return
		}
		if e != nil && !retry {
			Printf("Error getting %s user orgs: response: %+v, error: %+v, retrying rate\n", login, response, e)
			Printf("githubUserOrgs: handle rate\n")
			abuse := j.isAbuse(e)
			if abuse {
				sleepFor := 10 + rand.Intn(10)
				Printf("GitHub detected abuse (get user orgs %s), waiting for %ds\n", login, sleepFor)
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
		for _, organization := range organizations {
			org := map[string]interface{}{}
			jm, _ := jsoniter.Marshal(organization)
			_ = jsoniter.Unmarshal(jm, &org)
			orgsData = append(orgsData, org)
		}
		if response.NextPage == 0 {
			break
		}
		opt.Page = response.NextPage
		retry = false
		// Printf("user orgs got from API: %+v\n", orgsData)
	}
	if CacheGitHubUserOrgs {
		if j.GitHubUserOrgsMtx != nil {
			j.GitHubUserOrgsMtx.Lock()
		}
		j.GitHubUserOrgs[login] = orgsData
		if j.GitHubUserOrgsMtx != nil {
			j.GitHubUserOrgsMtx.Unlock()
		}
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
	if CacheGitHubRepo {
		j.GitHubRepo = make(map[string]map[string]interface{})
	}
	if CacheGitHubIssues {
		j.GitHubIssues = make(map[string][]map[string]interface{})
	}
	if CacheGitHubUser {
		j.GitHubUser = make(map[string]map[string]interface{})
	}
	if CacheGitHubIssueComments {
		j.GitHubIssueComments = make(map[string][]map[string]interface{})
	}
	if CacheGitHubCommentReactions {
		j.GitHubCommentReactions = make(map[string][]map[string]interface{})
	}
	if CacheGitHubIssueReactions {
		j.GitHubIssueReactions = make(map[string][]map[string]interface{})
	}
	if CacheGitHubPull {
		j.GitHubPull = make(map[string]map[string]interface{})
	}
	if CacheGitHubPulls {
		j.GitHubPulls = make(map[string][]map[string]interface{})
	}
	if CacheGitHubPullReviews {
		j.GitHubPullReviews = make(map[string][]map[string]interface{})
	}
	if CacheGitHubPullReviewComments {
		j.GitHubPullReviewComments = make(map[string][]map[string]interface{})
	}
	if CacheGitHubReviewCommentReactions {
		j.GitHubReviewCommentReactions = make(map[string][]map[string]interface{})
	}
	if CacheGitHubPullRequestedReviewers {
		j.GitHubPullRequestedReviewers = make(map[string][]map[string]interface{})
	}
	if CacheGitHubPullCommits {
		j.GitHubPullCommits = make(map[string][]map[string]interface{})
	}
	if CacheGitHubUserOrgs {
		j.GitHubUserOrgs = make(map[string][]map[string]interface{})
	}
	// Multithreading
	j.ThrN = GetThreadsNum(ctx)
	if j.ThrN > 1 {
		j.GitHubMtx = &sync.RWMutex{}
		if CacheGitHubRepo {
			j.GitHubRepoMtx = &sync.RWMutex{}
		}
		if CacheGitHubIssues {
			j.GitHubIssuesMtx = &sync.RWMutex{}
		}
		if CacheGitHubUser {
			j.GitHubUserMtx = &sync.RWMutex{}
		}
		if CacheGitHubIssueComments {
			j.GitHubIssueCommentsMtx = &sync.RWMutex{}
		}
		if CacheGitHubCommentReactions {
			j.GitHubCommentReactionsMtx = &sync.RWMutex{}
		}
		if CacheGitHubIssueReactions {
			j.GitHubIssueReactionsMtx = &sync.RWMutex{}
		}
		if CacheGitHubPull {
			j.GitHubPullMtx = &sync.RWMutex{}
		}
		if CacheGitHubPulls {
			j.GitHubPullsMtx = &sync.RWMutex{}
		}
		if CacheGitHubPullReviews {
			j.GitHubPullReviewsMtx = &sync.RWMutex{}
		}
		if CacheGitHubPullReviewComments {
			j.GitHubPullReviewCommentsMtx = &sync.RWMutex{}
		}
		if CacheGitHubReviewCommentReactions {
			j.GitHubReviewCommentReactionsMtx = &sync.RWMutex{}
		}
		if CacheGitHubPullRequestedReviewers {
			j.GitHubPullRequestedReviewersMtx = &sync.RWMutex{}
		}
		if CacheGitHubPullCommits {
			j.GitHubPullCommitsMtx = &sync.RWMutex{}
		}
		if CacheGitHubUserOrgs {
			j.GitHubUserOrgsMtx = &sync.RWMutex{}
		}
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
		return j.FetchItemsPullRequest(ctx)
	default:
		err = fmt.Errorf("FetchItems: unknown category %s", j.Category)
	}
	return
}

// FetchItemsRepository - implement raw repository data for GitHub datasource
func (j *DSGitHub) FetchItemsRepository(ctx *Ctx) (err error) {
	items := []interface{}{}
	item, err := j.githubRepo(ctx, j.Org, j.Repo)
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
	// ["user", "assignee", "assignees", "comments", "reactions"]
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
		iCnt, ok := Dig(issue, []string{"reactions", "total_count"}, false, true)
		if ok {
			issue["reactions_data"] = []interface{}{}
			cnt := int(iCnt.(float64))
			if cnt > 0 {
				issue["reactions_data"], err = j.githubIssueReactions(ctx, j.Org, j.Repo, int(number.(float64)))
				if err != nil {
					return
				}
			}
		}
	}
	return
}

// ProcessPull - add PRs sub items
func (j *DSGitHub) ProcessPull(ctx *Ctx, inPull map[string]interface{}) (pull map[string]interface{}, err error) {
	pull = inPull
	pull["user_data"] = map[string]interface{}{}
	pull["merged_by_data"] = map[string]interface{}{}
	pull["review_comments_data"] = map[string]interface{}{}
	pull["reviews_data"] = []interface{}{}
	pull["requested_reviewers_data"] = []interface{}{}
	pull["commits_data"] = []interface{}{}
	// ["user", "review_comments", "requested_reviewers", "merged_by", "commits", "assignee", "assignees"]
	number, ok := Dig(pull, []string{"number"}, false, true)
	if ok {
		iNumber := int(number.(float64))
		pull["reviews_data"], err = j.githubPullReviews(ctx, j.Org, j.Repo, iNumber)
		if err != nil {
			return
		}
		pull["review_comments_data"], err = j.githubPullReviewComments(ctx, j.Org, j.Repo, iNumber)
		if err != nil {
			return
		}
		pull["requested_reviewers_data"], err = j.githubPullRequestedReviewers(ctx, j.Org, j.Repo, iNumber)
		if err != nil {
			return
		}
		// That would fetch the full commit data
		//pull["commits_data"], err = j.githubPullCommits(ctx, j.Org, j.Repo, iNumber, true)
		var commitsData []map[string]interface{}
		commitsData, err = j.githubPullCommits(ctx, j.Org, j.Repo, iNumber, false)
		if err != nil {
			return
		}
		ary := []interface{}{}
		for _, com := range commitsData {
			sha, ok := Dig(com, []string{"sha"}, false, true)
			if ok {
				ary = append(ary, sha)
			}
		}
		pull["commits_data"] = ary
	}
	userLogin, ok := Dig(pull, []string{"user", "login"}, false, true)
	if ok {
		pull["user_data"], _, err = j.githubUser(ctx, userLogin.(string))
		if err != nil {
			return
		}
	}
	mergedByLogin, ok := Dig(pull, []string{"merged_by", "login"}, false, true)
	if ok {
		pull["merged_by_data"], _, err = j.githubUser(ctx, mergedByLogin.(string))
		if err != nil {
			return
		}
	}
	assigneeLogin, ok := Dig(pull, []string{"assignee", "login"}, false, true)
	if ok {
		pull["assignee_data"], _, err = j.githubUser(ctx, assigneeLogin.(string))
		if err != nil {
			return
		}
	}
	iAssignees, ok := Dig(pull, []string{"assignees"}, false, true)
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
		pull["assignees_data"] = assigneesAry
	}
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
	// Process pull requests (possibly in threads)
	var (
		ch          chan error
		allPulls    []interface{}
		allPullsMtx *sync.Mutex
		escha       []chan error
		eschaMtx    *sync.Mutex
	)
	if j.ThrN > 1 {
		ch = make(chan error)
		allPullsMtx = &sync.Mutex{}
		eschaMtx = &sync.Mutex{}
	}
	nThreads := 0
	processPull := func(c chan error, pull map[string]interface{}) (wch chan error, e error) {
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		item, err := j.ProcessPull(ctx, pull)
		FatalOnError(err)
		esItem := j.AddMetadata(ctx, item)
		if ctx.Project != "" {
			item["project"] = ctx.Project
		}
		esItem["data"] = item
		if allPullsMtx != nil {
			allPullsMtx.Lock()
		}
		allPulls = append(allPulls, esItem)
		nPulls := len(allPulls)
		if nPulls >= ctx.ESBulkSize {
			sendToElastic := func(c chan error) (ee error) {
				defer func() {
					if c != nil {
						c <- ee
					}
				}()
				ee = SendToElastic(ctx, j, true, UUID, allPulls)
				if ee != nil {
					Printf("error %v sending %d pulls to ElasticSearch\n", ee, len(allPulls))
				}
				allPulls = []interface{}{}
				if allPullsMtx != nil {
					allPullsMtx.Unlock()
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
			if allPullsMtx != nil {
				allPullsMtx.Unlock()
			}
		}
		return
	}
	var pulls []map[string]interface{}
	// PullRequests.Lit doesn't return merged_by data, we need to use PullRequests.Get on each pull
	// if ctx.DateFrom != nil {
	if 1 == 1 {
		pulls, err = j.githubPullsFromIssues(ctx, j.Org, j.Repo, ctx.DateFrom)
	} else {
		pulls, err = j.githubPulls(ctx, j.Org, j.Repo)
	}
	FatalOnError(err)
	Printf("got %d pulls\n", len(pulls))
	if j.ThrN > 1 {
		for _, pull := range pulls {
			go func(pr map[string]interface{}) {
				var (
					e    error
					esch chan error
				)
				esch, e = processPull(ch, pr)
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
			}(pull)
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
		for _, pull := range pulls {
			_, err = processPull(nil, pull)
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
	nPulls := len(allPulls)
	if ctx.Debug > 0 {
		Printf("%d remaining pulls to send to ES\n", nPulls)
	}
	if nPulls > 0 {
		err = SendToElastic(ctx, j, true, UUID, allPulls)
		if err != nil {
			Printf("Error %v sending %d pulls to ES\n", err, len(allPulls))
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
	id, ok := item.(map[string]interface{})["id"]
	if !ok {
		Fatalf("%s: ItemID() - cannot extract id from %+v", j.DS, DumpKeys(item))
	}
	return fmt.Sprintf("%s/%d", j.Category, int64(id.(float64)))
}

// AddMetadata - add metadata to the item
func (j *DSGitHub) AddMetadata(ctx *Ctx, item interface{}) (mItem map[string]interface{}) {
	mItem = make(map[string]interface{})
	origin := j.URL
	tag := ctx.Tag
	if tag == "" {
		tag = origin
	}
	itemID := j.ItemID(item)
	// fmt.Printf("id = %s\n", itemID)
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
	mItem["is_github_"+j.Category] = 1
	mItem["search_fields"] = make(map[string]interface{})
	FatalOnError(DeepSet(mItem, []string{"search_fields", "owner"}, j.Org, false))
	FatalOnError(DeepSet(mItem, []string{"search_fields", "repo"}, j.Repo, false))
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

// IdentityForObject - construct identity from a given object
func (j *DSGitHub) IdentityForObject(ctx *Ctx, item map[string]interface{}) (identity [3]string) {
	if ctx.Debug > 1 {
		defer func() {
			Printf("IdentityForObject: %+v -> %+v\n", item, identity)
		}()
	}
	for i, prop := range []string{"name", "login", "email"} {
		iVal, ok := Dig(item, []string{prop}, false, true)
		if ok {
			val, ok := iVal.(string)
			if ok {
				identity[i] = val
			}
		} else {
			identity[i] = Nil
		}
	}
	return
}

// GetItemIdentities return list of item's identities, each one is [3]string
// (name, username, email) tripples, special value Nil "none" means null
// we use string and not *string which allows nil to allow usage as a map key
func (j *DSGitHub) GetItemIdentities(ctx *Ctx, doc interface{}) (identities map[[3]string]struct{}, err error) {
	switch j.Category {
	case "repository":
		return
	case "issue":
		// issue: user_data
		// issue: assignee_data
		// issue: assignees_data[]
		// issue: comments_data[].user_data
		// issue: comments_data[].reactions_data[].user_data
		// issue: reactions_data[].user_data
		identities = make(map[[3]string]struct{})
		item, _ := Dig(doc, []string{"data"}, true, false)
		user, _ := Dig(item, []string{"user_data"}, true, false)
		identities[j.IdentityForObject(ctx, user.(map[string]interface{}))] = struct{}{}
		assignee, ok := Dig(item, []string{"assignee_data"}, false, true)
		if ok && assignee != nil {
			identities[j.IdentityForObject(ctx, assignee.(map[string]interface{}))] = struct{}{}
		}
		assignees, ok := Dig(item, []string{"assignees_data"}, false, true)
		if ok && assignees != nil {
			ary, _ := assignees.([]interface{})
			for _, assignee := range ary {
				if assignee != nil {
					identities[j.IdentityForObject(ctx, assignee.(map[string]interface{}))] = struct{}{}
				}
			}
		}
		comments, ok := Dig(item, []string{"comments_data"}, false, true)
		if ok && comments != nil {
			ary, _ := comments.([]interface{})
			for _, comment := range ary {
				comm, _ := comment.(map[string]interface{})
				user, ok := Dig(comm, []string{"user_data"}, false, true)
				if ok && user != nil {
					identities[j.IdentityForObject(ctx, user.(map[string]interface{}))] = struct{}{}
				}
				reactions, ok2 := Dig(comm, []string{"reactions_data"}, false, true)
				if ok2 && reactions != nil {
					ary2, _ := reactions.([]interface{})
					for _, reaction := range ary2 {
						react, _ := reaction.(map[string]interface{})
						user, ok := Dig(react, []string{"user_data"}, false, true)
						if ok && user != nil {
							identities[j.IdentityForObject(ctx, user.(map[string]interface{}))] = struct{}{}
						}
					}
				}
			}
		}
		reactions, ok := Dig(item, []string{"reactions_data"}, false, true)
		if ok && reactions != nil {
			ary, _ := reactions.([]interface{})
			for _, reaction := range ary {
				react, _ := reaction.(map[string]interface{})
				user, ok := Dig(react, []string{"user_data"}, false, true)
				if ok && user != nil {
					identities[j.IdentityForObject(ctx, user.(map[string]interface{}))] = struct{}{}
				}
			}
		}
	case "pull_request":
		// pr:    user_data
		// pr:    merged_by_data
		// pr:    assignee_data
		// pr:    assignees_data[]
		// pr:    reviews_data[].user_data
		// pr:    review_comments_data[].user_data
		// pr:    review_comments_data[].reactions_data[].user_data
		// pr:    requested_reviewers_data[].user_data
		// pr:    commits_data[].author_data (not used)
		// pr:    commits_data[].committer_data (not used)
		identities = make(map[[3]string]struct{})
		item, _ := Dig(doc, []string{"data"}, true, false)
		user, _ := Dig(item, []string{"user_data"}, true, false)
		identities[j.IdentityForObject(ctx, user.(map[string]interface{}))] = struct{}{}
		mergedBy, ok := Dig(item, []string{"merged_by_data"}, false, true)
		if ok && mergedBy != nil {
			identities[j.IdentityForObject(ctx, mergedBy.(map[string]interface{}))] = struct{}{}
		}
		assignee, ok := Dig(item, []string{"assignee_data"}, false, true)
		if ok && assignee != nil {
			identities[j.IdentityForObject(ctx, assignee.(map[string]interface{}))] = struct{}{}
		}
		assignees, ok := Dig(item, []string{"assignees_data"}, false, true)
		if ok && assignees != nil {
			ary, _ := assignees.([]interface{})
			for _, assignee := range ary {
				if assignee != nil {
					identities[j.IdentityForObject(ctx, assignee.(map[string]interface{}))] = struct{}{}
				}
			}
		}
		comments, ok := Dig(item, []string{"review_comments_data"}, false, true)
		if ok && comments != nil {
			ary, _ := comments.([]interface{})
			for _, comment := range ary {
				comm, _ := comment.(map[string]interface{})
				user, ok := Dig(comm, []string{"user_data"}, false, true)
				if ok && user != nil {
					identities[j.IdentityForObject(ctx, user.(map[string]interface{}))] = struct{}{}
				}
				reactions, ok2 := Dig(comm, []string{"reactions_data"}, false, true)
				if ok2 && reactions != nil {
					ary2, _ := reactions.([]interface{})
					for _, reaction := range ary2 {
						react, _ := reaction.(map[string]interface{})
						user, ok := Dig(react, []string{"user_data"}, false, true)
						if ok && user != nil {
							identities[j.IdentityForObject(ctx, user.(map[string]interface{}))] = struct{}{}
						}
					}
				}
			}
		}
		reviews, ok := Dig(item, []string{"reviews_data"}, false, true)
		if ok && reviews != nil {
			ary, _ := reviews.([]interface{})
			for _, review := range ary {
				rev, _ := review.(map[string]interface{})
				user, ok := Dig(rev, []string{"user_data"}, false, true)
				if ok && user != nil {
					identities[j.IdentityForObject(ctx, user.(map[string]interface{}))] = struct{}{}
				}
			}
		}
		// We don't process commits_data - we only hold array of commits SHAs here
		// Code to process this is commented out because p2o is not doing this neither
	}
	return
}

// GitHubEnrichItemsFunc - iterate items and enrich them
// items is a current pack of input items
// docs is a pointer to where extracted identities will be stored
func GitHubEnrichItemsFunc(ctx *Ctx, ds DS, thrN int, items []interface{}, docs *[]interface{}) (err error) {
	j, _ := ds.(*DSGitHub)
	switch j.Category {
	case "repository":
		return j.GitHubRepositoryEnrichItemsFunc(ctx, thrN, items, docs)
	case "issue":
		return j.GitHubIssueEnrichItemsFunc(ctx, thrN, items, docs)
	case "pull_request":
		return j.GitHubPullRequestEnrichItemsFunc(ctx, thrN, items, docs)
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

// GitHubIssueEnrichItemsFunc - iterate items and enrich them
// items is a current pack of input items
// docs is a pointer to where extracted identities will be stored
func (j *DSGitHub) GitHubIssueEnrichItemsFunc(ctx *Ctx, thrN int, items []interface{}, docs *[]interface{}) (err error) {
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
	getRichItems := func(doc map[string]interface{}) (richItems []interface{}, e error) {
		var rich map[string]interface{}
		rich, e = j.EnrichItem(ctx, doc, "", dbConfigured, nil)
		if e != nil {
			return
		}
		richItems = append(richItems, rich)
		data, _ := Dig(doc, []string{"data"}, true, false)
		// issue: assignees_data[]
		// issue: comments_data[].user_data
		// issue: comments_data[].reactions_data[].user_data
		// issue: reactions_data[].user_data
		iComments, ok := Dig(data, []string{"comments_data"}, false, true)
		if ok && iComments != nil {
			comments, ok := iComments.([]interface{})
			if ok {
				var comms []map[string]interface{}
				for _, iComment := range comments {
					comment, ok := iComment.(map[string]interface{})
					if !ok {
						continue
					}
					comms = append(comms, comment)
				}
				if len(comms) > 0 {
					var riches []interface{}
					riches, e = j.EnrichIssueComments(ctx, rich, comms, dbConfigured)
					if e != nil {
						return
					}
					richItems = append(richItems, riches...)
				}
			}
		}
		// Possibly enrich assignees items
		// Possibly enrich reactions items (on issue and maybe all its sub-comments)
		return
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
		richItems, e := getRichItems(doc)
		if e != nil {
			return
		}
		for _, rich := range richItems {
			e = EnrichItem(ctx, j, rich.(map[string]interface{}))
			if e != nil {
				return
			}
		}
		if thrN > 1 {
			mtx.Lock()
		}
		*docs = append(*docs, richItems...)
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

// GitHubPullRequestEnrichItemsFunc - iterate items and enrich them
// items is a current pack of input items
// docs is a pointer to where extracted identities will be stored
func (j *DSGitHub) GitHubPullRequestEnrichItemsFunc(ctx *Ctx, thrN int, items []interface{}, docs *[]interface{}) (err error) {
	if ctx.Debug > 0 {
		Printf("github enrich pull request items %d/%d func\n", len(items), len(*docs))
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
	getRichItems := func(doc map[string]interface{}) (richItems []interface{}, e error) {
		var rich map[string]interface{}
		rich, e = j.EnrichItem(ctx, doc, "", dbConfigured, nil)
		if e != nil {
			return
		}
		richItems = append(richItems, rich)
		data, _ := Dig(doc, []string{"data"}, true, false)
		// pr:    assignees_data[]
		// pr:    reviews_data[].user_data
		// pr:    review_comments_data[].user_data
		// pr:    review_comments_data[].reactions_data[].user_data
		// pr:    requested_reviewers_data[].user_data
		iReviews, ok := Dig(data, []string{"reviews_data"}, false, true)
		if ok && iReviews != nil {
			reviews, ok := iReviews.([]interface{})
			if ok {
				var revs []map[string]interface{}
				for _, iReview := range reviews {
					review, ok := iReview.(map[string]interface{})
					if !ok {
						continue
					}
					revs = append(revs, review)
				}
				if len(revs) > 0 {
					var riches []interface{}
					riches, e = j.EnrichPullRequestReviews(ctx, rich, revs, dbConfigured)
					if e != nil {
						return
					}
					richItems = append(richItems, riches...)
				}
			}
		}
		iComments, ok := Dig(data, []string{"review_comments_data"}, false, true)
		if ok && iComments != nil {
			comments, ok := iComments.([]interface{})
			if ok {
				var comms []map[string]interface{}
				for _, iComment := range comments {
					comment, ok := iComment.(map[string]interface{})
					if !ok {
						continue
					}
					comms = append(comms, comment)
				}
				if len(comms) > 0 {
					var riches []interface{}
					riches, e = j.EnrichPullRequestComments(ctx, rich, comms, dbConfigured)
					if e != nil {
						return
					}
					richItems = append(richItems, riches...)
				}
			}
		}
		// Possibly enrich assignees items
		// Possibly enrich reactions items (on PR sub comments)
		// Possibly enrich requested reviewers data
		return
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
		richItems, e := getRichItems(doc)
		if e != nil {
			return
		}
		for _, rich := range richItems {
			e = EnrichItem(ctx, j, rich.(map[string]interface{}))
			if e != nil {
				return
			}
		}
		if thrN > 1 {
			mtx.Lock()
		}
		*docs = append(*docs, richItems...)
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
	case "issue":
		return j.EnrichIssueItem(ctx, item, author, affs, extra)
	case "pull_request":
		return j.EnrichPullRequestItem(ctx, item, author, affs, extra)
	default:
		err = fmt.Errorf("EnrichItem: unknown category %s", j.Category)
	}
	return
}

// EnrichIssueComments - return rich comments from raw issue
func (j *DSGitHub) EnrichIssueComments(ctx *Ctx, issue map[string]interface{}, comments []map[string]interface{}, affs bool) (richItems []interface{}, err error) {
	// xxx
	return
}

// EnrichPullRequestComments - return rich comments from raw issue
func (j *DSGitHub) EnrichPullRequestComments(ctx *Ctx, issue map[string]interface{}, comments []map[string]interface{}, affs bool) (richItems []interface{}, err error) {
	// xxx
	return
}

// EnrichPullRequestReviews - return rich comments from raw issue
func (j *DSGitHub) EnrichPullRequestReviews(ctx *Ctx, issue map[string]interface{}, comments []map[string]interface{}, affs bool) (richItems []interface{}, err error) {
	// xxx
	return
}

// EnrichIssueItem - return rich item from raw item for a given author type
func (j *DSGitHub) EnrichIssueItem(ctx *Ctx, item map[string]interface{}, author string, affs bool, extra interface{}) (rich map[string]interface{}, err error) {
	rich = make(map[string]interface{})
	issue, ok := item["data"].(map[string]interface{})
	if !ok {
		err = fmt.Errorf("missing data field in item %+v", DumpKeys(item))
		return
	}
	for _, field := range RawFields {
		v, _ := item[field]
		rich[field] = v
	}
	rich["repo_name"] = j.URL
	rich["issue_id"], _ = issue["id"]
	updatedOn, _ := Dig(item, []string{j.DateField(ctx)}, true, false)
	for prop, value := range CommonFields(j, updatedOn, j.Category) {
		rich[prop] = value
	}
	// xxx
	rich["type"] = "issue"
	rich["category"] = "issue"
	return
}

// EnrichPullRequestItem - return rich item from raw item for a given author type
func (j *DSGitHub) EnrichPullRequestItem(ctx *Ctx, item map[string]interface{}, author string, affs bool, extra interface{}) (rich map[string]interface{}, err error) {
	rich = make(map[string]interface{})
	pull, ok := item["data"].(map[string]interface{})
	if !ok {
		err = fmt.Errorf("missing data field in item %+v", DumpKeys(item))
		return
	}
	for _, field := range RawFields {
		v, _ := item[field]
		rich[field] = v
	}
	rich["repo_name"] = j.URL
	rich["pr_id"], _ = pull["id"]
	updatedOn, _ := Dig(item, []string{j.DateField(ctx)}, true, false)
	for prop, value := range CommonFields(j, updatedOn, j.Category) {
		rich[prop] = value
	}
	// xxx
	rich["type"] = "pull_request"
	rich["category"] = "pull_request"
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
	for prop, value := range CommonFields(j, updatedOn, j.Category) {
		rich[prop] = value
	}
	rich["type"] = "repository"
	rich["category"] = "repository"
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
func (j *DSGitHub) AllRoles(ctx *Ctx, item map[string]interface{}) (roles []string, static bool) {
	if j.Category == "repository" {
		return
	}
	// IMPL:
	// This will depend on github documents types
	return []string{Author}, true
}

// CalculateTimeToReset - calculate time to reset rate limits based on rate limit value and rate limit reset value
func (j *DSGitHub) CalculateTimeToReset(ctx *Ctx, rateLimit, rateLimitReset int) (seconds int) {
	seconds = rateLimitReset
	return
}

package dads

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/go-github/v37/github"
	jsoniter "github.com/json-iterator/go"
	"golang.org/x/oauth2"
)

const (
	// GitHubBackendVersion - backend version
	GitHubBackendVersion = "0.1.0"
	// GitHubURLRoot - GitHub URL root
	GitHubURLRoot = "https://github.com/"
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
	// AbuseWaitSeconds - N - wait random(N:2N) seconds if GitHub detected abuse
	// 7 means from 7 to 13 seconds, 10 on average
	AbuseWaitSeconds = 7
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
	// WantEnrichIssueAssignees - do we want to create rich documents for issue assignees (it contains identity data too).
	WantEnrichIssueAssignees = true
	// WantEnrichIssueCommentReactions - do we want to create rich documents for issue comment reactions (it contains identity data too).
	WantEnrichIssueCommentReactions = true
	// WantEnrichIssueReactions - do we want to create rich documents for issue reactions (it contains identity data too).
	WantEnrichIssueReactions = true
	// WantEnrichPullRequestAssignees - do we want to create rich documents for pull request assignees (it contains identity data too).
	WantEnrichPullRequestAssignees = true
	// WantEnrichPullRequestCommentReactions - do we want to create rich documents for pull request comment reactions (it contains identity data too).
	WantEnrichPullRequestCommentReactions = true
	// WantEnrichPullRequestRequestedReviewers - do we want to create rich documents for pull request requested reviewers (it contains identity data too).
	WantEnrichPullRequestRequestedReviewers = true
)

var (
	// GitHubRawMapping - GitHub raw index mapping
	// GitHubRawMapping = []byte(`{"properties":{"metadata__updated_on":{"type":"date"}}}`)
	GitHubRawMapping = []byte(`{"dynamic":true,"properties":{"metadata__updated_on":{"type":"date","format":"strict_date_optional_time||epoch_millis"},"data":{"properties":{"comments_data":{"dynamic":false,"properties":{"body":{"type":"text","index":true}}},"review_comments_data":{"dynamic":false,"properties":{"body":{"type":"text","index":true},"diff_hunk":{"type":"text","index":true}}},"reviews_data":{"dynamic":false,"properties":{"body":{"type":"text","index":true}}},"body":{"type":"text","index":true}}}}}`)
	// GitHubRichMapping - GitHub rich index mapping
	// GitHubRichMapping = []byte(`{"properties":{"metadata__updated_on":{"type":"date"},"merge_author_geolocation":{"type":"geo_point"},"assignee_geolocation":{"type":"geo_point"},"state":{"type":"keyword"},"user_geolocation":{"type":"geo_point"},"title_analyzed":{"type":"text","index":true}}}`)
	GitHubRichMapping = []byte(`{"dynamic":true,"properties":{"metadata__updated_on":{"type":"date","format":"strict_date_optional_time||epoch_millis"},"merge_author_geolocation":{"type":"geo_point"},"assignee_geolocation":{"type":"geo_point"},"state":{"type":"keyword"},"user_geolocation":{"type":"geo_point"},"title_analyzed":{"type":"text","index":true},"body_analyzed":{"type":"text","index":true}},"dynamic_templates":[{"notanalyzed":{"match":"*","unmatch":"body","match_mapping_type":"string","mapping":{"type":"keyword"}}},{"formatdate":{"match":"*","match_mapping_type":"date","mapping":{"format":"strict_date_optional_time||epoch_millis","type":"date"}}}]}`)
	// GitHubCategories - categories defined for GitHub
	GitHubCategories = map[string]struct{}{"issue": {}, "pull_request": {}, "repository": {}}
	// GitHubIssueRoles - roles to fetch affiliation data for github issue
	GitHubIssueRoles = []string{"user_data", "assignee_data"}
	// GitHubIssueCommentRoles - roles to fetch affiliation data for github issue comment
	GitHubIssueCommentRoles = []string{"user_data"}
	// GitHubIssueAssigneeRoles - roles to fetch affiliation data for github issue comment
	GitHubIssueAssigneeRoles = []string{"assignee"}
	// GitHubIssueReactionRoles - roles to fetch affiliation data for github issue reactions or issue comment reactions
	GitHubIssueReactionRoles = []string{"user_data"}
	// GitHubPullRequestRoles - roles to fetch affiliation data for github pull request
	GitHubPullRequestRoles = []string{"user_data", "assignee_data", "merged_by_data"}
	// GitHubPullRequestCommentRoles - roles to fetch affiliation data for github pull request comment
	GitHubPullRequestCommentRoles = []string{"user_data"}
	// GitHubPullRequestAssigneeRoles - roles to fetch affiliation data for github pull request comment
	GitHubPullRequestAssigneeRoles = []string{"assignee"}
	// GitHubPullRequestReactionRoles - roles to fetch affiliation data for github pull request comment reactions
	GitHubPullRequestReactionRoles = []string{"user_data"}
	// GitHubPullRequestRequestedReviewerRoles - roles to fetch affiliation data for github pull request requested reviewer
	GitHubPullRequestRequestedReviewerRoles = []string{"requested_reviewer"}
	// GitHubPullRequestReviewRoles - roles to fetch affiliation data for github pull request comment
	GitHubPullRequestReviewRoles = []string{"user_data"}
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
	RateHandled                     bool
	CanCache                        bool
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
	GitHubRateMtx                   *sync.RWMutex
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
				Printf("Parsed wait time from api non-success response message: %v: %s\n", rem, err.Error())
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
	if j.GitHubRateMtx != nil {
		j.GitHubRateMtx.RLock()
	}
	handled := j.RateHandled
	if handled {
		aHint = j.Hint
		canCache = j.CanCache
	}
	if j.GitHubRateMtx != nil {
		j.GitHubRateMtx.RUnlock()
	}
	if handled {
		Printf("%s/%s: rate is already handled elsewhere, returning #%d token\n", j.URL, j.Category, aHint)
		return
	}
	if j.GitHubRateMtx != nil {
		j.GitHubRateMtx.Lock()
		defer j.GitHubRateMtx.Unlock()
	}
	h, _, rem, wait := j.getRateLimits(j.Context, ctx, j.Clients, true)
	for {
		if ctx.Debug > 1 {
			Printf("Checking token %d %+v %+v\n", h, rem, wait)
		}
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
	j.CanCache = canCache
	if ctx.Debug > 1 {
		Printf("Found usable token %d/%d/%v, cache enabled: %v\n", aHint, rem[h], wait[h], canCache)
	}
	j.RateHandled = true
	Printf("%s/%s: selected new token #%d\n", j.URL, j.Category, j.Hint)
	return
}

func (j *DSGitHub) isAbuse(e error) (abuse, rateLimit bool) {
	if e == nil {
		return
	}
	defer func() {
		// if abuse || rateLimit {
		// Clear rate handled flag on every error - chances are that next rate handle will recover
		Printf("%s/%s: GitHub error: abuse:%v, rate limit:%v\n", j.URL, j.Category, abuse, rateLimit)
		if e != nil {
			if j.GitHubRateMtx != nil {
				j.GitHubRateMtx.Lock()
			}
			j.RateHandled = false
			if j.GitHubRateMtx != nil {
				j.GitHubRateMtx.Unlock()
			}
		}
	}()
	errStr := e.Error()
	// GitHub can return '401 Bad credentials' when token(s) was/were revoken
	// abuse = strings.Contains(errStr, "403 You have triggered an abuse detection mechanism") || strings.Contains(errStr, "401 Bad credentials")
	abuse = strings.Contains(errStr, "403 You have triggered an abuse detection mechanism")
	rateLimit = strings.Contains(errStr, "403 API rate limit")
	return
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
			if ctx.Debug > 2 {
				Printf("repos found in cache: %+v\n", repoData)
			}
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
		if ctx.Debug > 2 {
			Printf("GET %s/%s -> {%+v, %+v, %+v}\n", org, repo, rep, response, e)
		}
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
			Printf("Unable to get %s repo: response: %+v, because: %+v, retrying rate\n", origin, response, e)
			Printf("githubRepos: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				Printf("GitHub detected abuse (get repo %s), waiting for %ds\n", origin, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				Printf("Rate limit reached on a token (get repo %s) waiting 1s before token switch\n", origin)
				time.Sleep(time.Duration(1) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse && !rateLimit {
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
		if ctx.Debug > 2 {
			Printf("repos got from API: %+v\n", repoData)
		}
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
			if ctx.Debug > 1 {
				Printf("user found in memory cache: %+v\n", user)
			}
			return
		}
		// Try file cache 2nd
		if CacheGitHubUserFiles {
			path := j.CacheDir + login + ".json"
			lockPath := path + ".lock"
			file, e := os.Stat(path)
			if e == nil {
				for {
					waited := 0
					_, e := os.Stat(lockPath)
					if e == nil {
						if ctx.Debug > 0 {
							Printf("user %s lock file %s present, waiting 1s\n", user, lockPath)
						}
						time.Sleep(time.Duration(1) * time.Second)
						waited++
						continue
					}
					if waited > 0 {
						if ctx.Debug > 0 {
							Printf("user %s lock file %s was present, waited %ds\n", user, lockPath, waited)
						}
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
								if ctx.Debug > 1 {
									Printf("user found in files cache: %+v\n", user)
								}
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
				if ctx.Debug > 1 {
					Printf("githubUser: no %s user cache file: %v\n", path, e)
				}
			}
			locked := false
			lockFile, e := os.Create(lockPath)
			if e != nil {
				Printf("githubUser: create %s lock file failed: %v\n", lockPath, e)
			} else {
				locked = true
				_ = lockFile.Close()
			}
			defer func() {
				if locked {
					defer func() {
						if ctx.Debug > 1 {
							Printf("remove lock file %s\n", lockPath)
						}
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
					Printf("githubUser: cannot write file %s, %d bytes: %v\n", path, len(bts), err)
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
		if ctx.Debug > 2 {
			Printf("GET %s -> {%+v, %+v, %+v}\n", login, usr, response, e)
		}
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if CacheGitHubUser {
				if j.GitHubUserMtx != nil {
					j.GitHubUserMtx.Lock()
				}
				if ctx.Debug > 0 {
					Printf("user not found using API: %s\n", login)
				}
				j.GitHubUser[login] = map[string]interface{}{}
				if j.GitHubUserMtx != nil {
					j.GitHubUserMtx.Unlock()
				}
			}
			return
		}
		if e != nil && !retry {
			Printf("Unable to get %s user: response: %+v, because: %+v, retrying rate\n", login, response, e)
			Printf("githubUser: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				Printf("GitHub detected abuse (get user %s), waiting for %ds\n", login, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				Printf("Rate limit reached on a token (get user %s) waiting 1s before token switch\n", login)
				time.Sleep(time.Duration(1) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse && !rateLimit {
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
			if ctx.Debug > 1 {
				Printf("user found using API: %+v\n", user)
			}
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
			if ctx.Debug > 2 {
				Printf("issues found in cache: %+v\n", issuesData)
			}
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
		if ctx.Debug > 2 {
			Printf("GET %s/%s -> {%+v, %+v, %+v}\n", org, repo, issues, response, e)
		}
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
			Printf("Unable to get %s issues: response: %+v, because: %+v, retrying rate\n", origin, response, e)
			Printf("githubIssues: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				Printf("GitHub detected abuse (get issues %s), waiting for %ds\n", origin, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				Printf("Rate limit reached on a token (get issues %s) waiting 1s before token switch\n", origin)
				time.Sleep(time.Duration(1) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse && !rateLimit {
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
			iss["body_analyzed"], _ = iss["body"]
			iss["is_pull"] = issue.IsPullRequest()
			issuesData = append(issuesData, iss)
		}
		if response.NextPage == 0 {
			break
		}
		opt.Page = response.NextPage
		if ctx.Debug > 0 {
			runtime.GC()
			Printf("%s/%s: processing next issues page: %d\n", j.URL, j.Category, opt.Page)
		}
		retry = false
	}
	if ctx.Debug > 2 {
		Printf("issues got from API: %+v\n", issuesData)
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
			if ctx.Debug > 2 {
				Printf("issue comments found in cache: %+v\n", comments)
			}
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
		if ctx.Debug > 2 {
			Printf("GET %s/%s -> {%+v, %+v, %+v}\n", org, repo, comms, response, e)
		}
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
			Printf("Unable to get %s issue comments: response: %+v, because: %+v, retrying rate\n", key, response, e)
			Printf("githubIssueComments: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				Printf("GitHub detected abuse (get issue comments %s), waiting for %ds\n", key, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				Printf("Rate limit reached on a token (get issue comments %s) waiting 1s before token switch\n", key)
				time.Sleep(time.Duration(1) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse && !rateLimit {
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
			com["body_analyzed"], _ = com["body"]
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
		if ctx.Debug > 0 {
			Printf("processing next issue comments page: %d\n", opt.Page)
		}
		retry = false
	}
	if ctx.Debug > 2 {
		Printf("issue comments got from API: %+v\n", comments)
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
	if ctx.Debug > 1 {
		fmt.Printf("githubCommentReactions %s\n", key)
	}
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
			if ctx.Debug > 2 {
				Printf("comment reactions found in cache: %+v\n", reactions)
			}
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
		if ctx.Debug > 2 {
			Printf("GET %s/%s/%d -> {%+v, %+v, %+v}\n", org, repo, cid, reacts, response, e)
		}
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
			Printf("Unable to get %s comment reactions: response: %+v, because: %+v, retrying rate\n", key, response, e)
			Printf("githubCommentReactions: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				Printf("GitHub detected abuse (get comment reactions %s), waiting for %ds\n", key, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				Printf("Rate limit reached on a token (get comment reactions %s) waiting 1s before token switch\n", key)
				time.Sleep(time.Duration(1) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse && !rateLimit {
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
		if ctx.Debug > 0 {
			Printf("processing next comment reactions page: %d\n", opt.Page)
		}
		retry = false
	}
	if ctx.Debug > 2 {
		Printf("comment reactions got from API: %+v\n", reactions)
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
	if ctx.Debug > 1 {
		fmt.Printf("githubIssueReactions %s\n", key)
	}
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
			if ctx.Debug > 2 {
				Printf("issue reactions found in cache: %+v\n", reactions)
			}
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
		if ctx.Debug > 2 {
			Printf("GET %s/%s/%d -> {%+v, %+v, %+v}\n", org, repo, number, reacts, response, e)
		}
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
			Printf("Unable to get %s issue reactions: response: %+v, because: %+v, retrying rate\n", key, response, e)
			Printf("githubIssueReactions: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				Printf("GitHub detected abuse (get issue reactions %s), waiting for %ds\n", key, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				Printf("Rate limit reached on a token (get issue reactions %s) waiting 1s before token switch\n", key)
				time.Sleep(time.Duration(1) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse && !rateLimit {
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
		if ctx.Debug > 0 {
			Printf("processing next issue reactions page: %d\n", opt.Page)
		}
		retry = false
	}
	if ctx.Debug > 2 {
		Printf("issue reactions got from API: %+v\n", reactions)
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
			if ctx.Debug > 2 {
				Printf("pull found in cache: %+v\n", pullData)
			}
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
		if ctx.Debug > 2 {
			Printf("GET %s/%s/%d -> {%+v, %+v, %+v}\n", org, repo, number, pull, response, e)
		}
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
			Printf("Unable to get %s pull: response: %+v, because: %+v, retrying rate\n", key, response, e)
			Printf("githubPulls: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				Printf("GitHub detected abuse (get pull %s), waiting for %ds\n", key, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				Printf("Rate limit reached on a token (get pull %s) waiting 1s before token switch\n", key)
				time.Sleep(time.Duration(1) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse && !rateLimit {
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
		pullData["body_analyzed"], _ = pullData["body"]
		if ctx.Debug > 2 {
			Printf("pull got from API: %+v\n", pullData)
		}
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

// githubPullsFromIssues - consider fetching this data in a stream-like mode to avoid a need of pulling all data and then of everything at once
func (j *DSGitHub) githubPullsFromIssues(ctx *Ctx, org, repo string, since *time.Time) (pullsData []map[string]interface{}, err error) {
	var (
		issues []map[string]interface{}
		pull   map[string]interface{}
		ok     bool
	)
	issues, err = j.githubIssues(ctx, org, repo, ctx.DateFrom)
	if err != nil {
		return
	}
	i, pulls := 0, 0
	nIssues := len(issues)
	Printf("%s/%s: processing %d issues (to filter for PRs)\n", j.URL, j.Category, nIssues)
	if j.ThrN > 1 {
		nThreads := 0
		ch := make(chan interface{})
		for _, issue := range issues {
			i++
			if i%ItemsPerPage == 0 {
				runtime.GC()
				Printf("%s/%s: processing %d/%d issues, %d pulls so far\n", j.URL, j.Category, i, nIssues, pulls)
			}
			isPR, _ := issue["is_pull"]
			if !isPR.(bool) {
				continue
			}
			pulls++
			number, _ := issue["number"]
			go func(ch chan interface{}, num int) {
				pr, e := j.githubPull(ctx, org, repo, num)
				if e != nil {
					ch <- e
					return
				}
				ch <- pr
			}(ch, int(number.(float64)))
			nThreads++
			if nThreads == j.ThrN {
				obj := <-ch
				nThreads--
				err, ok = obj.(error)
				if ok {
					return
				}
				pullsData = append(pullsData, obj.(map[string]interface{}))
			}
		}
		for nThreads > 0 {
			obj := <-ch
			nThreads--
			err, ok = obj.(error)
			if ok {
				return
			}
			pullsData = append(pullsData, obj.(map[string]interface{}))
		}
	} else {
		for _, issue := range issues {
			i++
			if i%ItemsPerPage == 0 {
				runtime.GC()
				Printf("%s/%s: processed %d/%d issues, %d pulls so far\n", j.URL, j.Category, i, nIssues, pulls)
			}
			isPR, _ := issue["is_pull"]
			if !isPR.(bool) {
				continue
			}
			pulls++
			number, _ := issue["number"]
			pull, err = j.githubPull(ctx, org, repo, int(number.(float64)))
			if err != nil {
				return
			}
			pullsData = append(pullsData, pull)
		}
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
			if ctx.Debug > 2 {
				Printf("pulls found in cache: %+v\n", pullsData)
			}
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
		if ctx.Debug > 2 {
			Printf("GET %s/%s -> {%+v, %+v, %+v}\n", org, repo, pulls, response, e)
		}
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
			Printf("Unable to get %s pulls: response: %+v, because: %+v, retrying rate\n", origin, response, e)
			Printf("githubPulls: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				Printf("GitHub detected abuse (get pulls %s), waiting for %ds\n", origin, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				Printf("Rate limit reached on a token (get pulls %s) waiting 1s before token switch\n", origin)
				time.Sleep(time.Duration(1) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse && !rateLimit {
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
			pr["body_analyzed"], _ = pr["body"]
			pullsData = append(pullsData, pr)
		}
		if response.NextPage == 0 {
			break
		}
		opt.Page = response.NextPage
		if ctx.Debug > 0 {
			Printf("%s/%s: processing next pulls page: %d\n", j.URL, j.Category, opt.Page)
		}
		retry = false
	}
	if ctx.Debug > 2 {
		Printf("pulls got from API: %+v\n", pullsData)
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
			if ctx.Debug > 2 {
				Printf("pull reviews found in cache: %+v\n", reviews)
			}
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
		if ctx.Debug > 2 {
			Printf("GET %s/%s/%d -> {%+v, %+v, %+v}\n", org, repo, number, revs, response, e)
		}
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
			Printf("Unable to get %s pull reviews: response: %+v, because: %+v, retrying rate\n", key, response, e)
			Printf("githubPullReviews: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				Printf("GitHub detected abuse (get pull reviews %s), waiting for %ds\n", key, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				Printf("Rate limit reached on a token (get pull reviews %s) waiting 1s before token switch\n", key)
				time.Sleep(time.Duration(1) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse && !rateLimit {
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
			rev["body_analyzed"], _ = rev["body"]
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
		if ctx.Debug > 0 {
			Printf("processing next pull reviews page: %d\n", opt.Page)
		}
		retry = false
	}
	if ctx.Debug > 2 {
		Printf("pull reviews got from API: %+v\n", reviews)
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
			if ctx.Debug > 2 {
				Printf("pull review comments found in cache: %+v\n", reviewComments)
			}
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
		if ctx.Debug > 2 {
			Printf("GET %s/%s/%d -> {%+v, %+v, %+v}\n", org, repo, number, revComms, response, e)
		}
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
			Printf("Unable to get %s pull review comments: response: %+v, because: %+v, retrying rate\n", key, response, e)
			Printf("githubPullReviewComments: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				Printf("GitHub detected abuse (get pull review comments %s), waiting for %ds\n", key, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				Printf("Rate limit reached on a token (get pull review comments %s) waiting 1s before token switch\n", key)
				time.Sleep(time.Duration(1) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse && !rateLimit {
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
			revComm["body_analyzed"], _ = revComm["body"]
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
		if ctx.Debug > 0 {
			Printf("processing next pull review comments page: %d\n", opt.Page)
		}
		retry = false
	}
	if ctx.Debug > 2 {
		Printf("pull review comments got from API: %+v\n", reviewComments)
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
	if ctx.Debug > 1 {
		fmt.Printf("githubReviewCommentReactions %s\n", key)
	}
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
			if ctx.Debug > 2 {
				Printf("comment reactions found in cache: %+v\n", reactions)
			}
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
		if ctx.Debug > 2 {
			Printf("GET %s/%s/%d -> {%+v, %+v, %+v}\n", org, repo, cid, reacts, response, e)
		}
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
			Printf("Unable to get %s comment reactions: response: %+v, because: %+v, retrying rate\n", key, response, e)
			Printf("githubReviewCommentReactions: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				Printf("GitHub detected abuse (get pull comment reactions %s), waiting for %ds\n", key, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				Printf("Rate limit reached on a token (get pull comment reactions %s) waiting 1s before token switch\n", key)
				time.Sleep(time.Duration(1) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse && !rateLimit {
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
		if ctx.Debug > 0 {
			Printf("processing next pull review comment reactions page: %d\n", opt.Page)
		}
		retry = false
	}
	if ctx.Debug > 2 {
		Printf("review comment reactions got from API: %+v\n", reactions)
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
			if ctx.Debug > 2 {
				Printf("pull requested reviewers found in cache: %+v\n", reviewers)
			}
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
		if ctx.Debug > 2 {
			Printf("GET %s/%s/%d -> {%+v, %+v, %+v}\n", org, repo, number, revsObj, response, e)
		}
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
			Printf("Unable to get %s pull requested reviewers: response: %+v, because: %+v, retrying rate\n", key, response, e)
			Printf("githubPullRequestedReviewers: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				Printf("GitHub detected abuse (get pull requested reviewers %s), waiting for %ds\n", key, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				Printf("Rate limit reached on a token (get pull requested reviewers %s) waiting 1s before token switch\n", key)
				time.Sleep(time.Duration(1) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse && !rateLimit {
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
			if reviewer == nil || reviewer.Login == nil {
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
		if ctx.Debug > 0 {
			Printf("processing next pull requested reviewers page: %d\n", opt.Page)
		}
		retry = false
	}
	if ctx.Debug > 2 {
		Printf("pull requested reviewers got from API: %+v\n", reviewers)
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
			if ctx.Debug > 2 {
				Printf("pull commits found in cache: %+v\n", commits)
			}
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
		if ctx.Debug > 2 {
			Printf("GET %s/%s/%d -> {%+v, %+v, %+v}\n", org, repo, number, comms, response, e)
		}
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
			Printf("Unable to get %s pull commits: response: %+v, because: %+v, retrying rate\n", key, response, e)
			Printf("githubPullCommits: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				Printf("GitHub detected abuse (get pull commits %s), waiting for %ds\n", key, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				Printf("Rate limit reached on a token (get pull commits %s) waiting 1s before token switch\n", key)
				time.Sleep(time.Duration(1) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse && !rateLimit {
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
		if ctx.Debug > 0 {
			Printf("processing next pull commits page: %d\n", opt.Page)
		}
		retry = false
	}
	if ctx.Debug > 2 {
		Printf("pull commits got from API: %+v\n", commits)
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
			if ctx.Debug > 2 {
				Printf("user orgs found in cache: %+v\n", orgsData)
			}
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
		if ctx.Debug > 2 {
			Printf("GET %s -> {%+v, %+v, %+v}\n", login, organizations, response, e)
		}
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
			Printf("Unable to get %s user orgs: response: %+v, because: %+v, retrying rate\n", login, response, e)
			Printf("githubUserOrgs: handle rate\n")
			abuse, rateLimit := j.isAbuse(e)
			if abuse {
				sleepFor := AbuseWaitSeconds + rand.Intn(AbuseWaitSeconds)
				Printf("GitHub detected abuse (get user orgs %s), waiting for %ds\n", login, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if rateLimit {
				Printf("Rate limit reached on a token (get user orgs %s) waiting 1s before token switch\n", login)
				time.Sleep(time.Duration(1) * time.Second)
			}
			if j.GitHubMtx != nil {
				j.GitHubMtx.Lock()
			}
			j.Hint, _ = j.handleRate(ctx)
			c = j.Clients[j.Hint]
			if j.GitHubMtx != nil {
				j.GitHubMtx.Unlock()
			}
			if !abuse && !rateLimit {
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
		if ctx.Debug > 0 {
			Printf("processing next user orgs page: %d\n", opt.Page)
		}
		retry = false
	}
	if ctx.Debug > 2 {
		Printf("user orgs got from API: %+v\n", orgsData)
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
	if strings.HasSuffix(j.Repo, ".git") {
		lRepo := len(j.Repo)
		j.Repo = j.Repo[:lRepo-4]
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
		j.GitHubRateMtx = &sync.RWMutex{}
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
	if item == nil {
		Fatalf("there is no such repo %s/%s", j.Org, j.Repo)
	}
	item["fetched_on"] = fmt.Sprintf("%.6f", float64(time.Now().UnixNano())/1.0e9)
	esItem := j.AddMetadata(ctx, item)
	if ctx.Project != "" {
		item["project"] = ctx.Project
	}
	esItem["data"] = item
	items = append(items, esItem)
	err = SendToElastic(ctx, j, true, UUID, items)
	if err != nil {
		Printf("%s/%s: Error %v sending %d messages to ES\n", j.URL, j.Category, err, len(items))
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
	pull["assignee_data"] = map[string]interface{}{}
	pull["merged_by_data"] = map[string]interface{}{}
	pull["review_comments_data"] = []interface{}{}
	pull["assignees_data"] = []interface{}{}
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
		// TODO: commits
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
	nThreads, nIss, issProcessed := 0, 0, 0
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
		if issProcessed%ItemsPerPage == 0 {
			Printf("%s/%s: processed %d/%d issues\n", j.URL, j.Category, issProcessed, nIss)
		}
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
					Printf("%s/%s: error %v sending %d issues to ElasticSearch\n", j.URL, j.Category, ee, len(allIssues))
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
	runtime.GC()
	nIss = len(issues)
	Printf("%s/%s: got %d issues\n", j.URL, j.Category, nIss)
	if j.ThrN > 1 {
		for _, issue := range issues {
			go func(iss map[string]interface{}) {
				var (
					e    error
					esch chan error
				)
				esch, e = processIssue(ch, iss)
				if e != nil {
					Printf("%s/%s: issues process error: %v\n", j.URL, j.Category, e)
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
				issProcessed++
				nThreads--
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				return
			}
			issProcessed++
		}
	} else {
		for _, issue := range issues {
			_, err = processIssue(nil, issue)
			if err != nil {
				return
			}
			issProcessed++
		}
	}
	if eschaMtx != nil {
		eschaMtx.Lock()
	}
	for _, esch := range escha {
		err = <-esch
		if err != nil {
			if eschaMtx != nil {
				eschaMtx.Unlock()
			}
			return
		}
	}
	if eschaMtx != nil {
		eschaMtx.Unlock()
	}
	nIssues := len(allIssues)
	if ctx.Debug > 0 {
		Printf("%d remaining issues to send to ES\n", nIssues)
	}
	if nIssues > 0 {
		err = SendToElastic(ctx, j, true, UUID, allIssues)
		if err != nil {
			Printf("%s/%s: error %v sending %d issues to ES\n", j.URL, j.Category, err, len(allIssues))
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
	nThreads, pullsProcessed, nPRs := 0, 0, 0
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
		if pullsProcessed%ItemsPerPage == 0 {
			Printf("%s/%s: processed %d/%d pulls\n", j.URL, j.Category, pullsProcessed, nPRs)
		}
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
					Printf("%s/%s: error %v sending %d pulls to ElasticSearch\n", j.URL, j.Category, ee, len(allPulls))
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
	// PullRequests.List doesn't return merged_by data, we need to use PullRequests.Get on each pull
	// If it would we could use Pulls API to fetch all pulls when no date from is specified
	// If there is a date from Pulls API doesn't support Since parameter
	// if ctx.DateFrom != nil {
	if 1 == 1 {
		pulls, err = j.githubPullsFromIssues(ctx, j.Org, j.Repo, ctx.DateFrom)
	} else {
		pulls, err = j.githubPulls(ctx, j.Org, j.Repo)
	}
	FatalOnError(err)
	runtime.GC()
	nPRs = len(pulls)
	Printf("%s/%s: got %d pulls\n", j.URL, j.Category, nPRs)
	if j.ThrN > 1 {
		for _, pull := range pulls {
			go func(pr map[string]interface{}) {
				var (
					e    error
					esch chan error
				)
				esch, e = processPull(ch, pr)
				if e != nil {
					Printf("%s/%s: pulls process error: %v\n", j.URL, j.Category, e)
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
				pullsProcessed++
				nThreads--
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				return
			}
			pullsProcessed++
		}
	} else {
		for _, pull := range pulls {
			_, err = processPull(nil, pull)
			if err != nil {
				return
			}
			pullsProcessed++
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
			Printf("%s/%s: error %v sending %d pulls to ES\n", j.URL, j.Category, err, len(allPulls))
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
	if j.Category == "repository" {
		return UUID
	}
	return DefaultIDField
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
	number, ok := item.(map[string]interface{})["number"]
	if !ok {
		Fatalf("%s: ItemID() - cannot extract number from %+v", j.DS, DumpKeys(item))
	}
	return fmt.Sprintf("%s/%s/%s/%d", j.Org, j.Repo, j.Category, int(number.(float64)))
	/*
		id, ok := item.(map[string]interface{})["id"]
		if !ok {
			Fatalf("%s: ItemID() - cannot extract id from %+v", j.DS, DumpKeys(item))
		}
		return fmt.Sprintf("%s/%d", j.Category, int64(id.(float64)))
	*/
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
			Printf("%s/%s: IdentityForObject: %+v -> %+v\n", j.URL, j.Category, item, identity)
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
		user, ok := Dig(item, []string{"user_data"}, false, true)
		if ok && user != nil && len(user.(map[string]interface{})) > 0 {
			identities[j.IdentityForObject(ctx, user.(map[string]interface{}))] = struct{}{}
		}
		/*
			user, _ := Dig(item, []string{"user_data"}, false, true)
			if user == nil {
				if ctx.Debug > 1 {
					fmt.Printf("missing user_data property in an issue item %+v\n", DumpPreview(item, 64))
				}
				return
			}
		*/
		assignee, ok := Dig(item, []string{"assignee_data"}, false, true)
		if ok && assignee != nil && len(assignee.(map[string]interface{})) > 0 {
			identities[j.IdentityForObject(ctx, assignee.(map[string]interface{}))] = struct{}{}
		}
		assignees, ok := Dig(item, []string{"assignees_data"}, false, true)
		if ok && assignees != nil {
			ary, _ := assignees.([]interface{})
			for _, assignee := range ary {
				if assignee != nil && len(assignee.(map[string]interface{})) > 0 {
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
				if ok && user != nil && len(user.(map[string]interface{})) > 0 {
					identities[j.IdentityForObject(ctx, user.(map[string]interface{}))] = struct{}{}
				}
				reactions, ok2 := Dig(comm, []string{"reactions_data"}, false, true)
				if ok2 && reactions != nil {
					ary2, _ := reactions.([]interface{})
					for _, reaction := range ary2 {
						react, _ := reaction.(map[string]interface{})
						user, ok := Dig(react, []string{"user_data"}, false, true)
						if ok && user != nil && len(user.(map[string]interface{})) > 0 {
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
				if ok && user != nil && len(user.(map[string]interface{})) > 0 {
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
		user, ok := Dig(item, []string{"user_data"}, false, true)
		if ok && user != nil && len(user.(map[string]interface{})) > 0 {
			identities[j.IdentityForObject(ctx, user.(map[string]interface{}))] = struct{}{}
		}
		/*
			user, _ := Dig(item, []string{"user_data"}, false, true)
			if user == nil {
				if ctx.Debug > 1 {
					fmt.Printf("missing user_data property in a pull request item %+v\n", DumpPreview(item, 64))
				}
				return
			}
		*/
		mergedBy, ok := Dig(item, []string{"merged_by_data"}, false, true)
		if ok && mergedBy != nil && len(mergedBy.(map[string]interface{})) > 0 {
			identities[j.IdentityForObject(ctx, mergedBy.(map[string]interface{}))] = struct{}{}
		}
		assignee, ok := Dig(item, []string{"assignee_data"}, false, true)
		if ok && assignee != nil && len(assignee.(map[string]interface{})) > 0 {
			identities[j.IdentityForObject(ctx, assignee.(map[string]interface{}))] = struct{}{}
		}
		assignees, ok := Dig(item, []string{"assignees_data"}, false, true)
		if ok && assignees != nil {
			ary, _ := assignees.([]interface{})
			for _, assignee := range ary {
				if assignee != nil && len(assignee.(map[string]interface{})) > 0 {
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
				if ok && user != nil && len(user.(map[string]interface{})) > 0 {
					identities[j.IdentityForObject(ctx, user.(map[string]interface{}))] = struct{}{}
				}
				reactions, ok2 := Dig(comm, []string{"reactions_data"}, false, true)
				if ok2 && reactions != nil {
					ary2, _ := reactions.([]interface{})
					for _, reaction := range ary2 {
						react, _ := reaction.(map[string]interface{})
						user, ok := Dig(react, []string{"user_data"}, false, true)
						if ok && user != nil && len(user.(map[string]interface{})) > 0 {
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
				if ok && user != nil && len(user.(map[string]interface{})) > 0 {
					identities[j.IdentityForObject(ctx, user.(map[string]interface{}))] = struct{}{}
				}
			}
		}
		// TODO: commits
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
		Printf("%s/%s: github enrich repository items %d/%d func\n", j.URL, j.Category, len(items), len(*docs))
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
		Printf("%s/%s: github enrich issue items %d/%d func\n", j.URL, j.Category, len(items), len(*docs))
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
					if WantEnrichIssueCommentReactions {
						var reacts []map[string]interface{}
						for _, comment := range comms {
							iReactions, ok := Dig(comment, []string{"reactions_data"}, false, true)
							if ok && iReactions != nil {
								reactions, ok := iReactions.([]interface{})
								if ok {
									for _, iReaction := range reactions {
										reaction, ok := iReaction.(map[string]interface{})
										if !ok {
											continue
										}
										// Store parent comment (not present in issue)
										reaction["parent"] = comment
										reacts = append(reacts, reaction)
									}
								}
							}
						}
						if len(reacts) > 0 {
							var riches []interface{}
							riches, e = j.EnrichIssueReactions(ctx, rich, reacts, dbConfigured)
							if e != nil {
								return
							}
							richItems = append(richItems, riches...)
						}
					}
				}
			}
		}
		if WantEnrichIssueAssignees {
			iAssignees, ok := Dig(data, []string{"assignees_data"}, false, true)
			if ok && iAssignees != nil {
				assignees, ok := iAssignees.([]interface{})
				if ok {
					var asgs []map[string]interface{}
					for _, iAssignee := range assignees {
						assignee, ok := iAssignee.(map[string]interface{})
						if !ok {
							continue
						}
						asgs = append(asgs, assignee)
					}
					if len(asgs) > 0 {
						var riches []interface{}
						riches, e = j.EnrichIssueAssignees(ctx, rich, asgs, dbConfigured)
						if e != nil {
							return
						}
						richItems = append(richItems, riches...)
					}
				}
			}
		}
		if WantEnrichIssueReactions {
			iReactions, ok := Dig(data, []string{"reactions_data"}, false, true)
			if ok && iReactions != nil {
				reactions, ok := iReactions.([]interface{})
				if ok {
					var reacts []map[string]interface{}
					for _, iReaction := range reactions {
						reaction, ok := iReaction.(map[string]interface{})
						if !ok {
							continue
						}
						reacts = append(reacts, reaction)
					}
					if len(reacts) > 0 {
						var riches []interface{}
						riches, e = j.EnrichIssueReactions(ctx, rich, reacts, dbConfigured)
						if e != nil {
							return
						}
						richItems = append(richItems, riches...)
					}
				}
			}
		}
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
		Printf("%s/%s: github enrich pull request items %d/%d func\n", j.URL, j.Category, len(items), len(*docs))
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
		if WantEnrichPullRequestAssignees {
			iAssignees, ok := Dig(data, []string{"assignees_data"}, false, true)
			if ok && iAssignees != nil {
				assignees, ok := iAssignees.([]interface{})
				if ok {
					var asgs []map[string]interface{}
					for _, iAssignee := range assignees {
						assignee, ok := iAssignee.(map[string]interface{})
						if !ok {
							continue
						}
						asgs = append(asgs, assignee)
					}
					if len(asgs) > 0 {
						var riches []interface{}
						riches, e = j.EnrichPullRequestAssignees(ctx, rich, asgs, dbConfigured)
						if e != nil {
							return
						}
						richItems = append(richItems, riches...)
					}
				}
			}
		}
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
					if WantEnrichPullRequestCommentReactions {
						var reacts []map[string]interface{}
						for _, comment := range comms {
							iReactions, ok := Dig(comment, []string{"reactions_data"}, false, true)
							if ok && iReactions != nil {
								reactions, ok := iReactions.([]interface{})
								if ok {
									for _, iReaction := range reactions {
										reaction, ok := iReaction.(map[string]interface{})
										if !ok {
											continue
										}
										reaction["parent"] = comment
										reacts = append(reacts, reaction)
									}
								}
							}
						}
						if len(reacts) > 0 {
							var riches []interface{}
							riches, e = j.EnrichPullRequestReactions(ctx, rich, reacts, dbConfigured)
							if e != nil {
								return
							}
							richItems = append(richItems, riches...)
						}
					}
				}
			}
		}
		if WantEnrichPullRequestRequestedReviewers {
			iReviewers, ok := Dig(data, []string{"requested_reviewers_data"}, false, true)
			if ok && iReviewers != nil {
				reviewers, ok := iReviewers.([]interface{})
				if ok {
					var revs []map[string]interface{}
					for _, iReviewer := range reviewers {
						reviewer, ok := iReviewer.(map[string]interface{})
						if !ok {
							continue
						}
						revs = append(revs, reviewer)
					}
					if len(revs) > 0 {
						var riches []interface{}
						riches, e = j.EnrichPullRequestRequestedReviewers(ctx, rich, revs, dbConfigured)
						if e != nil {
							return
						}
						richItems = append(richItems, riches...)
					}
				}
			}
		}
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
	Printf("%s/%s: enriching items\n", j.URL, j.Category)
	err = ForEachESItem(ctx, j, true, ESBulkUploadFunc, GitHubEnrichItemsFunc, nil, true)
	Printf("%s/%s: enriched items\n", j.URL, j.Category)
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
	// type: category, type(_), item_type( ), issue_comment=true
	// copy issue: github_repo, repo_name, repository
	// copy comment: created_at, updated_at, body, body_analyzed, author_association, url, html_url
	// identify: id, id_in_repo, issue_comment_id, url_id
	// standard: metadata..., origin, project, project_slug, uuid
	// parent: issue_id, issue_number
	// calc: n_reactions
	// identity: author_... -> commenter_...,
	// common: is_github_issue=1, is_github_issue_comment=1
	iID, _ := issue["id"]
	id, _ := iID.(string)
	iIssueID, _ := issue["issue_id"]
	issueID := int(iIssueID.(float64))
	issueNumber, _ := issue["id_in_repo"]
	iNumber, _ := issueNumber.(int)
	iGithubRepo, _ := issue["github_repo"]
	githubRepo, _ := iGithubRepo.(string)
	copyIssueFields := []string{"category", "github_repo", "repo_name", "repository", "repo_short_name", "pull_request"}
	copyCommentFields := []string{"created_at", "updated_at", "body", "body_analyzed", "author_association", "url", "html_url"}
	for _, comment := range comments {
		rich := make(map[string]interface{})
		for _, field := range RawFields {
			v, _ := issue[field]
			rich[field] = v
		}
		for _, field := range copyIssueFields {
			rich[field], _ = issue[field]
		}
		for _, field := range copyCommentFields {
			rich[field], _ = comment[field]
		}
		if ctx.Project != "" {
			rich["project"] = ctx.Project
		}
		rich["type"] = "issue_comment"
		rich["item_type"] = "issue comment"
		rich["issue_comment"] = true
		rich["issue_created_at"], _ = issue["created_at"]
		rich["issue_id"] = issueID
		rich["issue_number"] = issueNumber
		iCID, _ := comment["id"]
		cid := int64(iCID.(float64))
		rich["id_in_repo"] = cid
		rich["issue_comment_id"] = cid
		rich["id"] = id + "/comment/" + fmt.Sprintf("%d", cid)
		rich["url_id"] = fmt.Sprintf("%s/issues/%d/comments/%d", githubRepo, iNumber, cid)
		reactions := 0
		iReactions, ok := Dig(comment, []string{"reactions", "total_count"}, false, true)
		if ok {
			reactions = int(iReactions.(float64))
		}
		rich["n_reactions"] = reactions
		rich["commenter_association"], _ = comment["author_association"]
		rich["commenter_login"], _ = Dig(comment, []string{"user", "login"}, false, true)
		iCommenterData, ok := comment["user_data"]
		if ok && iCommenterData != nil {
			user, _ := iCommenterData.(map[string]interface{})
			rich["author_login"], _ = user["login"]
			rich["author_name"], _ = user["name"]
			rich["author_avatar_url"], _ = user["avatar_url"]
			rich["commenter_avatar_url"] = rich["author_avatar_url"]
			rich["commenter_name"], _ = user["name"]
			rich["commenter_domain"] = nil
			iEmail, ok := user["email"]
			if ok {
				email, _ := iEmail.(string)
				ary := strings.Split(email, "@")
				if len(ary) > 1 {
					rich["commenter_domain"] = strings.TrimSpace(ary[1])
				}
			}
			rich["commenter_org"], _ = user["company"]
			rich["commenter_location"], _ = user["location"]
			rich["commenter_geolocation"] = nil
		} else {
			rich["author_login"] = nil
			rich["author_name"] = nil
			rich["author_avatar_url"] = nil
			rich["commenter_avatar_url"] = nil
			rich["commenter_name"] = nil
			rich["commenter_domain"] = nil
			rich["commenter_org"] = nil
			rich["commenter_location"] = nil
			rich["commenter_geolocation"] = nil
		}
		iCreatedAt, _ := comment["created_at"]
		createdAt, _ := TimeParseInterfaceString(iCreatedAt)
		rich[j.DateField(ctx)] = createdAt
		if affs {
			authorKey := "user_data"
			var affsItems map[string]interface{}
			affsItems, err = j.AffsItems(ctx, comment, GitHubIssueCommentRoles, createdAt)
			if err != nil {
				return
			}
			for prop, value := range affsItems {
				rich[prop] = value
			}
			for _, suff := range AffsFields {
				rich[Author+suff] = rich[authorKey+suff]
				rich["commenter"+suff] = rich[authorKey+suff]
			}
			orgsKey := authorKey + MultiOrgNames
			_, ok := Dig(rich, []string{orgsKey}, false, true)
			if !ok {
				rich[orgsKey] = []interface{}{}
			}
		}
		for prop, value := range CommonFields(j, createdAt, j.Category) {
			rich[prop] = value
		}
		for prop, value := range CommonFields(j, createdAt, j.Category+"_comment") {
			rich[prop] = value
		}
		richItems = append(richItems, rich)
	}
	return
}

// EnrichIssueAssignees - return rich assignees from raw issue
func (j *DSGitHub) EnrichIssueAssignees(ctx *Ctx, issue map[string]interface{}, assignees []map[string]interface{}, affs bool) (richItems []interface{}, err error) {
	// type: category, type(_), item_type( ), issue_assignee=true
	// copy issue: github_repo, repo_name, repository
	// identify: id, id_in_repo, issue_assignee_login, url_id
	// standard: metadata..., origin, project, project_slug, uuid
	// parent: issue_id, issue_number
	// identity: author_... -> assignee_...,
	// common: is_github_issue=1, is_github_issue_assignee=1
	iID, _ := issue["id"]
	id, _ := iID.(string)
	iIssueID, _ := issue["issue_id"]
	issueID := int(iIssueID.(float64))
	issueNumber, _ := issue["id_in_repo"]
	iNumber, _ := issueNumber.(int)
	iGithubRepo, _ := issue["github_repo"]
	githubRepo, _ := iGithubRepo.(string)
	copyIssueFields := []string{"category", "github_repo", "repo_name", "repository", "repo_short_name", "pull_request"}
	for _, assignee := range assignees {
		rich := make(map[string]interface{})
		for _, field := range RawFields {
			v, _ := issue[field]
			rich[field] = v
		}
		for _, field := range copyIssueFields {
			rich[field], _ = issue[field]
		}
		if ctx.Project != "" {
			rich["project"] = ctx.Project
		}
		rich["type"] = "issue_assignee"
		rich["item_type"] = "issue assignee"
		rich["issue_assignee"] = true
		rich["issue_id"] = issueID
		rich["issue_number"] = issueNumber
		iLogin, _ := assignee["login"]
		login, _ := iLogin.(string)
		rich["id_in_repo"], _ = assignee["id"]
		rich["issue_assignee_login"] = login
		rich["id"] = id + "/assignee/" + login
		rich["url_id"] = fmt.Sprintf("%s/issues/%d/assignees/%s", githubRepo, iNumber, login)
		rich["author_login"] = login
		rich["author_name"], _ = assignee["name"]
		rich["author_avatar_url"], _ = assignee["avatar_url"]
		rich["assignee_avatar_url"] = rich["author_avatar_url"]
		rich["assignee_login"] = login
		rich["assignee_name"], _ = assignee["name"]
		rich["assignee_domain"] = nil
		iEmail, ok := assignee["email"]
		if ok {
			email, _ := iEmail.(string)
			ary := strings.Split(email, "@")
			if len(ary) > 1 {
				rich["assignee_domain"] = strings.TrimSpace(ary[1])
			}
		}
		rich["assignee_org"], _ = assignee["company"]
		rich["assignee_location"], _ = assignee["location"]
		rich["assignee_geolocation"] = nil
		// We consider assignee assignment at issue creation date
		iCreatedAt, _ := issue["created_at"]
		createdAt, _ := iCreatedAt.(time.Time)
		rich[j.DateField(ctx)] = createdAt
		if affs {
			authorKey := "assignee"
			var affsItems map[string]interface{}
			affsItems, err = j.AffsItems(ctx, map[string]interface{}{"assignee": assignee}, GitHubIssueAssigneeRoles, createdAt)
			if err != nil {
				return
			}
			for prop, value := range affsItems {
				rich[prop] = value
			}
			for _, suff := range AffsFields {
				rich[Author+suff] = rich[authorKey+suff]
			}
			orgsKey := authorKey + MultiOrgNames
			_, ok := Dig(rich, []string{orgsKey}, false, true)
			if !ok {
				rich[orgsKey] = []interface{}{}
			}
		}
		for prop, value := range CommonFields(j, createdAt, j.Category) {
			rich[prop] = value
		}
		for prop, value := range CommonFields(j, createdAt, j.Category+"_assignee") {
			rich[prop] = value
		}
		richItems = append(richItems, rich)
	}
	return
}

// EnrichIssueReactions - return rich reactions from raw issue and/or issue comment
func (j *DSGitHub) EnrichIssueReactions(ctx *Ctx, issue map[string]interface{}, reactions []map[string]interface{}, affs bool) (richItems []interface{}, err error) {
	// type: category, type(_), item_type( ), issue_reaction=true | issue_comment_reaction=true
	// copy issue: github_repo, repo_name, repository
	// copy reaction: content
	// identify: id, id_in_repo, issue_reaction_id | issue_comment_reaction_id, url_id
	// standard: metadata..., origin, project, project_slug, uuid
	// parent: issue_id, issue_number
	// identity: author_... -> actor_...,
	// common: is_github_issue=1, is_github_issue_reaction=1 | is_github_issue_comment_reaction=1
	iID, _ := issue["id"]
	id, _ := iID.(string)
	iIssueID, _ := issue["issue_id"]
	issueID := int(iIssueID.(float64))
	issueNumber, _ := issue["id_in_repo"]
	iNumber, _ := issueNumber.(int)
	iGithubRepo, _ := issue["github_repo"]
	githubRepo, _ := iGithubRepo.(string)
	copyIssueFields := []string{"category", "github_repo", "repo_name", "repository", "repo_short_name", "pull_request"}
	copyReactionFields := []string{"content"}
	for _, reaction := range reactions {
		rich := make(map[string]interface{})
		for _, field := range RawFields {
			v, _ := issue[field]
			rich[field] = v
		}
		for _, field := range copyIssueFields {
			rich[field], _ = issue[field]
		}
		for _, field := range copyReactionFields {
			rich[field], _ = reaction[field]
		}
		if ctx.Project != "" {
			rich["project"] = ctx.Project
		}
		rich["issue_id"] = issueID
		rich["issue_number"] = issueNumber
		iRID, _ := reaction["id"]
		rid := int64(iRID.(float64))
		var (
			comment        map[string]interface{}
			createdAt      time.Time
			reactionSuffix string
		)
		iComment, ok := reaction["parent"]
		if ok {
			comment, _ = iComment.(map[string]interface{})
		}
		if comment != nil {
			reactionSuffix = "_comment_reaction"
			iCID, _ := comment["id"]
			cid := int64(iCID.(float64))
			rich["type"] = "issue" + reactionSuffix
			rich["item_type"] = "issue comment reaction"
			rich["issue"+reactionSuffix] = true
			rich["issue_comment_id"] = cid
			rich["issue_comment_reaction_id"] = rid
			rich["id_in_repo"] = rid
			rich["id"] = id + "/comment/" + fmt.Sprintf("%d", cid) + "/reaction/" + fmt.Sprintf("%d", rid)
			rich["url_id"] = fmt.Sprintf("%s/issues/%d/comments/%d/reactions/%d", githubRepo, iNumber, cid, rid)
			iCreatedAt, _ := comment["created_at"]
			// createdAt is comment creation date for comment reactions
			// reaction itself doesn't have any date in GH API
			createdAt, _ = TimeParseInterfaceString(iCreatedAt)
		} else {
			reactionSuffix = "_reaction"
			rich["type"] = "issue" + reactionSuffix
			rich["item_type"] = "issue reaction"
			rich["issue"+reactionSuffix] = true
			rich["issue_reaction_id"] = rid
			rich["id_in_repo"] = rid
			rich["id"] = id + "/reaction/" + fmt.Sprintf("%d", rid)
			rich["url_id"] = fmt.Sprintf("%s/issues/%d/reactions/%d", githubRepo, iNumber, rid)
			iCreatedAt, _ := issue["created_at"]
			// createdAt is issue creation date for issue reactions
			// reaction itself doesn't have any date in GH API
			createdAt, _ = iCreatedAt.(time.Time)
		}
		iUserData, ok := reaction["user_data"]
		if ok && iUserData != nil {
			user, _ := iUserData.(map[string]interface{})
			rich["author_login"], _ = user["login"]
			rich["actor_login"], _ = user["login"]
			rich["author_name"], _ = user["name"]
			rich["author_avatar_url"], _ = user["avatar_url"]
			rich["actor_avatar_url"] = rich["author_avatar_url"]
			rich["actor_name"], _ = user["name"]
			rich["actor_domain"] = nil
			iEmail, ok := user["email"]
			if ok {
				email, _ := iEmail.(string)
				ary := strings.Split(email, "@")
				if len(ary) > 1 {
					rich["actor_domain"] = strings.TrimSpace(ary[1])
				}
			}
			rich["actor_org"], _ = user["company"]
			rich["actor_location"], _ = user["location"]
			rich["actor_geolocation"] = nil
		} else {
			rich["author_login"] = nil
			rich["author_name"] = nil
			rich["author_avatar_url"] = nil
			rich["actor_avatar_url"] = nil
			rich["actor_login"] = nil
			rich["actor_name"] = nil
			rich["actor_domain"] = nil
			rich["actor_org"] = nil
			rich["actor_location"] = nil
			rich["actor_geolocation"] = nil
		}
		rich[j.DateField(ctx)] = createdAt
		if affs {
			authorKey := "user_data"
			var affsItems map[string]interface{}
			affsItems, err = j.AffsItems(ctx, reaction, GitHubIssueReactionRoles, createdAt)
			if err != nil {
				return
			}
			for prop, value := range affsItems {
				rich[prop] = value
			}
			for _, suff := range AffsFields {
				rich[Author+suff] = rich[authorKey+suff]
				rich["actor"+suff] = rich[authorKey+suff]
			}
			orgsKey := authorKey + MultiOrgNames
			_, ok := Dig(rich, []string{orgsKey}, false, true)
			if !ok {
				rich[orgsKey] = []interface{}{}
			}
		}
		for prop, value := range CommonFields(j, createdAt, j.Category) {
			rich[prop] = value
		}
		for prop, value := range CommonFields(j, createdAt, j.Category+reactionSuffix) {
			rich[prop] = value
		}
		richItems = append(richItems, rich)
	}
	return
}

// EnrichPullRequestComments - return rich comments from raw pull request
func (j *DSGitHub) EnrichPullRequestComments(ctx *Ctx, pull map[string]interface{}, comments []map[string]interface{}, affs bool) (richItems []interface{}, err error) {
	// type: category, type(_), item_type( ), pull_request_comment=true
	// copy pull request: github_repo, repo_name, repository
	// copy comment: created_at, updated_at, body, body_analyzed, author_association, url, html_url
	// identify: id, id_in_repo, pull_request_comment_id, url_id
	// standard: metadata..., origin, project, project_slug, uuid
	// parent: pull_request_id, pull_request_number
	// calc: n_reactions
	// identity: author_... -> commenter_...,
	// common: is_github_pull_request=1, is_github_pull_request_comment=1
	iID, _ := pull["id"]
	id, _ := iID.(string)
	iPullID, _ := pull["pull_request_id"]
	pullID := int(iPullID.(float64))
	pullNumber, _ := pull["id_in_repo"]
	iNumber, _ := pullNumber.(int)
	iGithubRepo, _ := pull["github_repo"]
	githubRepo, _ := iGithubRepo.(string)
	copyPullFields := []string{"category", "github_repo", "repo_name", "repository", "repo_short_name"}
	copyCommentFields := []string{"created_at", "updated_at", "body", "body_analyzed", "author_association", "url", "html_url"}
	for _, comment := range comments {
		rich := make(map[string]interface{})
		for _, field := range RawFields {
			v, _ := pull[field]
			rich[field] = v
		}
		for _, field := range copyPullFields {
			rich[field], _ = pull[field]
		}
		for _, field := range copyCommentFields {
			rich[field], _ = comment[field]
		}
		if ctx.Project != "" {
			rich["project"] = ctx.Project
		}
		rich["type"] = "pull_request_comment"
		rich["item_type"] = "pull request comment"
		rich["pull_request_comment"] = true
		rich["pull_request_created_at"], _ = pull["created_at"]
		rich["pull_request_id"] = pullID
		rich["pull_request_number"] = pullNumber
		iCID, _ := comment["id"]
		cid := int64(iCID.(float64))
		rich["id_in_repo"] = cid
		rich["pull_request_comment_id"] = cid
		rich["id"] = id + "/comment/" + fmt.Sprintf("%d", cid)
		rich["url_id"] = fmt.Sprintf("%s/pulls/%d/comments/%d", githubRepo, iNumber, cid)
		reactions := 0
		iReactions, ok := Dig(comment, []string{"reactions", "total_count"}, false, true)
		if ok {
			reactions = int(iReactions.(float64))
		}
		rich["n_reactions"] = reactions
		rich["commenter_association"], _ = comment["author_association"]
		rich["commenter_login"], _ = Dig(comment, []string{"user", "login"}, false, true)
		iCommenterData, ok := comment["user_data"]
		if ok && iCommenterData != nil {
			user, _ := iCommenterData.(map[string]interface{})
			rich["author_login"], _ = user["login"]
			rich["author_name"], _ = user["name"]
			rich["author_avatar_url"], _ = user["avatar_url"]
			rich["commenter_avatar_url"] = rich["author_avatar_url"]
			rich["commenter_name"], _ = user["name"]
			rich["commenter_domain"] = nil
			iEmail, ok := user["email"]
			if ok {
				email, _ := iEmail.(string)
				ary := strings.Split(email, "@")
				if len(ary) > 1 {
					rich["commenter_domain"] = strings.TrimSpace(ary[1])
				}
			}
			rich["commenter_org"], _ = user["company"]
			rich["commenter_location"], _ = user["location"]
			rich["commenter_geolocation"] = nil
		} else {
			rich["author_login"] = nil
			rich["author_name"] = nil
			rich["author_avatar_url"] = nil
			rich["commenter_avatar_url"] = nil
			rich["commenter_name"] = nil
			rich["commenter_domain"] = nil
			rich["commenter_org"] = nil
			rich["commenter_location"] = nil
			rich["commenter_geolocation"] = nil
		}
		iCreatedAt, _ := comment["created_at"]
		createdAt, _ := TimeParseInterfaceString(iCreatedAt)
		rich[j.DateField(ctx)] = createdAt
		if affs {
			authorKey := "user_data"
			var affsItems map[string]interface{}
			affsItems, err = j.AffsItems(ctx, comment, GitHubPullRequestCommentRoles, createdAt)
			if err != nil {
				return
			}
			for prop, value := range affsItems {
				rich[prop] = value
			}
			for _, suff := range AffsFields {
				rich[Author+suff] = rich[authorKey+suff]
				rich["commenter"+suff] = rich[authorKey+suff]
			}
			orgsKey := authorKey + MultiOrgNames
			_, ok := Dig(rich, []string{orgsKey}, false, true)
			if !ok {
				rich[orgsKey] = []interface{}{}
			}
		}
		for prop, value := range CommonFields(j, createdAt, j.Category) {
			rich[prop] = value
		}
		for prop, value := range CommonFields(j, createdAt, j.Category+"_comment") {
			rich[prop] = value
		}
		richItems = append(richItems, rich)
	}
	return
}

// EnrichPullRequestReviews - return rich reviews from raw pull request
func (j *DSGitHub) EnrichPullRequestReviews(ctx *Ctx, pull map[string]interface{}, reviews []map[string]interface{}, affs bool) (richItems []interface{}, err error) {
	// type: category, type(_), item_type( ), pull_request_review=true
	// copy pull request: github_repo, repo_name, repository
	// copy review: body, body_analyzed, submitted_at, commit_id, html_url, pull_request_url, state, author_association
	// identify: id, id_in_repo, pull_request_comment_id, url_id
	// standard: metadata..., origin, project, project_slug, uuid
	// parent: pull_request_id, pull_request_number
	// calc: n_reactions
	// identity: author_... -> reviewer_...,
	// common: is_github_pull_request=1, is_github_pull_request_review=1
	iID, _ := pull["id"]
	id, _ := iID.(string)
	iPullID, _ := pull["pull_request_id"]
	pullID := int(iPullID.(float64))
	pullNumber, _ := pull["id_in_repo"]
	iNumber, _ := pullNumber.(int)
	iGithubRepo, _ := pull["github_repo"]
	pullCreatedAt, _ := pull["created_at"]
	githubRepo, _ := iGithubRepo.(string)
	copyPullFields := []string{"category", "github_repo", "repo_name", "repository", "url", "repo_short_name", "merged"}
	copyReviewFields := []string{"body", "body_analyzed", "submitted_at", "commit_id", "html_url", "pull_request_url", "state", "author_association", "is_first_review", "is_first_approval"}
	bApproved := false
	firstReview := time.Now()
	firstApproval := time.Now()
	firstReviewIdx := -1
	firstApprovalIdx := -1
	for i, review := range reviews {
		review["is_first_review"] = false
		review["is_first_approval"] = false
		iSubmittedAt, _ := review["submitted_at"]
		submittedAt, _ := TimeParseInterfaceString(iSubmittedAt)
		if submittedAt.Before(firstReview) {
			firstReview = submittedAt
			firstReviewIdx = i
		}
		approved, ok := review["state"]
		if !ok {
			continue
		}
		if approved.(string) == "APPROVED" {
			bApproved = true
			if submittedAt.Before(firstApproval) {
				firstApproval = submittedAt
				firstApprovalIdx = i
			}
		}
	}
	if firstReviewIdx >= 0 {
		reviews[firstReviewIdx]["is_first_review"] = true
	}
	if firstApprovalIdx >= 0 {
		reviews[firstApprovalIdx]["is_first_approval"] = true
	}
	for _, review := range reviews {
		rich := make(map[string]interface{})
		for _, field := range RawFields {
			v, _ := pull[field]
			rich[field] = v
		}
		for _, field := range copyPullFields {
			rich[field], _ = pull[field]
		}
		for _, field := range copyReviewFields {
			rich[field], _ = review[field]
		}
		if ctx.Project != "" {
			rich["project"] = ctx.Project
		}
		rich["type"] = "pull_request_review"
		rich["item_type"] = "pull request review"
		rich["pull_request_review"] = true
		rich["pull_request_id"] = pullID
		rich["pull_request_number"] = pullNumber
		rich["is_approved"] = bApproved
		iRID, _ := review["id"]
		rid := int64(iRID.(float64))
		rich["id_in_repo"] = rid
		rich["pull_request_review_id"] = rid
		rich["pull_request_created_at"] = pullCreatedAt
		rich["id"] = id + "/review/" + fmt.Sprintf("%d", rid)
		rich["url_id"] = fmt.Sprintf("%s/pulls/%d/reviews/%d", githubRepo, iNumber, rid)
		rich["reviewer_association"], _ = review["author_association"]
		rich["reviewer_login"], _ = Dig(review, []string{"user", "login"}, false, true)
		iReviewerData, ok := review["user_data"]
		if ok && iReviewerData != nil {
			user, _ := iReviewerData.(map[string]interface{})
			rich["author_login"], _ = user["login"]
			rich["author_name"], _ = user["name"]
			rich["author_avatar_url"], _ = user["avatar_url"]
			rich["reviewer_avatar_url"] = rich["author_avatar_url"]
			rich["reviewer_name"], _ = user["name"]
			rich["reviewer_domain"] = nil
			iEmail, ok := user["email"]
			if ok {
				email, _ := iEmail.(string)
				ary := strings.Split(email, "@")
				if len(ary) > 1 {
					rich["reviewer_domain"] = strings.TrimSpace(ary[1])
				}
			}
			rich["reviewer_org"], _ = user["company"]
			rich["reviewer_location"], _ = user["location"]
			rich["reviewer_geolocation"] = nil
		} else {
			rich["author_login"] = nil
			rich["author_name"] = nil
			rich["author_avatar_url"] = nil
			rich["reviewer_avatar_url"] = nil
			rich["reviewer_name"] = nil
			rich["reviewer_domain"] = nil
			rich["reviewer_org"] = nil
			rich["reviewer_location"] = nil
			rich["reviewer_geolocation"] = nil
		}
		iSubmittedAt, _ := review["submitted_at"]
		submittedAt, _ := TimeParseInterfaceString(iSubmittedAt)
		rich[j.DateField(ctx)] = submittedAt
		if affs {
			authorKey := "user_data"
			var affsItems map[string]interface{}
			affsItems, err = j.AffsItems(ctx, review, GitHubPullRequestReviewRoles, submittedAt)
			if err != nil {
				return
			}
			for prop, value := range affsItems {
				rich[prop] = value
			}
			for _, suff := range AffsFields {
				rich[Author+suff] = rich[authorKey+suff]
				rich["reviewer"+suff] = rich[authorKey+suff]
			}
			orgsKey := authorKey + MultiOrgNames
			_, ok := Dig(rich, []string{orgsKey}, false, true)
			if !ok {
				rich[orgsKey] = []interface{}{}
			}
		}
		for prop, value := range CommonFields(j, submittedAt, j.Category) {
			rich[prop] = value
		}
		for prop, value := range CommonFields(j, submittedAt, j.Category+"_review") {
			rich[prop] = value
		}
		richItems = append(richItems, rich)
	}
	pull["is_approved"] = bApproved
	return
}

// EnrichPullRequestAssignees - return rich assignees from raw pull request
func (j *DSGitHub) EnrichPullRequestAssignees(ctx *Ctx, pull map[string]interface{}, assignees []map[string]interface{}, affs bool) (richItems []interface{}, err error) {
	// type: category, type(_), item_type( ), pull_request_assignee=true
	// copy pull request: github_repo, repo_name, repository
	// identify: id, id_in_repo, pull_request_assignee_login, url_id
	// standard: metadata..., origin, project, project_slug, uuid
	// parent: pull_request_id, pull_request_number
	// identity: author_... -> assignee_...,
	// common: is_github_pull_request=1, is_github_pull_request_assignee=1
	iID, _ := pull["id"]
	id, _ := iID.(string)
	iPullID, _ := pull["pull_request_id"]
	pullID := int(iPullID.(float64))
	pullNumber, _ := pull["id_in_repo"]
	iNumber, _ := pullNumber.(int)
	iGithubRepo, _ := pull["github_repo"]
	githubRepo, _ := iGithubRepo.(string)
	copyPullFields := []string{"category", "github_repo", "repo_name", "repository", "repo_short_name"}
	for _, assignee := range assignees {
		rich := make(map[string]interface{})
		for _, field := range RawFields {
			v, _ := pull[field]
			rich[field] = v
		}
		for _, field := range copyPullFields {
			rich[field], _ = pull[field]
		}
		if ctx.Project != "" {
			rich["project"] = ctx.Project
		}
		rich["type"] = "pull_request_assignee"
		rich["item_type"] = "pull request assignee"
		rich["pull_request_assignee"] = true
		rich["pull_request_id"] = pullID
		rich["pull_request_number"] = pullNumber
		iLogin, _ := assignee["login"]
		login, _ := iLogin.(string)
		rich["id_in_repo"], _ = assignee["id"]
		rich["pull_request_assignee_login"] = login
		rich["id"] = id + "/assignee/" + login
		rich["url_id"] = fmt.Sprintf("%s/pulls/%d/assignees/%s", githubRepo, iNumber, login)
		rich["author_login"] = login
		rich["author_name"], _ = assignee["name"]
		rich["author_avatar_url"], _ = assignee["avatar_url"]
		rich["assignee_avatar_url"] = rich["author_avatar_url"]
		rich["assignee_login"] = login
		rich["assignee_name"], _ = assignee["name"]
		rich["assignee_domain"] = nil
		iEmail, ok := assignee["email"]
		if ok {
			email, _ := iEmail.(string)
			ary := strings.Split(email, "@")
			if len(ary) > 1 {
				rich["assignee_domain"] = strings.TrimSpace(ary[1])
			}
		}
		rich["assignee_org"], _ = assignee["company"]
		rich["assignee_location"], _ = assignee["location"]
		rich["assignee_geolocation"] = nil
		// We consider assignee enrollment at pull request creation date
		iCreatedAt, _ := pull["created_at"]
		createdAt, _ := iCreatedAt.(time.Time)
		rich[j.DateField(ctx)] = createdAt
		if affs {
			authorKey := "assignee"
			var affsItems map[string]interface{}
			affsItems, err = j.AffsItems(ctx, map[string]interface{}{"assignee": assignee}, GitHubPullRequestAssigneeRoles, createdAt)
			if err != nil {
				return
			}
			for prop, value := range affsItems {
				rich[prop] = value
			}
			for _, suff := range AffsFields {
				rich[Author+suff] = rich[authorKey+suff]
			}
			orgsKey := authorKey + MultiOrgNames
			_, ok := Dig(rich, []string{orgsKey}, false, true)
			if !ok {
				rich[orgsKey] = []interface{}{}
			}
		}
		for prop, value := range CommonFields(j, createdAt, j.Category) {
			rich[prop] = value
		}
		for prop, value := range CommonFields(j, createdAt, j.Category+"_assignee") {
			rich[prop] = value
		}
		richItems = append(richItems, rich)
	}
	return
}

// EnrichPullRequestReactions - return rich reactions from raw pull request comment
func (j *DSGitHub) EnrichPullRequestReactions(ctx *Ctx, pull map[string]interface{}, reactions []map[string]interface{}, affs bool) (richItems []interface{}, err error) {
	// type: category, type(_), item_type( ), pull_request_comment_reaction=true
	// copy pull request: github_repo, repo_name, repository
	// copy reaction: content
	// identify: id, id_in_repo, pull_request_comment_reaction_id, url_id
	// standard: metadata..., origin, project, project_slug, uuid
	// parent: pull_request_id, pull_request_number
	// identity: author_... -> actor_...,
	// common: is_github_pull_request=1, is_github_pull_request_comment_reaction=1
	iID, _ := pull["id"]
	id, _ := iID.(string)
	iPullID, _ := pull["pull_request_id"]
	pullID := int(iPullID.(float64))
	pullNumber, _ := pull["id_in_repo"]
	iNumber, _ := pullNumber.(int)
	iGithubRepo, _ := pull["github_repo"]
	githubRepo, _ := iGithubRepo.(string)
	copyPullFields := []string{"category", "github_repo", "repo_name", "repository", "repo_short_name"}
	copyReactionFields := []string{"content"}
	reactionSuffix := "_comment_reaction"
	for _, reaction := range reactions {
		rich := make(map[string]interface{})
		for _, field := range RawFields {
			v, _ := pull[field]
			rich[field] = v
		}
		for _, field := range copyPullFields {
			rich[field], _ = pull[field]
		}
		for _, field := range copyReactionFields {
			rich[field], _ = reaction[field]
		}
		if ctx.Project != "" {
			rich["project"] = ctx.Project
		}
		rich["pull_request_id"] = pullID
		rich["pull_request_number"] = pullNumber
		iRID, _ := reaction["id"]
		rid := int64(iRID.(float64))
		iComment, _ := reaction["parent"]
		comment, _ := iComment.(map[string]interface{})
		iCID, _ := comment["id"]
		cid := int64(iCID.(float64))
		rich["type"] = "pull_request" + reactionSuffix
		rich["item_type"] = "pull request comment reaction"
		rich["pull_request"+reactionSuffix] = true
		rich["pull_request_comment_id"] = cid
		rich["pull_request_comment_reaction_id"] = rid
		rich["id_in_repo"] = rid
		rich["id"] = id + "/comment/" + fmt.Sprintf("%d", cid) + "/reaction/" + fmt.Sprintf("%d", rid)
		rich["url_id"] = fmt.Sprintf("%s/pulls/%d/comments/%d/reactions/%d", githubRepo, iNumber, cid, rid)
		iCreatedAt, _ := comment["created_at"]
		iUserData, ok := reaction["user_data"]
		if ok && iUserData != nil {
			user, _ := iUserData.(map[string]interface{})
			rich["author_login"], _ = user["login"]
			rich["actor_login"], _ = user["login"]
			rich["author_name"], _ = user["name"]
			rich["author_avatar_url"], _ = user["avatar_url"]
			rich["actor_avatar_url"] = rich["author_avatar_url"]
			rich["actor_name"], _ = user["name"]
			rich["actor_domain"] = nil
			iEmail, ok := user["email"]
			if ok {
				email, _ := iEmail.(string)
				ary := strings.Split(email, "@")
				if len(ary) > 1 {
					rich["actor_domain"] = strings.TrimSpace(ary[1])
				}
			}
			rich["actor_org"], _ = user["company"]
			rich["actor_location"], _ = user["location"]
			rich["actor_geolocation"] = nil
		} else {
			rich["author_login"] = nil
			rich["author_name"] = nil
			rich["author_avatar_url"] = nil
			rich["actor_avatar_url"] = nil
			rich["actor_login"] = nil
			rich["actor_name"] = nil
			rich["actor_domain"] = nil
			rich["actor_org"] = nil
			rich["actor_location"] = nil
			rich["actor_geolocation"] = nil
		}
		// createdAt is pull request comment creation date
		// reaction itself doesn't have any date in GH API
		createdAt, _ := TimeParseInterfaceString(iCreatedAt)
		rich[j.DateField(ctx)] = createdAt
		if affs {
			authorKey := "user_data"
			var affsItems map[string]interface{}
			affsItems, err = j.AffsItems(ctx, reaction, GitHubPullRequestReactionRoles, createdAt)
			if err != nil {
				return
			}
			for prop, value := range affsItems {
				rich[prop] = value
			}
			for _, suff := range AffsFields {
				rich[Author+suff] = rich[authorKey+suff]
				rich["actor"+suff] = rich[authorKey+suff]
			}
			orgsKey := authorKey + MultiOrgNames
			_, ok := Dig(rich, []string{orgsKey}, false, true)
			if !ok {
				rich[orgsKey] = []interface{}{}
			}
		}
		for prop, value := range CommonFields(j, createdAt, j.Category) {
			rich[prop] = value
		}
		for prop, value := range CommonFields(j, createdAt, j.Category+reactionSuffix) {
			rich[prop] = value
		}
		richItems = append(richItems, rich)
	}
	return
}

// EnrichPullRequestRequestedReviewers - return rich requested reviewers from raw pull request
func (j *DSGitHub) EnrichPullRequestRequestedReviewers(ctx *Ctx, pull map[string]interface{}, requestedReviewers []map[string]interface{}, affs bool) (richItems []interface{}, err error) {
	// type: category, type(_), item_type( ), pull_request_requested_reviewer=true
	// copy pull request: github_repo, repo_name, repository
	// identify: id, id_in_repo, pull_request_requested_reviewer_login, url_id
	// standard: metadata..., origin, project, project_slug, uuid
	// parent: pull_request_id, pull_request_number
	// identity: author_... -> requested_reviewer_...,
	// common: is_github_pull_request=1, is_github_pull_request_requested_reviewer=1
	iID, _ := pull["id"]
	id, _ := iID.(string)
	iPullID, _ := pull["pull_request_id"]
	pullID := int(iPullID.(float64))
	pullNumber, _ := pull["id_in_repo"]
	iNumber, _ := pullNumber.(int)
	iGithubRepo, _ := pull["github_repo"]
	githubRepo, _ := iGithubRepo.(string)
	copyPullFields := []string{"category", "github_repo", "repo_name", "repository", "repo_short_name"}
	for _, reviewer := range requestedReviewers {
		rich := make(map[string]interface{})
		for _, field := range RawFields {
			v, _ := pull[field]
			rich[field] = v
		}
		for _, field := range copyPullFields {
			rich[field], _ = pull[field]
		}
		if ctx.Project != "" {
			rich["project"] = ctx.Project
		}
		rich["type"] = "pull_request_requested_reviewer"
		rich["item_type"] = "pull request requested reviewer"
		rich["pull_request_requested_reviewer"] = true
		rich["pull_request_id"] = pullID
		rich["pull_request_number"] = pullNumber
		iLogin, _ := reviewer["login"]
		login, _ := iLogin.(string)
		rich["id_in_repo"], _ = reviewer["id"]
		rich["pull_request_requested_reviewer_login"] = login
		rich["id"] = id + "/requested_reviewer/" + login
		rich["url_id"] = fmt.Sprintf("%s/pulls/%d/requested_reviewers/%s", githubRepo, iNumber, login)
		rich["author_login"] = login
		rich["author_name"], _ = reviewer["name"]
		rich["author_avatar_url"], _ = reviewer["avatar_url"]
		rich["requested_reviewer_avatar_url"] = rich["author_avatar_url"]
		rich["requested_reviewer_login"] = login
		rich["requested_reviewer_name"], _ = reviewer["name"]
		rich["requested_reviewer_domain"] = nil
		iEmail, ok := reviewer["email"]
		if ok {
			email, _ := iEmail.(string)
			ary := strings.Split(email, "@")
			if len(ary) > 1 {
				rich["requested_reviewer_domain"] = strings.TrimSpace(ary[1])
			}
		}
		rich["requested_reviewer_org"], _ = reviewer["company"]
		rich["requested_reviewer_location"], _ = reviewer["location"]
		rich["requested_reviewer_geolocation"] = nil
		// We consider requested reviewer enrollment at pull request creation date
		iCreatedAt, _ := pull["created_at"]
		createdAt, _ := iCreatedAt.(time.Time)
		rich[j.DateField(ctx)] = createdAt
		if affs {
			authorKey := "requested_reviewer"
			var affsItems map[string]interface{}
			affsItems, err = j.AffsItems(ctx, map[string]interface{}{"requested_reviewer": reviewer}, GitHubPullRequestRequestedReviewerRoles, createdAt)
			if err != nil {
				return
			}
			for prop, value := range affsItems {
				rich[prop] = value
			}
			for _, suff := range AffsFields {
				rich[Author+suff] = rich[authorKey+suff]
			}
			orgsKey := authorKey + MultiOrgNames
			_, ok := Dig(rich, []string{orgsKey}, false, true)
			if !ok {
				rich[orgsKey] = []interface{}{}
			}
		}
		for prop, value := range CommonFields(j, createdAt, j.Category) {
			rich[prop] = value
		}
		for prop, value := range CommonFields(j, createdAt, j.Category+"_requested_reviewer") {
			rich[prop] = value
		}
		richItems = append(richItems, rich)
	}
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
	if ctx.Project != "" {
		rich["project"] = ctx.Project
	}
	rich["repo_name"] = j.URL
	rich["repository"] = j.URL
	// I think we don't need original UUID in id
	/*
		uuid, ok := rich[UUID].(string)
		if !ok {
			err = fmt.Errorf("cannot read string uuid from %+v", DumpPreview(rich, 100))
			return
		}
		iid := uuid + "/" + j.ItemID(issue)
		rich["id"] = iid
	*/
	rich["id"] = j.ItemID(issue)
	rich["issue_id"], _ = issue["id"]
	iCreatedAt, _ := issue["created_at"]
	createdAt, _ := TimeParseInterfaceString(iCreatedAt)
	updatedOn, _ := Dig(item, []string{j.DateField(ctx)}, true, false)
	rich["type"] = j.Category
	rich["category"] = j.Category
	now := time.Now()
	rich["created_at"] = createdAt
	rich["updated_at"] = updatedOn
	iClosedAt, ok := issue["closed_at"]
	rich["closed_at"] = iClosedAt
	if ok && iClosedAt != nil {
		closedAt, e := TimeParseInterfaceString(iClosedAt)
		if e == nil {
			rich["time_to_close_days"] = float64(closedAt.Sub(createdAt).Seconds()) / 86400.0
		} else {
			rich["time_to_close_days"] = nil
		}
	} else {
		rich["time_to_close_days"] = nil
	}
	state, ok := issue["state"]
	rich["state"] = state
	if ok && state != nil && state.(string) == "closed" {
		rich["time_open_days"] = rich["time_to_close_days"]
	} else {
		rich["time_open_days"] = float64(now.Sub(createdAt).Seconds()) / 86400.0
	}
	iNumber, _ := issue["number"]
	number := int(iNumber.(float64))
	rich["id_in_repo"] = number
	rich["title"], _ = issue["title"]
	rich["title_analyzed"], _ = issue["title"]
	rich["body"], _ = issue["body"]
	rich["body_analyzed"], _ = issue["body"]
	rich["url"], _ = issue["html_url"]
	rich["user_login"], _ = Dig(issue, []string{"user", "login"}, false, true)
	iUserData, ok := issue["user_data"]
	if ok && iUserData != nil {
		user, _ := iUserData.(map[string]interface{})
		rich["author_login"], _ = user["login"]
		rich["author_name"], _ = user["name"]
		rich["author_avatar_url"], _ = user["avatar_url"]
		rich["user_avatar_url"] = rich["author_avatar_url"]
		rich["user_name"], _ = user["name"]
		rich["user_domain"] = nil
		iEmail, ok := user["email"]
		if ok {
			email, _ := iEmail.(string)
			ary := strings.Split(email, "@")
			if len(ary) > 1 {
				rich["user_domain"] = strings.TrimSpace(ary[1])
			}
		}
		rich["user_org"], _ = user["company"]
		rich["user_location"], _ = user["location"]
		rich["user_geolocation"] = nil
	} else {
		rich["author_login"] = nil
		rich["author_name"] = nil
		rich["author_avatar_url"] = nil
		rich["user_avatar_url"] = nil
		rich["user_name"] = nil
		rich["user_domain"] = nil
		rich["user_org"] = nil
		rich["user_location"] = nil
		rich["user_geolocation"] = nil
	}
	iAssigneeData, ok := issue["assignee_data"]
	if ok && iAssigneeData != nil {
		assignee, _ := iAssigneeData.(map[string]interface{})
		rich["assignee_login"], _ = assignee["login"]
		rich["assignee_name"], _ = assignee["name"]
		rich["assignee_avatar_url"], _ = assignee["avatar_url"]
		rich["assignee_domain"] = nil
		iEmail, ok := assignee["email"]
		if ok {
			email, _ := iEmail.(string)
			ary := strings.Split(email, "@")
			if len(ary) > 1 {
				rich["assignee_domain"] = strings.TrimSpace(ary[1])
			}
		}
		rich["assignee_org"], _ = assignee["company"]
		rich["assignee_location"], _ = assignee["location"]
		rich["assignee_geolocation"] = nil
	} else {
		rich["assignee_login"] = nil
		rich["assignee_name"] = nil
		rich["assignee_avatar_url"] = nil
		rich["assignee_domain"] = nil
		rich["assignee_org"] = nil
		rich["assignee_location"] = nil
		rich["assignee_geolocation"] = nil
	}
	iLabels, ok := issue["labels"]
	if ok && iLabels != nil {
		ary, _ := iLabels.([]interface{})
		labels := []interface{}{}
		for _, iLabel := range ary {
			label, _ := iLabel.(map[string]interface{})
			iLabelName, _ := label["name"]
			labelName, _ := iLabelName.(string)
			if labelName != "" {
				labels = append(labels, labelName)
			}
		}
		rich["labels"] = labels
	}
	nAssignees := 0
	iAssignees, ok := issue["assignees_data"]
	if ok && iAssignees != nil {
		ary, _ := iAssignees.([]interface{})
		nAssignees = len(ary)
		assignees := []interface{}{}
		for _, iAssignee := range ary {
			assignee, _ := iAssignee.(map[string]interface{})
			iAssigneeLogin, _ := assignee["login"]
			assigneeLogin, _ := iAssigneeLogin.(string)
			if assigneeLogin != "" {
				assignees = append(assignees, assigneeLogin)
			}
		}
		rich["assignees_data"] = assignees
	}
	rich["n_assignees"] = nAssignees
	nCommenters := 0
	nComments := 0
	reactions := 0
	iComments, ok := issue["comments_data"]
	if ok && iComments != nil {
		ary, _ := iComments.([]interface{})
		nComments = len(ary)
		commenters := map[string]interface{}{}
		for _, iComment := range ary {
			comment, _ := iComment.(map[string]interface{})
			iCommenter, _ := Dig(comment, []string{"user", "login"}, false, true)
			commenter, _ := iCommenter.(string)
			if commenter != "" {
				commenters[commenter] = struct{}{}
			}
			iReactions, ok := Dig(comment, []string{"reactions", "total_count"}, false, true)
			if ok {
				reacts := int(iReactions.(float64))
				reactions += reacts
			}
		}
		nCommenters = len(commenters)
		comms := []string{}
		for commenter := range commenters {
			comms = append(comms, commenter)
		}
		rich["commenters"] = comms
	}
	rich["n_commenters"] = nCommenters
	rich["n_comments"] = nComments
	_, hasHead := issue["head"]
	_, hasPR := issue["pull_request"]
	if !hasHead && !hasPR {
		rich["pull_request"] = false
		rich["item_type"] = "issue"
	} else {
		rich["pull_request"] = true
		// "pull request" and "issue pull request" are different object
		// one is an issue object that is also a pull request, while the another is a pull request object
		// rich["item_type"] = "pull request"
		rich["item_type"] = "issue pull request"
	}
	githubRepo := j.URL
	if strings.HasSuffix(githubRepo, ".git") {
		githubRepo = githubRepo[:len(githubRepo)-4]
	}
	if strings.Contains(githubRepo, GitHubURLRoot) {
		githubRepo = strings.Replace(githubRepo, GitHubURLRoot, "", -1)
	}
	var repoShortName string
	arr := strings.Split(githubRepo, "/")
	if len(arr) > 1 {
		repoShortName = arr[1]
	}
	rich["repo_short_name"] = repoShortName
	rich["github_repo"] = githubRepo
	rich["url_id"] = fmt.Sprintf("%s/issues/%d", githubRepo, number)
	rich["time_to_first_attention"] = nil
	commentsVal := 0
	iCommentsVal, ok := issue["comments"]
	if ok {
		commentsVal = int(iCommentsVal.(float64))
	}
	rich["n_total_comments"] = commentsVal
	iReactions, ok := Dig(issue, []string{"reactions", "total_count"}, false, true)
	if ok {
		reacts := int(iReactions.(float64))
		reactions += reacts
	}
	rich["n_reactions"] = reactions
	// if comments+reactions > 0 {
	if commentsVal > 0 || nComments > 0 {
		firstAttention := j.GetFirstIssueAttention(issue)
		rich["time_to_first_attention"] = float64(firstAttention.Sub(createdAt).Seconds()) / 86400.0
	}
	rich[j.DateField(ctx)] = createdAt
	if affs {
		authorKey := "user_data"
		var affsItems map[string]interface{}
		affsItems, err = j.AffsItems(ctx, issue, GitHubIssueRoles, createdAt)
		if err != nil {
			return
		}
		for prop, value := range affsItems {
			rich[prop] = value
		}
		for _, suff := range AffsFields {
			rich[Author+suff] = rich[authorKey+suff]
		}
		orgsKey := authorKey + MultiOrgNames
		_, ok := Dig(rich, []string{orgsKey}, false, true)
		if !ok {
			rich[orgsKey] = []interface{}{}
		}
	}
	for prop, value := range CommonFields(j, createdAt, j.Category) {
		rich[prop] = value
	}
	return
}

// GetFirstIssueAttention - get first non-author action date on the issue
func (j *DSGitHub) GetFirstIssueAttention(issue map[string]interface{}) (dt time.Time) {
	iUserLogin, _ := Dig(issue, []string{"user", "login"}, false, true)
	userLogin, _ := iUserLogin.(string)
	dts := []time.Time{}
	udts := []time.Time{}
	iComments, ok := issue["comments_data"]
	if ok && iComments != nil {
		ary, _ := iComments.([]interface{})
		for _, iComment := range ary {
			comment, _ := iComment.(map[string]interface{})
			iCommentLogin, _ := Dig(comment, []string{"user", "login"}, false, true)
			commentLogin, _ := iCommentLogin.(string)
			iCreatedAt, _ := comment["created_at"]
			createdAt, _ := TimeParseInterfaceString(iCreatedAt)
			if userLogin == commentLogin {
				udts = append(udts, createdAt)
				continue
			}
			dts = append(dts, createdAt)
		}
	}
	// NOTE: p2o does it but reactions API doesn't have any datetimefield specifying when reaction was made
	/*
		iReactions, ok := issue["reactions_data"]
		if ok && iReactions != nil {
			ary, _ := iReactions.([]interface{})
			for _, iReaction := range ary {
				reaction, _ := iReaction.(map[string]interface{})
				iReactionLogin, _ := Dig(reaction, []string{"user", "login"}, false, true)
				reactionLogin, _ := iReactionLogin.(string)
				if userLogin == reactionLogin {
					continue
				}
				iCreatedAt, _ := reaction["created_at"]
				createdAt, _ := TimeParseInterfaceString(iCreatedAt)
				dts = append(dts, createdAt)
			}
		}
	*/
	nDts := len(dts)
	if nDts == 0 {
		// If there was no action of anybody else that author's, then fallback to author's actions
		dts = udts
		nDts = len(dts)
	}
	switch nDts {
	case 0:
		dt = time.Now()
	case 1:
		dt = dts[0]
	default:
		sort.Slice(dts, func(i, j int) bool {
			return dts[i].Before(dts[j])
		})
		dt = dts[0]
	}
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
	if ctx.Project != "" {
		rich["project"] = ctx.Project
	}
	rich["repo_name"] = j.URL
	rich["repository"] = j.URL
	rich["id"] = j.ItemID(pull)
	rich["pull_request_id"], _ = pull["id"]
	iCreatedAt, _ := pull["created_at"]
	createdAt, _ := TimeParseInterfaceString(iCreatedAt)
	updatedOn, _ := Dig(item, []string{j.DateField(ctx)}, true, false)
	rich["type"] = j.Category
	rich["category"] = j.Category
	now := time.Now()
	rich["created_at"] = createdAt
	rich["updated_at"] = updatedOn
	iClosedAt, ok := pull["closed_at"]
	rich["closed_at"] = iClosedAt
	if ok && iClosedAt != nil {
		closedAt, e := TimeParseInterfaceString(iClosedAt)
		if e == nil {
			rich["time_to_close_days"] = float64(closedAt.Sub(createdAt).Seconds()) / 86400.0
		} else {
			rich["time_to_close_days"] = nil
		}
	} else {
		rich["time_to_close_days"] = nil
	}
	state, ok := pull["state"]
	rich["state"] = state
	if ok && state != nil && state.(string) == "closed" {
		rich["time_open_days"] = rich["time_to_close_days"]
	} else {
		rich["time_open_days"] = float64(now.Sub(createdAt).Seconds()) / 86400.0
	}
	iNumber, _ := pull["number"]
	number := int(iNumber.(float64))
	rich["id_in_repo"] = number
	rich["title"], _ = pull["title"]
	rich["title_analyzed"], _ = pull["title"]
	rich["body"], _ = pull["body"]
	rich["body_analyzed"], _ = pull["body"]
	rich["url"], _ = pull["html_url"]
	iMergedAt, _ := pull["merged_at"]
	rich["merged_at"] = iMergedAt
	rich["merged"], _ = pull["merged"]
	rich["user_login"], _ = Dig(pull, []string{"user", "login"}, false, true)
	iUserData, ok := pull["user_data"]
	if ok && iUserData != nil {
		user, _ := iUserData.(map[string]interface{})
		rich["author_login"], _ = user["login"]
		rich["author_name"], _ = user["name"]
		rich["author_avatar_url"], _ = user["avatar_url"]
		rich["user_avatar_url"] = rich["author_avatar_url"]
		rich["user_name"], _ = user["name"]
		rich["user_domain"] = nil
		iEmail, ok := user["email"]
		if ok {
			email, _ := iEmail.(string)
			ary := strings.Split(email, "@")
			if len(ary) > 1 {
				rich["user_domain"] = strings.TrimSpace(ary[1])
			}
		}
		rich["user_org"], _ = user["company"]
		rich["user_location"], _ = user["location"]
		rich["user_geolocation"] = nil
	} else {
		rich["author_login"] = nil
		rich["author_name"] = nil
		rich["author_avatar_url"] = nil
		rich["user_avatar_url"] = nil
		rich["user_name"] = nil
		rich["user_domain"] = nil
		rich["user_org"] = nil
		rich["user_location"] = nil
		rich["user_geolocation"] = nil
	}
	iAssigneeData, ok := pull["assignee_data"]
	if ok && iAssigneeData != nil {
		assignee, _ := iAssigneeData.(map[string]interface{})
		rich["assignee_login"], _ = assignee["login"]
		rich["assignee_name"], _ = assignee["name"]
		rich["assignee_avatar_url"], _ = assignee["avatar_url"]
		rich["assignee_domain"] = nil
		iEmail, ok := assignee["email"]
		if ok {
			email, _ := iEmail.(string)
			ary := strings.Split(email, "@")
			if len(ary) > 1 {
				rich["assignee_domain"] = strings.TrimSpace(ary[1])
			}
		}
		rich["assignee_org"], _ = assignee["company"]
		rich["assignee_location"], _ = assignee["location"]
		rich["assignee_geolocation"] = nil
	} else {
		rich["assignee_login"] = nil
		rich["assignee_name"] = nil
		rich["assignee_avatar_url"] = nil
		rich["assignee_domain"] = nil
		rich["assignee_org"] = nil
		rich["assignee_location"] = nil
		rich["assignee_geolocation"] = nil
	}
	iMergedByData, ok := pull["merged_by_data"]
	if ok && iMergedByData != nil {
		mergedBy, _ := iMergedByData.(map[string]interface{})
		rich["merge_author_login"], _ = mergedBy["login"]
		rich["merge_author_name"], _ = mergedBy["name"]
		rich["merge_author_avatar_url"], _ = mergedBy["avatar_url"]
		rich["merge_author_domain"] = nil
		iEmail, ok := mergedBy["email"]
		if ok {
			email, _ := iEmail.(string)
			ary := strings.Split(email, "@")
			if len(ary) > 1 {
				rich["merge_author_domain"] = strings.TrimSpace(ary[1])
			}
		}
		rich["merge_author_org"], _ = mergedBy["company"]
		rich["merge_author_location"], _ = mergedBy["location"]
		rich["merge_author_geolocation"] = nil
	} else {
		rich["merge_author_login"] = nil
		rich["merge_author_name"] = nil
		rich["merge_author_avatar_url"] = nil
		rich["merge_author_domain"] = nil
		rich["merge_author_org"] = nil
		rich["merge_author_location"] = nil
		rich["merge_author_geolocation"] = nil
	}
	iLabels, ok := pull["labels"]
	if ok && iLabels != nil {
		ary, _ := iLabels.([]interface{})
		labels := []interface{}{}
		for _, iLabel := range ary {
			label, _ := iLabel.(map[string]interface{})
			iLabelName, _ := label["name"]
			labelName, _ := iLabelName.(string)
			if labelName != "" {
				labels = append(labels, labelName)
			}
		}
		rich["labels"] = labels
	}
	nAssignees := 0
	iAssignees, ok := pull["assignees_data"]
	if ok && iAssignees != nil {
		ary, _ := iAssignees.([]interface{})
		nAssignees = len(ary)
		assignees := []interface{}{}
		for _, iAssignee := range ary {
			assignee, _ := iAssignee.(map[string]interface{})
			iAssigneeLogin, _ := assignee["login"]
			assigneeLogin, _ := iAssigneeLogin.(string)
			if assigneeLogin != "" {
				assignees = append(assignees, assigneeLogin)
			}
		}
		rich["assignees_data"] = assignees
	}
	rich["n_assignees"] = nAssignees
	nRequestedReviewers := 0
	iRequestedReviewers, ok := pull["requested_reviewers_data"]
	if ok && iRequestedReviewers != nil {
		ary, _ := iRequestedReviewers.([]interface{})
		nRequestedReviewers = len(ary)
		requestedReviewers := []interface{}{}
		for _, iRequestedReviewer := range ary {
			requestedReviewer, _ := iRequestedReviewer.(map[string]interface{})
			iRequestedReviewerLogin, _ := requestedReviewer["login"]
			requestedReviewerLogin, _ := iRequestedReviewerLogin.(string)
			if requestedReviewerLogin != "" {
				requestedReviewers = append(requestedReviewers, requestedReviewerLogin)
			}
		}
		rich["requested_reviewers_data"] = requestedReviewers
	}
	rich["n_requested_reviewers"] = nRequestedReviewers
	nCommenters := 0
	nComments := 0
	reactions := 0
	iComments, ok := pull["review_comments_data"]
	if ok && iComments != nil {
		ary, _ := iComments.([]interface{})
		nComments = len(ary)
		commenters := map[string]interface{}{}
		for _, iComment := range ary {
			comment, _ := iComment.(map[string]interface{})
			iCommenter, _ := Dig(comment, []string{"user", "login"}, false, true)
			commenter, _ := iCommenter.(string)
			if commenter != "" {
				commenters[commenter] = struct{}{}
			}
			iReactions, ok := Dig(comment, []string{"reactions", "total_count"}, false, true)
			if ok {
				reacts := int(iReactions.(float64))
				reactions += reacts
			}
		}
		nCommenters = len(commenters)
		comms := []string{}
		for commenter := range commenters {
			comms = append(comms, commenter)
		}
		rich["commenters"] = comms
	}
	rich["n_commenters"] = nCommenters
	rich["n_comments"] = nComments
	nReviewCommenters := 0
	nReviewComments := 0
	iReviewComments, ok := pull["reviews_data"]
	if ok && iReviewComments != nil {
		ary, _ := iReviewComments.([]interface{})
		nReviewComments = len(ary)
		reviewCommenters := map[string]interface{}{}
		for _, iReviewComment := range ary {
			reviewComment, _ := iReviewComment.(map[string]interface{})
			iReviewCommenter, _ := Dig(reviewComment, []string{"user", "login"}, false, true)
			reviewCommenter, _ := iReviewCommenter.(string)
			if reviewCommenter != "" {
				reviewCommenters[reviewCommenter] = struct{}{}
			}
		}
		nReviewCommenters = len(reviewCommenters)
		revComms := []string{}
		for reviewCommenter := range reviewCommenters {
			revComms = append(revComms, reviewCommenter)
		}
		rich["review_commenters"] = revComms
	}
	rich["n_review_commenters"] = nReviewCommenters
	rich["n_review_comments"] = nReviewComments
	rich["pull_request"] = true
	rich["item_type"] = "pull request"
	githubRepo := j.URL
	if strings.HasSuffix(githubRepo, ".git") {
		githubRepo = githubRepo[:len(githubRepo)-4]
	}
	if strings.Contains(githubRepo, GitHubURLRoot) {
		githubRepo = strings.Replace(githubRepo, GitHubURLRoot, "", -1)
	}
	var repoShortName string
	arr := strings.Split(githubRepo, "/")
	if len(arr) > 1 {
		repoShortName = arr[1]
	}
	rich["repo_short_name"] = repoShortName
	rich["github_repo"] = githubRepo
	rich["url_id"] = fmt.Sprintf("%s/pull/%d", githubRepo, number)
	rich["forks"], _ = Dig(pull, []string{"base", "repo", "forks_count"}, false, true)
	rich["num_review_comments"], _ = pull["review_comments"]
	if iMergedAt != nil {
		mergedAt, e := TimeParseInterfaceString(iMergedAt)
		if e == nil {
			rich["code_merge_duration"] = float64(mergedAt.Sub(createdAt).Seconds()) / 86400.0
		} else {
			rich["code_merge_duration"] = nil
		}
	} else {
		rich["code_merge_duration"] = nil
	}
	commentsVal := 0
	iCommentsVal, ok := pull["comments"]
	if ok {
		commentsVal = int(iCommentsVal.(float64))
	}
	rich["n_total_comments"] = commentsVal
	// There is probably no value for "reactions", "total_count" on the top level of "pull" object, but we can attempt to get this
	iReactions, ok := Dig(pull, []string{"reactions", "total_count"}, false, true)
	if ok {
		reacts := int(iReactions.(float64))
		reactions += reacts
	}
	rich["n_reactions"] = reactions
	rich["time_to_merge_request_response"] = nil
	if nComments > 0 {
		firstReviewDate := j.GetFirstPullRequestReviewDate(pull, false)
		rich["time_to_merge_request_response"] = float64(firstReviewDate.Sub(createdAt).Seconds()) / 86400.0
	}
	if nReviewComments > 0 || nComments > 0 {
		firstAttentionDate := j.GetFirstPullRequestReviewDate(pull, true)
		rich["time_to_first_attention"] = float64(firstAttentionDate.Sub(createdAt).Seconds()) / 86400.0
	}
	rich[j.DateField(ctx)] = createdAt
	if affs {
		authorKey := "user_data"
		var affsItems map[string]interface{}
		affsItems, err = j.AffsItems(ctx, pull, GitHubPullRequestRoles, createdAt)
		if err != nil {
			return
		}
		for prop, value := range affsItems {
			rich[prop] = value
		}
		for _, suff := range AffsFields {
			rich[Author+suff] = rich[authorKey+suff]
		}
		orgsKey := authorKey + MultiOrgNames
		_, ok := Dig(rich, []string{orgsKey}, false, true)
		if !ok {
			rich[orgsKey] = []interface{}{}
		}
	}
	for prop, value := range CommonFields(j, createdAt, j.Category) {
		rich[prop] = value
	}
	return
}

// GetFirstPullRequestReviewDate - get first review date on a pull request
func (j *DSGitHub) GetFirstPullRequestReviewDate(pull map[string]interface{}, commsAndReviews bool) (dt time.Time) {
	iUserLogin, _ := Dig(pull, []string{"user", "login"}, false, true)
	userLogin, _ := iUserLogin.(string)
	dts := []time.Time{}
	udts := []time.Time{}
	iReviews, ok := pull["review_comments_data"]
	if ok && iReviews != nil {
		ary, _ := iReviews.([]interface{})
		for _, iReview := range ary {
			review, _ := iReview.(map[string]interface{})
			iReviewLogin, _ := Dig(review, []string{"user", "login"}, false, true)
			reviewLogin, _ := iReviewLogin.(string)
			iCreatedAt, _ := review["created_at"]
			createdAt, _ := TimeParseInterfaceString(iCreatedAt)
			if userLogin == reviewLogin {
				udts = append(udts, createdAt)
				continue
			}
			dts = append(dts, createdAt)
		}
	}
	if commsAndReviews {
		iReviews, ok := pull["reviews_data"]
		if ok && iReviews != nil {
			ary, _ := iReviews.([]interface{})
			for _, iReview := range ary {
				review, _ := iReview.(map[string]interface{})
				iReviewLogin, _ := Dig(review, []string{"user", "login"}, false, true)
				reviewLogin, _ := iReviewLogin.(string)
				iSubmittedAt, _ := review["submitted_at"]
				submittedAt, _ := TimeParseInterfaceString(iSubmittedAt)
				if userLogin == reviewLogin {
					udts = append(udts, submittedAt)
					continue
				}
				dts = append(dts, submittedAt)
			}
		}
	}
	nDts := len(dts)
	if nDts == 0 {
		// If there was no review of anybody else that author's, then fallback to author's review
		dts = udts
		nDts = len(dts)
	}
	switch nDts {
	case 0:
		dt = time.Now()
	case 1:
		dt = dts[0]
	default:
		sort.Slice(dts, func(i, j int) bool {
			return dts[i].Before(dts[j])
		})
		dt = dts[0]
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
	if ctx.Project != "" {
		rich["project"] = ctx.Project
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
	rich[j.DateField(ctx)] = updatedOn
	for prop, value := range CommonFields(j, updatedOn, j.Category) {
		rich[prop] = value
	}
	rich["type"] = j.Category
	rich["category"] = j.Category
	return
}

// AffsItems - return affiliations data items for given roles and date
func (j *DSGitHub) AffsItems(ctx *Ctx, item map[string]interface{}, roles []string, date interface{}) (affsItems map[string]interface{}, err error) {
	affsItems = make(map[string]interface{})
	dt, _ := date.(time.Time)
	for _, role := range roles {
		identity := j.GetRoleIdentity(ctx, item, role)
		if len(identity) == 0 {
			continue
		}
		affsIdentity, empty, e := IdentityAffsData(ctx, j, identity, nil, dt, role)
		if e != nil {
			Printf("%s/%s: AffsItems/IdentityAffsData: error: %v for %v,%v,%v\n", j.URL, j.Category, e, identity, dt, role)
		}
		if empty {
			Printf("%s/%s: no identity affiliation data for identity %+v, role %s\n", j.URL, j.Category, identity, role)
			continue
		}
		if ctx.Debug > 2 {
			Printf("%s/%s: Identity affiliation data for %+v: %+v\n", j.URL, j.Category, identity, affsIdentity)
		}
		for prop, value := range affsIdentity {
			affsItems[prop] = value
		}
		for _, suff := range RequiredAffsFields {
			k := role + suff
			_, ok := affsIdentity[k]
			if !ok {
				affsIdentity[k] = Unknown
			}
		}
	}
	return
}

// GetRoleIdentity - return identity data for a given role
func (j *DSGitHub) GetRoleIdentity(ctx *Ctx, item map[string]interface{}, role string) (identity map[string]interface{}) {
	user, ok := item[role]
	if ok && user != nil && len(user.(map[string]interface{})) > 0 {
		ident := j.IdentityForObject(ctx, user.(map[string]interface{}))
		identity = map[string]interface{}{
			"name":     ident[0],
			"username": ident[1],
			"email":    ident[2],
		}
	}
	return
}

// AllRoles - return all roles defined for the backend
// roles can be static (always the same) or dynamic (per item)
// second return parameter is static mode (true/false)
// dynamic roles will use item to get its roles
func (j *DSGitHub) AllRoles(ctx *Ctx, rich map[string]interface{}) (roles []string, static bool) {
	if ctx.Debug > 0 && j.Category != "repository" {
		defer func() {
			id, _ := rich["id"]
			uuid, _ := rich["uuid"]
			fmt.Printf("%s/%s: AllRoles(%v, %v) --> {%v, %+v}\n", j.URL, j.Category, id, uuid, static, roles)
		}()
	}
	var possibleRoles []string
	switch j.Category {
	case "repository":
		static = true
		return
	case "issue":
		roles = []string{Author}
		if rich == nil {
			return
		}
		typ, ok := rich["type"]
		if ok {
			switch typ.(string) {
			case "issue":
				possibleRoles = GitHubIssueRoles
			case "issue_comment":
				possibleRoles = GitHubIssueCommentRoles
				possibleRoles = append(possibleRoles, "commenter")
			case "issue_assignee":
				possibleRoles = GitHubIssueAssigneeRoles
			case "issue_reaction":
				possibleRoles = GitHubIssueReactionRoles
				possibleRoles = append(possibleRoles, "actor")
			case "issue_comment_reaction":
				possibleRoles = GitHubIssueReactionRoles
				possibleRoles = append(possibleRoles, "actor")
			}
		}
	case "pull_request":
		roles = []string{Author}
		if rich == nil {
			return
		}
		typ, ok := rich["type"]
		if ok {
			switch typ.(string) {
			case "pull_request":
				possibleRoles = GitHubPullRequestRoles
			case "pull_request_comment":
				possibleRoles = GitHubPullRequestCommentRoles
				possibleRoles = append(possibleRoles, "commenter")
			case "pull_request_assignee":
				possibleRoles = GitHubPullRequestAssigneeRoles
			case "pull_request_comment_reaction":
				possibleRoles = GitHubPullRequestReactionRoles
				possibleRoles = append(possibleRoles, "actor")
			case "pull_request_requested_reviewer":
				possibleRoles = GitHubPullRequestRequestedReviewerRoles
			case "pull_request_review":
				possibleRoles = GitHubPullRequestReviewRoles
				possibleRoles = append(possibleRoles, "reviewer")
			}
		}
	}
	for _, possibleRole := range possibleRoles {
		_, ok := Dig(rich, []string{possibleRole + "_id"}, false, true)
		if ok {
			roles = append(roles, possibleRole)
		}
	}
	return
}

// CalculateTimeToReset - calculate time to reset rate limits based on rate limit value and rate limit reset value
func (j *DSGitHub) CalculateTimeToReset(ctx *Ctx, rateLimit, rateLimitReset int) (seconds int) {
	seconds = rateLimitReset
	return
}

// HasIdentities - does this data source support identity data
func (j *DSGitHub) HasIdentities() bool {
	return j.Category != "repository"
}

// UseDefaultMapping - apply MappingNotAnalyzeString for raw/rich (raw=fals/true) index in this DS?
func (j *DSGitHub) UseDefaultMapping(ctx *Ctx, raw bool) bool {
	return raw
}

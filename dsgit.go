package dads

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
)

const (
	// GitBackendVersion - backend version
	GitBackendVersion = "0.0.1"
	// GitDefaultReposPath - default path where git repository clones
	GitDefaultReposPath = "$HOME/.perceval/repositories"
	// GitDefaultCachePath - default path where gitops cache files are stored
	GitDefaultCachePath = "$HOME/.perceval/cache"
	// GitOpsCommand - command that maintains git stats cache
	GitOpsCommand = "gitops.py"
	// GitOpsNoCleanup - if set, it will skip gitops.py repo cleanup
	// FIXME: turn off when finshed
	GitOpsNoCleanup = true
	// GitParseStateInit - init parser state
	GitParseStateInit = 0
	// GitParseStateCommit - commit parser state
	GitParseStateCommit = 1
	// GitParseStateHeader - header parser state
	GitParseStateHeader = 2
	// GitParseStateMessage - message parser state
	GitParseStateMessage = 3
	// GitParseStateFile - file parser state
	GitParseStateFile = 4
	// GitCommitDateField - date field in the commit structure
	GitCommitDateField = "CommitDate"
	// GitDefaultSearchField - default search field
	GitDefaultSearchField = "item_id"
)

var (
	// GitRawMapping - Git raw index mapping
	GitRawMapping = []byte(`{"dynamic":true,"properties":{"metadata__updated_on":{"type":"date"},"data":{"properties":{"message":{"type":"text","index":true}}}}}`)
	// GitRichMapping - Git rich index mapping
	GitRichMapping = []byte(`{"properties":{"metadata__updated_on":{"type":"date"},"message_analyzed":{"type":"text","index":true}}}`)
	// GitCategories - categories defined for Groupsio
	GitCategories = map[string]struct{}{"commit": {}}
	// GitDefaultEnv - default git command environment
	GitDefaultEnv = map[string]string{"LANG": "C", "PAGER": ""}
	// GitLogOptions - default git log options
	GitLogOptions = []string{
		"--raw",           // show data in raw format
		"--numstat",       // show added/deleted lines per file
		"--pretty=fuller", // pretty output
		"--decorate=full", // show full refs
		"--parents",       //show parents information
		"-M",              //detect and report renames
		"-C",              //detect and report copies
		"-c",              //show merge info
	}
	// GitCommitPattern - pattern to match a commit
	GitCommitPattern = regexp.MustCompile(`^commit[ \t](?P<commit>[a-f0-9]{40})(?:[ \t](?P<parents>[a-f0-9][a-f0-9 \t]+))?(?:[ \t]\((?P<refs>.+)\))?$`)
	// GitHeaderPattern - pattern to match a commit
	GitHeaderPattern = regexp.MustCompile(`^(?P<name>[a-zA-z0-9\-]+)\:[ \t]+(?P<value>.+)$`)
	// GitMessagePattern - message patterns
	GitMessagePattern = regexp.MustCompile(`^[\s]{4}(?P<msg>.*)$`)
	// GitTrailerPattern - message trailer pattern
	GitTrailerPattern = regexp.MustCompile(`^(?P<name>[a-zA-z0-9\-]+)\:[ \t]+(?P<value>.+)$`)
	// GitActionPattern - action pattern - note that original used `\.{,3}` which is not supported in go - you must specify from=0: `\.{0,3}`
	GitActionPattern = regexp.MustCompile(`^(?P<sc>\:+)(?P<modes>(?:\d{6}[ \t])+)(?P<indexes>(?:[a-f0-9]+\.{0,3}[ \t])+)(?P<action>[^\t]+)\t+(?P<file>[^\t]+)(?:\t+(?P<newfile>.+))?$`)
	// GitStatsPattern - stats pattern
	GitStatsPattern = regexp.MustCompile(`^(?P<added>\d+|-)[ \t]+(?P<removed>\d+|-)[ \t]+(?P<file>.+)$`)
	// GitAuthorsPattern - author pattern
	GitAuthorsPattern = regexp.MustCompile(`(?P<first_authors>.* .*) and (?P<last_author>.* .*) (?P<email>.*)`)
	// GitCoAuthorsPattern - author pattern
	GitCoAuthorsPattern = regexp.MustCompile(`Co-authored-by:(?P<first_authors>.* .*)<(?P<email>.*)>\n?`)
)

// RawPLS - programming language summary (all fields as strings)
type RawPLS struct {
	Language string `json:"language"`
	Files    string `json:"files"`
	Blank    string `json:"blank"`
	Comment  string `json:"comment"`
	Code     string `json:"code"`
}

// PLS - programming language summary
type PLS struct {
	Language string `json:"language"`
	Files    int    `json:"files"`
	Blank    int    `json:"blank"`
	Comment  int    `json:"comment"`
	Code     int    `json:"code"`
}

// DSGit - DS implementation for git
type DSGit struct {
	DS              string
	URL             string // From DA_GIT_URL - git repo path
	SingleOrigin    bool   // From DA_GIT_SINGLE_ORIGIN - if you want to store only one git endpoint in the index
	ReposPath       string // From DA_GIT_REPOS_PATH - default GitDefaultReposPath
	CachePath       string // From DA_GIT_CACHE_PATH - default GitDefaultCachePath
	NoSSLVerify     bool   // From DA_GIT_NO_SSL_VERIFY
	PairProgramming bool   // From DA_GIT_PAIR_PROGRAMMING
	// Non-config variables
	RepoName    string                            // repo name
	Loc         int                               // lines of code as reported by GitOpsCommand
	Pls         []PLS                             // programming language suppary as reported by GitOpsCommand
	GitPath     string                            // path to git repo clone
	LineScanner *bufio.Scanner                    // line scanner for git log
	CurrLine    int                               // current line in git log
	ParseState  int                               // 0-init, 1-commit, 2-header, 3-message, 4-file
	Commit      map[string]interface{}            // current parsed commit
	CommitFiles map[string]map[string]interface{} // current commit's files
}

// ParseArgs - parse git specific environment variables
func (j *DSGit) ParseArgs(ctx *Ctx) (err error) {
	j.DS = Git
	prefix := "DA_GIT_"
	j.URL = os.Getenv(prefix + "URL")
	j.SingleOrigin = StringToBool(os.Getenv(prefix + "SINGLE_ORIGIN"))
	j.PairProgramming = StringToBool(os.Getenv(prefix + "PAIR_PROGRAMMING"))
	if os.Getenv(prefix+"REPOS_PATH") != "" {
		j.ReposPath = os.Getenv(prefix + "REPOS_PATH")
	} else {
		j.ReposPath = GitDefaultReposPath
	}
	if os.Getenv(prefix+"CACHE_PATH") != "" {
		j.CachePath = os.Getenv(prefix + "REPOS_PATH")
	} else {
		j.CachePath = GitDefaultCachePath
	}
	j.NoSSLVerify = StringToBool(os.Getenv(prefix + "NO_SSL_VERIFY"))
	if j.NoSSLVerify {
		NoSSLVerify()
	}
	return
}

// Validate - is current DS configuration OK?
func (j *DSGit) Validate() (err error) {
	url := strings.TrimSpace(j.URL)
	if strings.HasSuffix(url, "/") {
		url = url[:len(url)-1]
	}
	ary := strings.Split(url, "/")
	j.RepoName = ary[len(ary)-1]
	if j.RepoName == "" {
		err = fmt.Errorf("Repo name must be set")
	}
	j.ReposPath = os.ExpandEnv(j.ReposPath)
	if strings.HasSuffix(j.ReposPath, "/") {
		j.ReposPath = j.ReposPath[:len(j.ReposPath)-1]
	}
	j.CachePath = os.ExpandEnv(j.CachePath)
	if strings.HasSuffix(j.CachePath, "/") {
		j.CachePath = j.CachePath[:len(j.CachePath)-1]
	}
	return
}

// Name - return data source name
func (j *DSGit) Name() string {
	return j.DS
}

// Info - return DS configuration in a human readable form
func (j DSGit) Info() string {
	return fmt.Sprintf("%+v", j)
}

// CustomFetchRaw - is this datasource using custom fetch raw implementation?
func (j *DSGit) CustomFetchRaw() bool {
	return false
}

// FetchRaw - implement fetch raw data for git datasource
func (j *DSGit) FetchRaw(ctx *Ctx) (err error) {
	Printf("%s should use generic FetchRaw()\n", j.DS)
	return
}

// CustomEnrich - is this datasource using custom enrich implementation?
func (j *DSGit) CustomEnrich() bool {
	return false
}

// Enrich - implement enrich data for git datasource
func (j *DSGit) Enrich(ctx *Ctx) (err error) {
	Printf("%s should use generic Enrich()\n", j.DS)
	return
}

// GetGitOps - LOC, lang summary stats
func (j *DSGit) GetGitOps(ctx *Ctx, thrN int) (ch chan error, err error) {
	worker := func(c chan error, url string) (e error) {
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		var (
			sout string
			serr string
		)
		cmdLine := []string{GitOpsCommand, url}
		var env map[string]string
		if GitOpsNoCleanup {
			env = map[string]string{"SKIP_CLEANUP": "1"}
		}
		sout, serr, e = ExecCommand(ctx, cmdLine, "", env)
		if e != nil {
			Printf("error executing %v: %v\n%s\n%s\n", cmdLine, e, sout, serr)
			return
		}
		type resultType struct {
			Loc int      `json:"loc"`
			Pls []RawPLS `json:"pls"`
		}
		var data resultType
		e = jsoniter.Unmarshal([]byte(sout), &data)
		if e != nil {
			Printf("error unmarshaling from %v\n", sout)
			return
		}
		j.Loc = data.Loc
		for _, f := range data.Pls {
			files, _ := strconv.Atoi(f.Files)
			blank, _ := strconv.Atoi(f.Blank)
			comment, _ := strconv.Atoi(f.Comment)
			code, _ := strconv.Atoi(f.Code)
			j.Pls = append(
				j.Pls,
				PLS{
					Language: f.Language,
					Files:    files,
					Blank:    blank,
					Comment:  comment,
					Code:     code,
				},
			)
		}
		return
	}
	if thrN <= 1 {
		return nil, worker(nil, j.URL)
	}
	ch = make(chan error)
	go func() { _ = worker(ch, j.URL) }()
	return ch, nil
}

// CreateGitRepo - clone git repo if needed
func (j *DSGit) CreateGitRepo(ctx *Ctx) (err error) {
	info, err := os.Stat(j.GitPath)
	var exists bool
	if !os.IsNotExist(err) {
		if info.IsDir() {
			exists = true
		} else {
			err = fmt.Errorf("%s exists and is a file, not a directory", j.GitPath)
			return
		}
	}
	if !exists {
		if ctx.Debug > 0 {
			Printf("cloning %s to %s\n", j.URL, j.GitPath)
		}
		cmdLine := []string{"git", "clone", "--bare", j.URL, j.GitPath}
		env := map[string]string{"LANG": "C"}
		var sout, serr string
		sout, serr, err = ExecCommand(ctx, cmdLine, "", env)
		if err != nil {
			Printf("error executing %v: %v\n%s\n%s\n", cmdLine, err, sout, serr)
			return
		}
		if ctx.Debug > 0 {
			Printf("cloned %s to %s\n", j.URL, j.GitPath)
		}
	}
	headPath := j.GitPath + "/HEAD"
	info, err = os.Stat(headPath)
	if os.IsNotExist(err) {
		Printf("Missing %s file\n", headPath)
		return
	}
	if info.IsDir() {
		err = fmt.Errorf("%s is a directory, not file", headPath)
	}
	return
}

// UpdateGitRepo - update git repo
func (j *DSGit) UpdateGitRepo(ctx *Ctx) (err error) {
	if ctx.Debug > 0 {
		Printf("updating repo %s\n", j.URL)
	}
	cmdLine := []string{"git", "fetch", "origin", "+refs/heads/*:refs/heads/*", "--prune"}
	var sout, serr string
	sout, serr, err = ExecCommand(ctx, cmdLine, j.GitPath, GitDefaultEnv)
	if err != nil {
		Printf("error executing %v: %v\n%s\n%s\n", cmdLine, err, sout, serr)
		return
	}
	if ctx.Debug > 0 {
		Printf("updated repo %s\n", j.URL)
	}
	return
}

// ParseGitLog - update git repo
func (j *DSGit) ParseGitLog(ctx *Ctx) (cmd *exec.Cmd, err error) {
	if ctx.Debug > 0 {
		Printf("parsing logs from %s\n", j.GitPath)
	}
	cmdLine := []string{"git", "log", "--reverse", "--topo-order", "--branches", "--tags", "--remotes=origin"}
	cmdLine = append(cmdLine, GitLogOptions...)
	if ctx.DateFrom != nil {
		cmdLine = append(cmdLine, "--since="+ToYMDHMSDate(*ctx.DateFrom))
	}
	if ctx.DateTo != nil {
		cmdLine = append(cmdLine, "--until="+ToYMDHMSDate(*ctx.DateTo))
	}
	var pipe io.ReadCloser
	pipe, cmd, err = ExecCommandPipe(ctx, cmdLine, j.GitPath, GitDefaultEnv)
	if err != nil {
		Printf("error executing %v: %v\n", cmdLine, err)
		return
	}
	j.LineScanner = bufio.NewScanner(pipe)
	if ctx.Debug > 0 {
		Printf("created logs scanner %s\n", j.GitPath)
	}
	return
}

// BuildCommit - return commit structure from the current parsed object
func (j *DSGit) BuildCommit(ctx *Ctx) (commit map[string]interface{}) {
	if ctx.Debug > 2 {
		defer func() {
			Printf("built commit %+v\n", commit)
		}()
	}
	commit = j.Commit
	ks := []string{}
	for k, v := range commit {
		if v == nil {
			ks = append(ks, k)
		}
	}
	for _, k := range ks {
		delete(commit, k)
	}
	files := []map[string]interface{}{}
	sf := []string{}
	for f := range j.CommitFiles {
		sf = append(sf, f)
	}
	sort.Strings(sf)
	for _, f := range sf {
		d := j.CommitFiles[f]
		ks = []string{}
		for k, v := range d {
			if v == nil {
				ks = append(ks, k)
			}
		}
		for _, k := range ks {
			delete(d, k)
		}
		files = append(files, d)
	}
	commit["files"] = files
	j.Commit = nil
	j.CommitFiles = nil
	return
}

// ParseInit - parse initial state
func (j *DSGit) ParseInit(ctx *Ctx, line string) (parsed bool, err error) {
	j.ParseState = GitParseStateCommit
	parsed = line == ""
	return
}

// ParseCommit - parse commit
func (j *DSGit) ParseCommit(ctx *Ctx, line string) (parsed bool, err error) {
	m := MatchGroups(GitCommitPattern, line)
	if len(m) == 0 {
		err = fmt.Errorf("expecting commit on line %d: '%s'", j.CurrLine, line)
		return
	}
	parentsAry := []string{}
	refsAry := []string{}
	parents, parentsPresent := m["parents"]
	if parentsPresent && parents != "" {
		parentsAry = strings.Split(strings.TrimSpace(parents), " ")
	}
	refs, refsPresent := m["refs"]
	if refsPresent && refs != "" {
		ary := strings.Split(strings.TrimSpace(refs), ",")
		for _, ref := range ary {
			ref = strings.TrimSpace(ref)
			if ref != "" {
				refsAry = append(refsAry, ref)
			}
		}
	}
	j.Commit = make(map[string]interface{})
	j.Commit["commit"] = m["commit"]
	j.Commit["parents"] = parentsAry
	j.Commit["refs"] = refsAry
	j.CommitFiles = make(map[string]map[string]interface{})
	j.ParseState = GitParseStateHeader
	parsed = true
	return
}

// ParseHeader - parse header state
func (j *DSGit) ParseHeader(ctx *Ctx, line string) (parsed bool, err error) {
	if line == "" {
		j.ParseState = GitParseStateMessage
		parsed = true
		return
	}
	m := MatchGroups(GitHeaderPattern, line)
	if len(m) == 0 {
		err = fmt.Errorf("invalid header format, line %d: '%s'", j.CurrLine, line)
		return
	}
	if m["name"] != "" {
		j.Commit[m["name"]] = m["value"]
	}
	parsed = true
	return
}

// ParseMessage - parse message state
func (j *DSGit) ParseMessage(ctx *Ctx, line string) (parsed bool, err error) {
	if line == "" {
		j.ParseState = GitParseStateFile
		parsed = true
		return
	}
	m := MatchGroups(GitMessagePattern, line)
	if len(m) == 0 {
		if ctx.Debug > 1 {
			Printf("invalid message format, line %d: '%s'", j.CurrLine, line)
		}
		j.ParseState = GitParseStateFile
		return
	}
	msg := m["msg"]
	currMsg, ok := j.Commit["message"]
	if ok {
		sMsg, _ := currMsg.(string)
		j.Commit["message"] = sMsg + "\n" + msg
	} else {
		j.Commit["message"] = msg
	}
	j.ParseTrailer(ctx, msg)
	parsed = true
	return
}

// ParseAction - parse action line
func (j *DSGit) ParseAction(ctx *Ctx, data map[string]string) {
	var (
		modesAry   []string
		indexesAry []string
	)
	modes, modesPresent := data["modes"]
	if modesPresent && modes != "" {
		modesAry = strings.Split(strings.TrimSpace(modes), " ")
	}
	indexes, indexesPresent := data["indexes"]
	if indexesPresent && indexes != "" {
		indexesAry = strings.Split(strings.TrimSpace(indexes), " ")
	}
	fileName := data["file"]
	_, ok := j.CommitFiles[fileName]
	if !ok {
		j.CommitFiles[fileName] = make(map[string]interface{})
	}
	j.CommitFiles[fileName]["modes"] = modesAry
	j.CommitFiles[fileName]["indexes"] = indexesAry
	j.CommitFiles[fileName]["action"] = data["action"]
	j.CommitFiles[fileName]["file"] = fileName
	j.CommitFiles[fileName]["newfile"] = data["newfile"]
}

// ExtractPrevFileName - extracts previous file name (before rename/move etc.)
func (*DSGit) ExtractPrevFileName(f string) (res string) {
	i := strings.Index(f, "{")
	j := strings.Index(f, "}")
	if i > -1 && j > -1 {
		k := IndexAt(f, " => ", i)
		prefix := f[:i]
		inner := f[i+1 : k]
		suffix := f[j+1:]
		res = prefix + inner + suffix
	} else if strings.Index(f, " => ") > -1 {
		res = strings.Split(f, " => ")[0]
	} else {
		res = f
	}
	return
}

// ParseStats - parse stats line
func (j *DSGit) ParseStats(ctx *Ctx, data map[string]string) {
	fileName := j.ExtractPrevFileName(data["file"])
	_, ok := j.CommitFiles[fileName]
	if !ok {
		j.CommitFiles[fileName] = make(map[string]interface{})
		j.CommitFiles[fileName]["file"] = fileName
	}
	j.CommitFiles[fileName]["added"] = data["added"]
	j.CommitFiles[fileName]["removed"] = data["removed"]
}

// ParseFile - parse file state
func (j *DSGit) ParseFile(ctx *Ctx, line string) (parsed bool, err error) {
	if line == "" {
		j.ParseState = GitParseStateCommit
		parsed = true
		return
	}
	m := MatchGroups(GitActionPattern, line)
	if len(m) > 0 {
		j.ParseAction(ctx, m)
		parsed = true
		return
	}
	m = MatchGroups(GitStatsPattern, line)
	if len(m) > 0 {
		j.ParseStats(ctx, m)
		parsed = true
		return
	}
	if ctx.Debug > 1 {
		Printf("invalid file section format, line %d: '%s'", j.CurrLine, line)
	}
	j.ParseState = GitParseStateCommit
	return
}

// ParseTrailer - parse possible trailer line
func (j *DSGit) ParseTrailer(ctx *Ctx, line string) {
	m := MatchGroups(GitTrailerPattern, line)
	if len(m) == 0 {
		return
	}
	trailer := m["name"]
	ary, ok := j.Commit[trailer]
	if ok {
		if ctx.Debug > 1 {
			Printf("trailer %s found in '%s'\n", trailer, line)
		}
		j.Commit[trailer] = append(ary.([]interface{}), m["value"])
	} else {
		j.Commit[trailer] = []interface{}{m["value"]}
	}
}

// ParseNextCommit - parse next git log commit or report end
func (j *DSGit) ParseNextCommit(ctx *Ctx) (commit map[string]interface{}, ok bool, err error) {
	for j.LineScanner.Scan() {
		j.CurrLine++
		line := strings.TrimRight(j.LineScanner.Text(), "\n")
		parsed := false
		for {
			switch j.ParseState {
			case GitParseStateInit:
				parsed, err = j.ParseInit(ctx, line)
			case GitParseStateCommit:
				parsed, err = j.ParseCommit(ctx, line)
			case GitParseStateHeader:
				parsed, err = j.ParseHeader(ctx, line)
			case GitParseStateMessage:
				parsed, err = j.ParseMessage(ctx, line)
			case GitParseStateFile:
				parsed, err = j.ParseFile(ctx, line)
			default:
				err = fmt.Errorf("unknown parse state:%d", j.ParseState)
			}
			if err != nil {
				Printf("parse next line '%s' error: %v\n", line, err)
				return
			}
			if j.ParseState == GitParseStateCommit && j.Commit != nil {
				commit = j.BuildCommit(ctx)
				ok = true
				return
			}
			if parsed {
				break
			}
		}
	}
	if j.Commit != nil {
		commit = j.BuildCommit(ctx)
		ok = true
	}
	return
}

// FetchItems - implement enrich data for git datasource
func (j *DSGit) FetchItems(ctx *Ctx) (err error) {
	var (
		ch            chan error
		allCommits    []interface{}
		allCommitsMtx *sync.Mutex
		escha         []chan error
		eschaMtx      *sync.Mutex
		goch          chan error
		waitLOCMtx    *sync.Mutex
	)
	thrN := GetThreadsNum(ctx)
	if thrN > 1 {
		ch = make(chan error)
		allCommitsMtx = &sync.Mutex{}
		eschaMtx = &sync.Mutex{}
		waitLOCMtx = &sync.Mutex{}
		goch, _ = j.GetGitOps(ctx, thrN)
	} else {
		_, err = j.GetGitOps(ctx, thrN)
		if err != nil {
			return
		}
	}
	// Do normal git processing, which don't needs gitops yet
	j.GitPath = j.ReposPath + "/" + j.URL + "-git"
	j.GitPath, err = EnsurePath(j.GitPath, true)
	FatalOnError(err)
	if ctx.Debug > 0 {
		Printf("path to store git repository: %s\n", j.GitPath)
	}
	FatalOnError(j.CreateGitRepo(ctx))
	FatalOnError(j.UpdateGitRepo(ctx))
	var cmd *exec.Cmd
	cmd, err = j.ParseGitLog(ctx)
	// Continue with operations that need git ops
	nThreads := 0
	locFinished := false
	waitForLOC := func() (e error) {
		if thrN == 1 {
			return
		}
		waitLOCMtx.Lock()
		if !locFinished {
			if ctx.Debug > 0 {
				Printf("waiting for git ops result\n")
			}
			e = <-goch
			if e != nil {
				waitLOCMtx.Unlock()
				return
			}
			locFinished = true
		}
		waitLOCMtx.Unlock()
		if ctx.Debug > 1 {
			Printf("loc: %d, programming languages summary: %+v\n", j.Loc, j.Pls)
		}
		return
	}
	processCommit := func(c chan error, commit map[string]interface{}) (wch chan error, e error) {
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		esItem := j.AddMetadata(ctx, commit)
		if ctx.Project != "" {
			commit["project"] = ctx.Project
		}
		e = waitForLOC()
		if e != nil {
			return
		}
		commit["total_lines_of_code"] = j.Loc
		commit["program_language_summary"] = j.Pls
		esItem["data"] = commit
		if allCommitsMtx != nil {
			allCommitsMtx.Lock()
		}
		allCommits = append(allCommits, esItem)
		nCommits := len(allCommits)
		if nCommits >= ctx.ESBulkSize {
			sendToElastic := func(c chan error) (ee error) {
				defer func() {
					if c != nil {
						c <- ee
					}
				}()
				ee = SendToElastic(ctx, j, true, UUID, allCommits)
				if ee != nil {
					Printf("error %v sending %d commits to ElasticSearch\n", ee, len(allCommits))
				}
				allCommits = []interface{}{}
				if allCommitsMtx != nil {
					allCommitsMtx.Unlock()
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
			if allCommitsMtx != nil {
				allCommitsMtx.Unlock()
			}
		}
		return
	}
	var (
		commit map[string]interface{}
		ok     bool
	)
	if thrN > 1 {
		for {
			commit, ok, err = j.ParseNextCommit(ctx)
			if err != nil {
				return
			}
			if !ok {
				break
			}
			go func(com map[string]interface{}) {
				var (
					e    error
					esch chan error
				)
				esch, e = processCommit(ch, com)
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
			}(commit)
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
		for {
			commit, ok, err = j.ParseNextCommit(ctx)
			if err != nil {
				return
			}
			if !ok {
				break
			}
			_, err = processCommit(nil, commit)
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
	err = cmd.Wait()
	if err != nil {
		return
	}
	nCommits := len(allCommits)
	if ctx.Debug > 0 {
		Printf("%d remaining commits to send to ES\n", nCommits)
	}
	if nCommits > 0 {
		err = SendToElastic(ctx, j, true, UUID, allCommits)
		if err != nil {
			Printf("Error %v sending %d commits to ES\n", err, len(allCommits))
		}
	}
	return
}

// SupportDateFrom - does DS support resuming from date?
func (j *DSGit) SupportDateFrom() bool {
	return true
}

// SupportOffsetFrom - does DS support resuming from offset?
func (j *DSGit) SupportOffsetFrom() bool {
	return false
}

// DateField - return date field used to detect where to restart from
func (j *DSGit) DateField(*Ctx) string {
	return DefaultDateField
}

// RichIDField - return rich ID field name
func (j *DSGit) RichIDField(*Ctx) string {
	return DefaultIDField
}

// RichAuthorField - return rich ID field name
func (j *DSGit) RichAuthorField(*Ctx) string {
	return DefaultAuthorField
}

// OffsetField - return offset field used to detect where to restart from
func (j *DSGit) OffsetField(*Ctx) string {
	return DefaultOffsetField
}

// OriginField - return origin field used to detect where to restart from
func (j *DSGit) OriginField(ctx *Ctx) string {
	if ctx.Tag != "" {
		return DefaultTagField
	}
	return DefaultOriginField
}

// Categories - return a set of configured categories
func (j *DSGit) Categories() map[string]struct{} {
	return GitCategories
}

// ResumeNeedsOrigin - is origin field needed when resuming
// Origin should be needed when multiple configurations save to the same index
func (j *DSGit) ResumeNeedsOrigin(ctx *Ctx) bool {
	return !j.SingleOrigin
}

// Origin - return current origin
func (j *DSGit) Origin(ctx *Ctx) string {
	return j.URL
}

// ItemID - return unique identifier for an item
func (j *DSGit) ItemID(item interface{}) string {
	id, ok := item.(map[string]interface{})[Commit].(string)
	if !ok {
		Fatalf("%s: ItemID() - cannot extract %s from %+v", j.DS, Commit, DumpKeys(item))
	}
	return id
}

// AddMetadata - add metadata to the item
func (j *DSGit) AddMetadata(ctx *Ctx, item interface{}) (mItem map[string]interface{}) {
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
	mItem["backend_name"] = j.DS
	mItem["backend_version"] = GitBackendVersion
	mItem["timestamp"] = fmt.Sprintf("%.06f", float64(timestamp.UnixNano())/1.0e3)
	mItem[UUID] = uuid
	mItem[DefaultOriginField] = origin
	mItem[DefaultTagField] = tag
	mItem["updated_on"] = updatedOn
	mItem["category"] = j.ItemCategory(item)
	mItem["search_fields"] = make(map[string]interface{})
	FatalOnError(DeepSet(mItem, []string{"search_fields", GitDefaultSearchField}, itemID, false))
	mItem[DefaultDateField] = ToESDate(updatedOn)
	mItem[DefaultTimestampField] = ToESDate(timestamp)
	mItem[ProjectSlug] = ctx.ProjectSlug
	return
}

// ItemUpdatedOn - return updated on date for an item
func (j *DSGit) ItemUpdatedOn(item interface{}) time.Time {
	iUpdated, _ := Dig(item, []string{GitCommitDateField}, true, false)
	sUpdated, ok := iUpdated.(string)
	if !ok {
		Fatalf("%s: ItemUpdatedOn() - cannot extract %s from %+v", j.DS, GitCommitDateField, DumpKeys(item))
	}
	updated, ok := ParseMBoxDate(sUpdated)
	if !ok {
		Fatalf("%s: ItemUpdatedOn() - cannot extract %s from %s", j.DS, GitCommitDateField, sUpdated)
	}
	return updated
}

// ItemCategory - return unique identifier for an item
func (j *DSGit) ItemCategory(item interface{}) string {
	return Commit
}

// ElasticRawMapping - Raw index mapping definition
func (j *DSGit) ElasticRawMapping() []byte {
	return GitRawMapping
}

// ElasticRichMapping - Rich index mapping definition
func (j *DSGit) ElasticRichMapping() []byte {
	return GitRichMapping
}

// GetAuthors - parse multiple authors used in pair programming mode
func (j *DSGit) GetAuthors(m map[string]string, n map[string][]string) (authors map[string]struct{}) {
	// FIXME:
	defer func() {
		Printf("GetAuthors(%+v,%+v) -> %+v\n", m, n, authors)
	}()
	if len(m) > 0 {
		for _, auth := range strings.Split(m["first_authors"], ",") {
			authors[strings.TrimSpace(auth)] = struct{}{}
		}
		authors[strings.TrimSpace(m["last_author"])] = struct{}{}
	}
	if len(n) > 0 {
		for _, auth := range n["first_authors"] {
			authors[strings.TrimSpace(auth)] = struct{}{}
		}
	}
	return
}

// IdentitiesFromGitAuthors - construct identities from git authors
func (j *DSGit) IdentitiesFromGitAuthors(ctx *Ctx, authors map[string]struct{}) (identities map[[3]string]struct{}) {
	init := false
	for author := range authors {
		fields := strings.Split(author, "<")
		name := strings.TrimSpace(fields[0])
		email := Nil
		if len(fields) > 1 {
			email = fields[1]
			email = email[:len(email)-1]
		}
		identity := [3]string{name, Nil, email}
		if !init {
			identities = make(map[[3]string]struct{})
			init = true
		}
		identities[identity] = struct{}{}
	}
	return
}

// GetItemIdentities return list of item's identities, each one is [3]string
// (name, username, email) tripples, special value Nil "<nil>" means null
// we use string and not *string which allows nil to allow usage as a map key
func (j *DSGit) GetItemIdentities(ctx *Ctx, doc interface{}) (identities map[[3]string]struct{}, err error) {
	var authorsMap map[string]struct{}
	iauthors, ok := Dig(doc, []string{"data", "Author"}, false, true)
	if ok {
		authors, _ := iauthors.(string)
		if j.PairProgramming {
			m1 := MatchGroups(GitAuthorsPattern, authors)
			m2 := MatchGroupsArray(GitCoAuthorsPattern, authors)
			if len(m1) > 0 || len(m2) > 0 {
				authorsMap = j.GetAuthors(m1, m2)
			}
		}
		if len(authorsMap) == 0 {
			authorsMap = map[string]struct{}{authors: {}}
		}
	}
	var committersMap map[string]struct{}
	icommitters, ok := Dig(doc, []string{"data", "Commit"}, false, true)
	if ok {
		committers, _ := icommitters.(string)
		if j.PairProgramming {
			m1 := MatchGroups(GitAuthorsPattern, committers)
			m2 := MatchGroupsArray(GitCoAuthorsPattern, committers)
			if len(m1) > 0 || len(m2) > 0 {
				committersMap = j.GetAuthors(m1, m2)
			}
		}
		if len(committersMap) == 0 {
			committersMap = map[string]struct{}{committers: {}}
		}
	}
	othersMap := map[string]struct{}{}
	for _, otherKey := range []string{"Signed-off-by", "Co-authored-by"} {
		iothers, ok := Dig(doc, []string{"data", otherKey}, false, true)
		if ok {
			others, _ := iothers.([]interface{})
			for _, other := range others {
				othersMap[strings.TrimSpace(other.(string))] = struct{}{}
			}
		}
	}
	for auth := range committersMap {
		authorsMap[auth] = struct{}{}
	}
	for auth := range othersMap {
		authorsMap[auth] = struct{}{}
	}
	if len(authorsMap) > 0 {
		identities = j.IdentitiesFromGitAuthors(ctx, authorsMap)
	}
	return
}

// GitEnrichItemsFunc - iterate items and enrich them
// items is a current pack of input items
// docs is a pointer to where extracted identities will be stored
func GitEnrichItemsFunc(ctx *Ctx, ds DS, thrN int, items []interface{}, docs *[]interface{}) (err error) {
	// IMPL:
	if ctx.Debug > 0 {
		Printf("stub enrich items %d/%d func\n", len(items), len(*docs))
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
			e = fmt.Errorf("Failed to parse document %+v\n", doc)
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
func (j *DSGit) EnrichItems(ctx *Ctx) (err error) {
	Printf("enriching items\n")
	err = ForEachESItem(ctx, j, true, ESBulkUploadFunc, GitEnrichItemsFunc, nil)
	return
}

// EnrichItem - return rich item from raw item for a given author type
func (j *DSGit) EnrichItem(ctx *Ctx, item map[string]interface{}, author string, affs bool, extra interface{}) (rich map[string]interface{}, err error) {
	// IMPL:
	rich = item
	return
}

// AffsItems - return affiliations data items for given roles and date
func (j *DSGit) AffsItems(ctx *Ctx, rawItem map[string]interface{}, roles []string, date interface{}) (affsItems map[string]interface{}, err error) {
	// IMPL:
	return
}

// GetRoleIdentity - return identity data for a given role
func (j *DSGit) GetRoleIdentity(ctx *Ctx, item map[string]interface{}, role string) map[string]interface{} {
	// IMPL:
	return map[string]interface{}{"name": nil, "username": nil, "email": nil}
}

// AllRoles - return all roles defined for the backend
// roles can be static (always the same) or dynamic (per item)
// second return parameter is static mode (true/false)
// dynamic roles will use item to get its roles
func (j *DSGit) AllRoles(ctx *Ctx, item map[string]interface{}) ([]string, bool) {
	// IMPL:
	return []string{Author}, true
}

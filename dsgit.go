package dads

import (
	"bufio"
	"fmt"
	"io"
	"math"
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
	// GitUUID - field used as a rich item ID when pair progrmamming is enabled
	GitUUID = "git_uuid"
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
	RecentLines []string                          // recent commit lines
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
func (j *DSGit) ParseFile(ctx *Ctx, line string) (parsed, empty bool, err error) {
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
	m = MatchGroups(GitCommitPattern, line)
	if len(m) > 0 {
		empty = true
	} else if ctx.Debug > 1 {
		Printf("invalid file section format, line %d: '%s'\n", j.CurrLine, line)
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
		// Trailer can be the same as header value, we still want to have it - with "-Trailer" prefix added
		_, ok = ary.(string)
		if ok {
			trailer += "-Trailer"
			ary2, ok2 := j.Commit[trailer]
			if ok2 {
				if ctx.Debug > 1 {
					Printf("renamed trailer %s found in '%s'\n", trailer, line)
				}
				j.Commit[trailer] = append(ary2.([]interface{}), m["value"])
			} else {
				if ctx.Debug > 1 {
					Printf("added renamed trailer %s\n", trailer)
				}
				j.Commit[trailer] = []interface{}{m["value"]}
			}
		} else {
			j.Commit[trailer] = append(ary.([]interface{}), m["value"])
		}
	} else {
		j.Commit[trailer] = []interface{}{m["value"]}
	}
}

// HandleRecentLines - keep last 30 lines, so we can show them on parser error
func (j *DSGit) HandleRecentLines(line string) {
	j.RecentLines = append(j.RecentLines, line)
	l := len(j.RecentLines)
	if l > 30 {
		j.RecentLines = j.RecentLines[1:]
	}
}

// ParseNextCommit - parse next git log commit or report end
func (j *DSGit) ParseNextCommit(ctx *Ctx) (commit map[string]interface{}, ok bool, err error) {
	for j.LineScanner.Scan() {
		j.CurrLine++
		line := strings.TrimRight(j.LineScanner.Text(), "\n")
		if ctx.Debug > 2 {
			j.HandleRecentLines(line)
		}
		if ctx.Debug > 2 {
			Printf("line %d: '%s'\n", j.CurrLine, line)
		}
		var (
			parsed bool
			empty  bool
			state  string
		)
		for {
			if ctx.Debug > 2 {
				state = fmt.Sprintf("%v", j.ParseState)
			}
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
				parsed, empty, err = j.ParseFile(ctx, line)
			default:
				err = fmt.Errorf("unknown parse state:%d", j.ParseState)
			}
			if ctx.Debug > 2 {
				state += fmt.Sprintf(" -> (%v,%v,%v)", j.ParseState, parsed, err)
				Printf("%s\n", state)
			}
			if err != nil {
				Printf("parse next line '%s' error: %v\n", line, err)
				return
			}
			if j.ParseState == GitParseStateCommit && j.Commit != nil {
				commit = j.BuildCommit(ctx)
				if empty {
					parsed, err = j.ParseCommit(ctx, line)
					if !parsed || err != nil {
						Printf("failed to parse commit after empty file section\n")
						return
					}
				}
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
			if ctx.Debug > 0 {
				Printf("loc: %d, programming languages: %d\n", j.Loc, len(j.Pls))
			}
		}
		waitLOCMtx.Unlock()
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
	if !locFinished {
		go func() {
			if ctx.Debug > 0 {
				Printf("gitops result not needed, but waiting for orphan process\n")
			}
			<-goch
			locFinished = true
			if ctx.Debug > 0 {
				Printf("loc: %d, programming languages: %d\n", j.Loc, len(j.Pls))
			}
		}()
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
	if j.PairProgramming {
		return GitUUID
	}
	return UUID
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
	commitSHA := j.ItemID(item)
	updatedOn := j.ItemUpdatedOn(item)
	uuid := UUIDNonEmpty(ctx, origin, commitSHA)
	timestamp := time.Now()
	mItem["backend_name"] = j.DS
	mItem["backend_version"] = GitBackendVersion
	mItem["timestamp"] = fmt.Sprintf("%.06f", float64(timestamp.UnixNano())/1.0e3)
	mItem[UUID] = uuid
	mItem[DefaultOriginField] = origin
	mItem[DefaultTagField] = tag
	mItem[DefaultOffsetField] = float64(updatedOn.Unix())
	mItem["category"] = j.ItemCategory(item)
	mItem["search_fields"] = make(map[string]interface{})
	FatalOnError(DeepSet(mItem, []string{"search_fields", GitDefaultSearchField}, commitSHA, false))
	mItem[DefaultDateField] = ToESDate(updatedOn)
	mItem[DefaultTimestampField] = ToESDate(timestamp)
	mItem[ProjectSlug] = ctx.ProjectSlug
	if ctx.Debug > 1 {
		Printf("%s: %s: %v %v\n", origin, uuid, commitSHA, updatedOn)
	}
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
func (j *DSGit) GetAuthors(ctx *Ctx, m map[string]string, n map[string][]string) (authors map[string]struct{}, author string) {
	if ctx.Debug > 1 {
		defer func() {
			Printf("GetAuthors(%+v,%+v) -> %+v,%s\n", m, n, authors, author)
		}()
	}
	if len(m) > 0 {
		authors = make(map[string]struct{})
		email := strings.TrimSpace(m["email"])
		if !strings.Contains(email, "<") || !strings.Contains(email, "@") || !strings.Contains(email, ">") {
			email = ""
		}
		for _, auth := range strings.Split(m["first_authors"], ",") {
			auth = strings.TrimSpace(auth)
			if email != "" && (!strings.Contains(auth, "<") || !strings.Contains(auth, "@") || !strings.Contains(auth, ">")) {
				auth += " " + email
			}
			authors[auth] = struct{}{}
			if author == "" {
				author = auth
			}
		}
		auth := strings.TrimSpace(m["last_author"])
		if email != "" && (!strings.Contains(auth, "<") || !strings.Contains(auth, "@") || !strings.Contains(auth, ">")) {
			auth += " " + email
		}
		authors[auth] = struct{}{}
		if author == "" {
			author = auth
		}
	}
	if len(n) > 0 {
		if authors == nil {
			authors = make(map[string]struct{})
		}
		nEmails := len(n["email"])
		for i, auth := range n["first_authors"] {
			email := ""
			if i < nEmails {
				email = strings.TrimSpace(n["email"][i])
				if !strings.Contains(email, "@") {
					email = ""
				}
			}
			auth = strings.TrimSpace(auth)
			if email != "" && !strings.Contains(auth, "@") {
				auth += " <" + email + ">"
			}
			authors[auth] = struct{}{}
			if author == "" {
				author = auth
			}
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

// GetAuthorsData - extract authors data from a given field (this supports pair programming)
func (j *DSGit) GetAuthorsData(ctx *Ctx, doc interface{}, auth string) (authorsMap map[string]struct{}, firstAuthor string) {
	iauthors, ok := Dig(doc, []string{"data", auth}, false, true)
	if ok {
		authors, _ := iauthors.(string)
		if j.PairProgramming {
			if ctx.Debug > 1 {
				Printf("pp %s: %s\n", auth, authors)
			}
			m1 := MatchGroups(GitAuthorsPattern, authors)
			m2 := MatchGroupsArray(GitCoAuthorsPattern, authors)
			if len(m1) > 0 || len(m2) > 0 {
				authorsMap, firstAuthor = j.GetAuthors(ctx, m1, m2)
			}
		}
		if len(authorsMap) == 0 {
			authorsMap = map[string]struct{}{authors: {}}
			firstAuthor = authors
		}
	}
	return
}

// GetOtherAuthors - get others authors - possible from fields: Signed-off-by and/or Co-authored-by
func (j *DSGit) GetOtherAuthors(ctx *Ctx, doc interface{}) (othersMap map[string]string) {
	for _, otherKey := range []string{"Signed-off-by", "Co-authored-by"} {
		iothers, ok := Dig(doc, []string{"data", otherKey}, false, true)
		if ok {
			others, _ := iothers.([]interface{})
			if ctx.Debug > 1 {
				Printf("pp %s: %s\n", otherKey, others)
			}
			othersMap = make(map[string]string)
			for _, other := range others {
				othersMap[strings.TrimSpace(other.(string))] = otherKey
			}
		}
	}
	return
}

// GetItemIdentities return list of item's identities, each one is [3]string
// (name, username, email) tripples, special value Nil "<nil>" means null
// we use string and not *string which allows nil to allow usage as a map key
func (j *DSGit) GetItemIdentities(ctx *Ctx, doc interface{}) (identities map[[3]string]struct{}, err error) {
	if ctx.Debug > 2 {
		defer func() {
			Printf("%+v\n", identities)
		}()
	}
	authorsMap, _ := j.GetAuthorsData(ctx, doc, "Author")
	committersMap, _ := j.GetAuthorsData(ctx, doc, "Commit")
	othersMap := j.GetOtherAuthors(ctx, doc)
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
	var (
		mtx *sync.RWMutex
		ch  chan error
	)
	if thrN > 1 {
		mtx = &sync.RWMutex{}
		ch = make(chan error)
	}
	dbConfigured := ctx.AffsDBConfigured()
	git, _ := ds.(*DSGit)
	var getRichItems func(map[string]interface{}) ([]interface{}, error)
	if git.PairProgramming {
		getRichItems = func(doc map[string]interface{}) (richItems []interface{}, e error) {
			idata, _ := Dig(doc, []string{"data"}, true, false)
			data, _ := idata.(map[string]interface{})
			data["Author-Original"] = data["Author"]
			authorsMap, firstAuthor := git.GetAuthorsData(ctx, doc, "Author")
			if len(authorsMap) > 1 {
				authors := []string{}
				for auth := range authorsMap {
					authors = append(authors, auth)
				}
				data["authors"] = authors
				data["Author"] = firstAuthor
			}
			committersMap, firstCommitter := git.GetAuthorsData(ctx, doc, "Commit")
			if len(committersMap) > 1 {
				committers := []string{}
				for committer := range committersMap {
					committers = append(committers, committer)
				}
				data["committers"] = committers
				data["Commit-Original"] = data["Commit"]
				data["Commit"] = firstCommitter
			}
			hasSigners := false
			hasCoAuthors := false
			var (
				signers   []string
				coAuthors []string
			)
			othersMap := git.GetOtherAuthors(ctx, doc)
			if len(othersMap) > 0 {
				signers = []string{firstAuthor}
				coAuthors = []string{firstAuthor}
				for auth, authType := range othersMap {
					if auth == firstAuthor {
						continue
					}
					if authType == "Signed-off-by" {
						hasSigners = true
						signers = append(signers, auth)
					} else {
						hasCoAuthors = true
						coAuthors = append(coAuthors, auth)
					}
				}
				if hasSigners {
					data["authors_signed_off"] = signers
				}
				if hasCoAuthors {
					data["co_authors"] = coAuthors
				}
			}
			uuid, _ := doc[UUID].(string)
			added := make(map[string]struct{})
			added[firstAuthor] = struct{}{}
			aIdx := 0
			flags := make(map[string]struct{})
			auth2UUID := make(map[string]string)
			if len(authorsMap) > 1 {
				for auth := range authorsMap {
					_, alreadyAdded := added[auth]
					if alreadyAdded {
						continue
					}
					added[auth] = struct{}{}
					flags["is_git_commit_multi_author"] = struct{}{}
					commitID := uuid + "_" + strconv.Itoa(aIdx)
					aIdx++
					auth2UUID[auth] = commitID
				}
			}
			if len(committersMap) > 1 {
				for auth := range committersMap {
					_, alreadyAdded := added[auth]
					if alreadyAdded {
						continue
					}
					added[auth] = struct{}{}
					flags["is_git_commit_multi_committer"] = struct{}{}
					commitID := uuid + "_" + strconv.Itoa(aIdx)
					aIdx++
					auth2UUID[auth] = commitID
				}
			}
			if hasSigners {
				for _, auth := range signers {
					_, alreadyAdded := added[auth]
					if alreadyAdded {
						continue
					}
					added[auth] = struct{}{}
					flags["is_git_commit_signed_off"] = struct{}{}
					commitID := uuid + "_" + strconv.Itoa(aIdx)
					aIdx++
					auth2UUID[auth] = commitID
				}
			}
			if hasCoAuthors {
				for _, auth := range coAuthors {
					_, alreadyAdded := added[auth]
					if alreadyAdded {
						continue
					}
					added[auth] = struct{}{}
					flags["is_git_commit_co_author"] = struct{}{}
					commitID := uuid + "_" + strconv.Itoa(aIdx)
					aIdx++
					auth2UUID[auth] = commitID
				}
			}
			for flag := range flags {
				data[flag] = 1
			}
			// Normal enrichment
			var rich map[string]interface{}
			rich, e = ds.EnrichItem(ctx, doc, "", dbConfigured, nil)
			if e != nil {
				return
			}
			richItems = append(richItems, rich)
			// additional authors, committers, signers and co-authors
			for auth, gitUUID := range auth2UUID {
				data["Author"] = auth
				rich, e = ds.EnrichItem(ctx, doc, "", dbConfigured, nil)
				if e != nil {
					return
				}
				rich[GitUUID] = gitUUID
				richItems = append(richItems, rich)
			}
			return
		}
	} else {
		getRichItems = func(doc map[string]interface{}) (richItems []interface{}, e error) {
			var rich map[string]interface{}
			rich, e = ds.EnrichItem(ctx, doc, "", dbConfigured, nil)
			if e != nil {
				return
			}
			richItems = append(richItems, rich)
			return
		}
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
			e = fmt.Errorf("Failed to parse document %+v\n", doc)
			return
		}
		richItems, e := getRichItems(doc)
		if e != nil {
			return
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
func (j *DSGit) EnrichItems(ctx *Ctx) (err error) {
	Printf("enriching items\n")
	err = ForEachESItem(ctx, j, true, ESBulkUploadFunc, GitEnrichItemsFunc, nil)
	return
}

// EnrichItem - return rich item from raw item for a given author type
func (j *DSGit) EnrichItem(ctx *Ctx, item map[string]interface{}, skip string, affs bool, extra interface{}) (rich map[string]interface{}, err error) {
	/*
		f1, _ := Dig(item, []string{"data", "is_git_commit_multi_author"}, false, true)
		f2, _ := Dig(item, []string{"data", "is_git_commit_multi_committer"}, false, true)
		f3, _ := Dig(item, []string{"data", "is_git_commit_signed_off"}, false, true)
		f4, _ := Dig(item, []string{"data", "is_git_commit_co_author"}, false, true)
		rich["author"] = auth
		rich["is_git_commit_multi_author"] = f1
		rich["is_git_commit_multi_committer"] = f2
		rich["is_git_commit_signed_off"] = f3
		rich["is_git_commit_co_author"] = f4
	*/
	rich = make(map[string]interface{})
	for _, field := range RawFields {
		v, _ := item[field]
		rich[field] = v
	}
	commit, ok := item["data"].(map[string]interface{})
	if !ok {
		err = fmt.Errorf("missing data field in item %+v", DumpKeys(item))
		return
	}
	rich[GitUUID] = rich[UUID]
	iAuthorDate, _ := Dig(commit, []string{"AuthorDate"}, true, false)
	sAuthorDate, _ := iAuthorDate.(string)
	authorDate, ok := ParseMBoxDate(sAuthorDate)
	if !ok {
		err = fmt.Errorf("cannot parse author date from %v", iAuthorDate)
		return
	}
	rich["author_date"] = authorDate
	rich["author_date_weekday"] = int(authorDate.Weekday())
	rich["author_date_hour"] = authorDate.Hour()
	rich["utc_author"] = authorDate
	rich["utc_author_date_weekday"] = rich["author_date_weekday"]
	rich["utc_author_date_hour"] = rich["author_date_hour"]
	iCommitDate, _ := Dig(commit, []string{"CommitDate"}, true, false)
	sCommitDate, _ := iCommitDate.(string)
	commitDate, ok := ParseMBoxDate(sCommitDate)
	if !ok {
		err = fmt.Errorf("cannot parse commit date from %v", iAuthorDate)
		return
	}
	rich["commit_date"] = commitDate
	rich["commit_date_weekday"] = int(commitDate.Weekday())
	rich["commit_date_hour"] = commitDate.Hour()
	rich["utc_commit"] = commitDate
	rich["utc_commit_date_weekday"] = rich["commit_date_weekday"]
	rich["utc_commit_date_hour"] = rich["commit_date_hour"]
	message, ok := Dig(commit, []string{"message"}, false, true)
	if ok {
		msg, _ := message.(string)
		rich["message_analyzed"] = msg
		if len(msg) > KeywordMaxlength {
			msg = msg[:KeywordMaxlength]
		}
		rich["message"] = msg
	} else {
		rich["message_analyzed"] = nil
		rich["message"] = nil
	}
	comm, ok := Dig(commit, []string{"commit"}, false, true)
	if ok {
		hsh, _ := comm.(string)
		rich["hash"] = hsh
		rich["hash_short"] = hsh[:6]
	} else {
		rich["hash"] = nil
	}
	iRefs, ok := Dig(commit, []string{"refs"}, false, true)
	if ok {
		refsAry, ok := iRefs.([]interface{})
		if ok {
			tags := []string{}
			for _, iRef := range refsAry {
				ref, _ := iRef.(string)
				if strings.Contains(ref, "tag: ") {
					tags = append(tags, ref)
				}
			}
			rich["commit_tags"] = tags
		}
	}
	rich["tz"] = 0
	rich["branches"] = []interface{}{}
	dtDiff := float64(commitDate.Sub(authorDate).Seconds()) / 3600.0
	dtDiff = math.Round(dtDiff*100.0) / 100.0
	rich["time_to_commit_hours"] = dtDiff
	iRepoName, _ := Dig(item, []string{"origin"}, true, false)
	repoName, ok := iRepoName.(string)
	if strings.HasPrefix(repoName, "http") {
		repoName = AnonymizeURL(repoName)
	}
	rich["repo_name"] = repoName
	// author, _ := Dig(commit, []string{"Author"}, true, false)
	// FIXME
	Printf("%+v\n", rich)
	os.Exit(1)
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

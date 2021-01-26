package googlegroups

import (
	"bytes"
	"encoding/base64"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
	"github.com/LF-Engineering/dev-analytics-libraries/http"
	"github.com/LF-Engineering/dev-analytics-libraries/uuid"
	"github.com/araddon/dateparse"
	"github.com/jhillyerd/enmime"
)

// Fetcher contains GoogleGroups datasource fetch logic
type Fetcher struct {
	DSName                string
	HTTPClientProvider    *http.ClientProvider
	ElasticSearchProvider *elastic.ClientProvider
	BackendVersion        string
	DateFrom              time.Time
	ArchivesBasePath      string
	JSONFilesBasePath     string
	GroupName             string
	ProjectSlug           string
	Project               string
}

// NewFetcher initiates a new GoogleGroups fetcher
func NewFetcher(groupName, projectSlug, project string, httpClientProvider *http.ClientProvider, esClientProvider *elastic.ClientProvider) *Fetcher {
	return &Fetcher{
		DSName:                GoogleGroups,
		HTTPClientProvider:    httpClientProvider,
		ElasticSearchProvider: esClientProvider,
		BackendVersion:        "0.0.1",
		DateFrom:              time.Time{},
		ArchivesBasePath:      archivesBasePath,
		JSONFilesBasePath:     jsonFilesBasePath,
		GroupName:             groupName,
		ProjectSlug:           projectSlug,
		Project:               project,
	}
}

// Fetch ...
func (f *Fetcher) Fetch(fromDate, now *time.Time) ([]*RawMessage, error) {
	log.Println("IN Fetch")
	cmd := exec.Command(script, f.GroupName)
	_, err := cmd.Output()
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	rawMessages := make([]*RawMessage, 0)
	var topics []GoogleGroupMessageThread
	fmt.Println(strings.TrimSpace(f.JSONFilesBasePath + f.GroupName + jsonExtension))
	bites, err := ioutil.ReadFile(strings.TrimSpace(f.JSONFilesBasePath + f.GroupName + jsonExtension))
	if err != nil {
		fmt.Println(err)
	}

	err = jsoniter.Unmarshal(bites, &topics)
	if err != nil {
		return nil, err
	}

	for _, message := range topics {
		for _, nMessage := range message.Messages {
			fmt.Println(f.ArchivesBasePath + "/" + f.GroupName + "/" + nMessage.File)
			path := f.ArchivesBasePath + "/" + f.GroupName + "/" + nMessage.File
			dir := f.ArchivesBasePath + "/" + f.GroupName + "/" + message.ID
			if _, err := os.Stat(path); os.IsNotExist(err) {
				fmt.Println("file exists: ", path)
				continue
			}
			messageDate, err := f.formatJSONDataDate(nMessage.Date)
			if err != nil {
				log.Println("date issue", err)
				return nil, err
			}

			if messageDate.Before(*fromDate) {
				continue
			}

			_, err = ioutil.ReadDir(dir)
			if err != nil {
				log.Fatalf("could not read directory %s: %v", dir, err)
			}

			type result struct {
				path string
				env  *enmime.Envelope
				err  error
			}
			results := make(chan result, 1024)

			var wgFiles, wgProcess sync.WaitGroup
			wgFiles.Add(1)
			go func() {
				defer wgFiles.Done()
				wgFiles.Add(1)
				go func(path string) {
					defer wgFiles.Done()
					env, err := f.readEnvelope(path)
					results <- result{path, env, err}
				}(path)
			}()

			archive := map[string][]*enmime.Envelope{}
			var nEnvs, nErr int
			wgProcess.Add(1)
			go func() {
				defer wgProcess.Done()
				for res := range results {
					if res.err != nil {
						log.Printf("processing %s: %v", res.path, res.err)
						nErr++
						continue
					}

					nEnvs++
					topic := filepath.Base(filepath.Dir(res.path))
					archive[topic] = append(archive[topic], res.env)
				}
			}()

			wgFiles.Wait()
			close(results)
			wgProcess.Wait()

			msg, err := f.getMessage(archive, now)
			if err != nil {
				return nil, err
			}
			fmt.Println(msg.UUID)
			rawMessages = append(rawMessages, msg)
		}
	}
	return rawMessages, err
}

func (f *Fetcher) readEnvelope(path string) (*enmime.Envelope, error) {
	text, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("could not open %s: %v", path, err)
	}
	env, err := enmime.ReadEnvelope(bytes.NewReader(text))
	for i := 0; i < 4 && err != nil; i++ {
		errStr := err.Error()
		if pos := strings.Index(errStr, malformedMIMEHeaderLineErrorMessage); pos >= 0 {
			data := strings.Replace(errStr[pos+len(malformedMIMEHeaderLineErrorMessage):], " ", "\r\n", -1)
			if base64RE.MatchString(data) {
				var decodedData []byte
				decodedData, err = base64.StdEncoding.DecodeString(data)
				if err == nil {
					text = bytes.Replace(text, []byte(data), decodedData, 1)
					env, err = enmime.ReadEnvelope(bytes.NewReader(text))
					if err == nil {
						return env, nil
					}
				} else {
					break
				}
			}
		} else {
			break
		}
	}
	if err != nil {
		return nil, fmt.Errorf("could not read envelope: %v", err)
	}
	return env, nil
}

func (f *Fetcher) getMessage(archive map[string][]*enmime.Envelope, now *time.Time) (rawMessage *RawMessage, err error) {
	rawMessage = new(RawMessage)
	seq := make([]string, 2)
	keys := make([]string, len(archive))
	{
		i := 0
		for key := range archive {
			keys[i] = key
			i++
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		envs := archive[key]
		seq[0] = key
		sort.Slice(envs, func(i, j int) bool {
			time1, err := dateparse.ParseAny(envs[i].GetHeader("Date"))
			if err != nil {
				log.Fatal(err)
			}
			time2, err := dateparse.ParseAny(envs[j].GetHeader("Date"))
			if err != nil {
				log.Fatal(err)
			}
			return time1.Before(time2)
		})

		thread := new(bytes.Buffer)
		for i, env := range envs {
			err = f.cleanupMessage(env.Text, thread)
			if err != nil {
				return nil, err
			}
			if i < len(envs)-1 {
				thread.Write([]byte("\n"))
			}
		}
		seq[1] = thread.String()
		// handle headers
		date, err := dateparse.ParseAny(envs[0].GetHeader("Date"))
		if err != nil {
			return nil, err
		}
		from := envs[0].GetHeader("From")
		to := envs[0].GetHeader("To")
		messageID := envs[0].GetHeader("Message-Id")
		inReplyTo := envs[0].GetHeader("In-Reply-To")
		references := envs[0].GetHeader("References")
		subject := envs[0].GetHeader("Subject")
		messageBody := thread.String()
		topicID := seq[0]

		uuID, err := uuid.Generate(GoogleGroups, messageID)
		if err != nil {
			return nil, err
		}

		rawMessage.From = from
		rawMessage.Date = date
		rawMessage.To = to
		rawMessage.MessageID = messageID
		rawMessage.InReplyTo = inReplyTo
		rawMessage.References = references
		rawMessage.Subject = subject
		rawMessage.MessageBody = messageBody
		rawMessage.TopicID = topicID
		rawMessage.MetadataUpdatedOn = *now
		rawMessage.MetadataTimestamp = *now
		rawMessage.ChangedAt = *now
		rawMessage.GroupName = f.GroupName
		rawMessage.ProjectSlug = f.ProjectSlug
		rawMessage.Project = f.Project
		rawMessage.UUID = uuID
		rawMessage.BackendName = fmt.Sprintf("%sFetch", strings.Title(GoogleGroups))
	}
	return
}

func (f *Fetcher) cleanupMessage(text string, output io.Writer) error {
	lines := strings.Split(text, "\n")
	first := true
	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)
		if trimmedLine == "" {
			continue
		}
		line = line[:strings.Index(line, trimmedLine)+len(trimmedLine)]
		if strings.HasPrefix(line, ">") {
			continue
		}
		if strings.HasPrefix(line, "On ") && strings.HasSuffix(line, " wrote:") {
			continue
		}
		if !first {
			if _, err := output.Write([]byte("\n")); err != nil {
				return err
			}
		} else {
			first = false
		}

		if _, err := output.Write([]byte(line)); err != nil {
			return err
		}
	}
	return nil
}

func (f *Fetcher) formatJSONDataDate(s string) (*time.Time, error) {
	layout := "02/01/06 03:04"
	t, err := time.Parse(layout, s)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

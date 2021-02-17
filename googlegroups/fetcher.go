package googlegroups

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	httpNative "net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
	"github.com/LF-Engineering/dev-analytics-libraries/http"
	"github.com/LF-Engineering/dev-analytics-libraries/uuid"
	"github.com/araddon/dateparse"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/gmail/v1"
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
	dir, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	credentialsFilePath := fmt.Sprintf("%+v/%+v/%+v", dir, "googlegroups", "credentials.json")
	b, err := ioutil.ReadFile(credentialsFilePath)
	if err != nil {
		log.Fatalf("Unable to read client secret file: %v", err)
	}

	// If modifying these scopes, delete your previously saved token.json.
	config, err := google.ConfigFromJSON(b, gmail.GmailModifyScope)
	if err != nil {
		log.Fatalf("Unable to parse client secret file to config: %v", err)
	}
	client := getClient(config)

	srv, err := gmail.New(client)
	if err != nil {
		log.Fatalf("Unable to retrieve Gmail client: %v", err)
	}

	user := "me"
	messageIDS := make([]string, 0)
	messages, err := srv.Users.Messages.List(user).Do()
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	for _, message := range messages.Messages {
		messageIDS = append(messageIDS, message.Id)
	}
	rawMessages := make([]*RawMessage, 0)

	for _, messageID := range messageIDS {
		type result struct {
			path string
			message  *gmail.Message
			err  error
		}
		results := make(chan result, 1024)

		var wgFiles, wgProcess sync.WaitGroup
		wgFiles.Add(1)
		go func() {
			defer wgFiles.Done()
			wgFiles.Add(1)
			go func(messageId string) {
				defer wgFiles.Done()
				msg, err := srv.Users.Messages.Get(user, messageId).Do()
				results <- result{messageId, msg, err}
			}(messageID)
		}()

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
				msg, err := f.getMessage(res.message, fromDate, now)
				if err != nil {
					return
				}
				rawMessages = append(rawMessages, msg)
				nEnvs++
			}
		}()
		wgFiles.Wait()
		close(results)
		wgProcess.Wait()
	}
	fmt.Println(len(rawMessages))
	for _, m := range rawMessages {
		fmt.Println(m)
	}
	os.Exit(1)
	return rawMessages, err
}

func (f *Fetcher) getMessage(msg *gmail.Message, fromDate, now *time.Time) (rawMessage *RawMessage, err error) {
	rawMessage = new(RawMessage)
	headers := GetHeadersData(msg)

	date, err := dateparse.ParseAny(headers.Date)
	if err != nil {
		log.Println(err)
	}
	//now := time.Now()
	from := headers.From
	to := headers.To
	messageID := headers.MessageID
	inReplyTo := headers.InReplyTo
	references := headers.References
	subject := headers.Subject
	messageBody := msg.Snippet

	if fromDate.After(date) {
		fmt.Println(fromDate, " > " ,date)
		return
	}

	timezone, err := f.getTimeZone(headers.Date)
	if err != nil {
		log.Println(err)
	}

	uuID, err := uuid.Generate("GoogleGroups", messageID)
	if err != nil {
		log.Println(err)
	}

	rawMessage.From = from
	rawMessage.Date = date
	rawMessage.To = to
	rawMessage.MessageID = messageID
	rawMessage.InReplyTo = inReplyTo
	rawMessage.References = references
	rawMessage.Subject = subject
	rawMessage.MessageBody = messageBody
	//rawMessage.TopicID = topicID
	//rawMessage.Topic = topic
	rawMessage.MetadataUpdatedOn = date
	rawMessage.MetadataTimestamp = *now
	rawMessage.ChangedAt = *now
	rawMessage.GroupName = f.GroupName
	rawMessage.ProjectSlug = f.ProjectSlug
	rawMessage.Project = f.Project
	rawMessage.UUID = uuID
	rawMessage.Timezone = timezone
	rawMessage.BackendName = fmt.Sprintf("%sFetch", strings.Title(GoogleGroups))
	rawMessage.BackendVersion = f.BackendVersion
	//rawMessage.UpdatedOn = timeLib.ConvertTimeToFloat(*now)
	//rawMessage.Timestamp = *now
	rawMessage.Origin = fmt.Sprintf("https://groups.google.com/g/%+v", f.GroupName)
	if strings.Contains(f.GroupName, "/") {
		splitGroupName := strings.Split(f.GroupName, "/")
		organization := strings.TrimSpace(splitGroupName[0])
		group := strings.TrimSpace(splitGroupName[1])
		rawMessage.Origin = fmt.Sprintf("https://groups.google.com/a/%+v/g/%+v", organization, group)
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
	layout := "mm/dd/yy 15:04"
	t, err := time.Parse(layout, s)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (f *Fetcher) getTimeZone(dateString string) (int, error) {
	trimBraces := strings.Split(dateString, " (")
	ss := strings.Split(trimBraces[0], " ")
	s := ss[len(ss)-1]
	strArr := []rune(s)
	sign := strArr[0]

	s = string(strArr[1:])
	if s != "0000" {
		s = strings.TrimRight(s, "0")
	}

	timezone, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}

	if string(sign) == "-" {
		timezone = -timezone
		return timezone, nil
	}

	return timezone, nil
}

// GetHeadersData gets some of the useful metadata from the headers.
func GetHeadersData(msg *gmail.Message) *HeadersData {
	data := &HeadersData{}
	for _, v := range msg.Payload.Headers {
		switch v.Name {
		case "Sender":
			data.Sender = v.Value
		case "From":
			data.From = v.Value
		case "Subject":
			data.Subject = v.Value
		case "Date":
			data.Date = v.Value
		case "Message-Id":
			data.MessageID = v.Value
		case "Message-ID":
			data.MessageID = v.Value
		case "In-Reply-To":
			data.InReplyTo = v.Value
		case "References":
			data.References = v.Value
		case "To":
			data.To = append(data.To, v.Value)
		case "Delivered-To":
			data.DeliveredTo = append(data.DeliveredTo, v.Value)
		}
	}
	return data
}

// Retrieve a token, saves the token, then returns the generated client.
func getClient(config *oauth2.Config) *httpNative.Client {
	// The file token.json stores the user's access and refresh tokens, and is
	// created automatically when the authorization flow completes for the first
	// time.
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	tokFile := fmt.Sprintf("%+v/%+v/%+v", dir, "googlegroups", "token.json")
	tok, err := tokenFromFile(tokFile)
	if err != nil {
		tok = getTokenFromWeb(config)
		saveToken(tokFile, tok)
	}
	return config.Client(context.Background(), tok)
}

// Request a token from the web, then returns the retrieved token.
func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following link in your browser then type the "+
		"authorization code: \n%v\n", authURL)

	var authCode string
	if _, err := fmt.Scan(&authCode); err != nil {
		log.Fatalf("Unable to read authorization code: %v", err)
	}

	tok, err := config.Exchange(context.TODO(), authCode)
	if err != nil {
		log.Fatalf("Unable to retrieve token from web: %v", err)
	}
	return tok
}

// Retrieves a token from a local file.
func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Fatal(err)
		}
	}()
	tok := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(tok)
	return tok, err
}

// Saves a token to a file path.
func saveToken(path string, token *oauth2.Token) {
	fmt.Printf("Saving credential file to: %s\n", path)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatalf("Unable to cache oauth token: %v", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Fatal(err)
		}
	}()
	if err := json.NewEncoder(f).Encode(token); err != nil {
		log.Fatal(err)
	}
}
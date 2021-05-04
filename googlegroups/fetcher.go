package googlegroups

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	httpNative "net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/LF-Engineering/dev-analytics-libraries/aws/ssm"
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
		GroupName:             groupName,
		ProjectSlug:           projectSlug,
		Project:               project,
	}
}

// Fetch ...
func (f *Fetcher) Fetch(fromDate, now *time.Time) ([]*RawMessage, error) {
	ssmClient, err := ssm.NewSSMClient()
	if err != nil {
		return nil, err
	}

	credentialsFileString, err := ssmClient.Param(CredentialsSSMParamName, true, false, "", "", "").GetValue()
	if err != nil {
		return nil, err
	}

	// If modifying these scopes, delete your previously saved token.json.
	config, err := google.ConfigFromJSON([]byte(credentialsFileString), gmail.GmailModifyScope)
	if err != nil {
		log.Fatalf("Unable to parse client secret file to config: %v", err)
	}

	client, err := getClient(config)
	if err != nil {
		return nil, err
	}

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
			path    string
			message *gmail.Message
			err     error
		}
		results := make(chan result, 0)

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
				if msg != nil {
					rawMessages = append(rawMessages, msg)
				}
				nEnvs++
			}
		}()
		wgFiles.Wait()
		close(results)
		wgProcess.Wait()
	}
	return rawMessages, err
}

func (f *Fetcher) getMessage(msg *gmail.Message, fromDate, now *time.Time) (rawMessage *RawMessage, err error) {
	rawMessage = new(RawMessage)
	headers := getHeadersData(msg)

	date, err := dateparse.ParseAny(headers.Date)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	//now := time.Now()
	from := headers.From
	to := headers.To
	sender := headers.Sender
	messageID := headers.MessageID
	inReplyTo := headers.InReplyTo
	references := headers.References
	subject := headers.Subject
	messageBody := msg.Snippet
	mailingList := headers.MailingList

	if sender != f.GroupName || mailingList != f.GroupName {
		errString := fmt.Sprintf("skipping subject [%+v] for group [%+v] & mailinglist [%+v]", subject, sender, mailingList)
		log.Println(errString)
		return nil, errors.New(errString)
	}

	if fromDate.After(date) {
		fmt.Println(fromDate, " > ", date)
		return nil, fmt.Errorf("fromDate: [%+v] greater than date: [%+v]", fromDate, date)
	}

	timezone, err := f.getTimeZone(headers.Date)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	uuID, err := uuid.Generate(GoogleGroups, messageID)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	groupName := getGroupName(f.GroupName)
	rawMessage.From = from
	rawMessage.Date = date
	rawMessage.To = to
	rawMessage.MessageID = messageID
	rawMessage.InReplyTo = inReplyTo
	rawMessage.References = references
	rawMessage.Subject = subject
	rawMessage.MessageBody = messageBody
	rawMessage.MetadataUpdatedOn = date
	rawMessage.MetadataTimestamp = *now
	rawMessage.ChangedAt = *now
	rawMessage.GroupName = groupName
	rawMessage.ProjectSlug = f.ProjectSlug
	rawMessage.Project = f.Project
	rawMessage.UUID = uuID
	rawMessage.Timezone = timezone
	rawMessage.BackendName = fmt.Sprintf("%sFetch", strings.Title(GoogleGroups))
	rawMessage.BackendVersion = f.BackendVersion
	if msg.ThreadId != "" {
		rawMessage.TopicID = msg.ThreadId
	}

	if inReplyTo == "" {
		rawMessage.Topic = subject
	}

	if rawMessage.TopicID != "" && rawMessage.Topic == "" {
		rawMessage.Topic = getSubjectFromReply(subject)
	}

	rawMessage.Origin = fmt.Sprintf("https://groups.google.com/g/%+v", groupName)
	if strings.Contains(groupName, "/") {
		splitGroupName := strings.Split(groupName, "/")
		organization := strings.TrimSpace(splitGroupName[0])
		group := strings.TrimSpace(splitGroupName[1])
		rawMessage.Origin = fmt.Sprintf("https://groups.google.com/a/%+v/g/%+v", organization, group)
	}
	return
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

func getGroupName(s string) string {
	splitEmail := strings.Split(s, "@")
	groupName := ""
	domain := ""
	if len(splitEmail) > 1 {
		groupName, domain = splitEmail[0], splitEmail[1]
		if domain == "googlegroups.com" {
			return groupName
		}
		return fmt.Sprintf("%s/%s", domain, groupName)
	}
	return ""
}

func getSubjectFromReply(s string) string {
	splitSubject := strings.Split(s, "Re: ")
	if len(splitSubject) > 1 {
		return splitSubject[1]
	}
	return splitSubject[0]
}

// getHeadersData gets some of the useful metadata from the headers.
func getHeadersData(msg *gmail.Message) *HeadersData {
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
		case "Mailing-list":
			mlList := strings.Split(v.Value, ";")
			mlValue := strings.Split(mlList[0], "list ")
			data.MailingList = strings.TrimSpace(mlValue[1])
		}
	}
	return data
}

// Retrieve a token, saves the token, then returns the generated client.
func getClient(config *oauth2.Config) (*httpNative.Client, error) {
	// The file token.json stores the user's access and refresh tokens, and is
	// created automatically when the authorization flow completes for the first
	// time.
	tok, err := getTokenFromSSM(TokenSSMParamName)
	if err != nil {
		tok = getTokenFromWeb(config)
		err = saveTokenToSSM(TokenSSMParamName, tok)
		if err != nil {
			return nil, err
		}
	}
	return config.Client(context.Background(), tok), nil
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

// Retrieves a token from ssm.
func getTokenFromSSM(ssmKey string) (*oauth2.Token, error) {
	ssmClient, err := ssm.NewSSMClient()
	if err != nil {
		return nil, err
	}

	tokenString, err := ssmClient.Param(ssmKey, true, false, "", "", "").GetValue()
	if err != nil {
		return nil, err
	}

	tok := &oauth2.Token{}
	err = json.Unmarshal([]byte(tokenString), &tok)
	return tok, err
}

// Saves a token to ssm store.
func saveTokenToSSM(ssmKey string, token *oauth2.Token) error {
	ssmClient, err := ssm.NewSSMClient()
	if err != nil {
		return err
	}

	tokenBytes, err := json.Marshal(token)
	if err != nil {
		return err
	}

	message, err := ssmClient.Param(ssmKey, true, true, "text", "SecureString", string(tokenBytes)).SetValue()
	if err != nil {
		return err
	}
	log.Println(message)
	return nil
}

// Update a token in ssm store.
func updateTokenInSSM(ssmKey string, token *oauth2.Token) error {
	ssmClient, err := ssm.NewSSMClient()
	if err != nil {
		return err
	}

	tokenBytes, err := json.Marshal(token)
	if err != nil {
		return err
	}

	message, err := ssmClient.Param(ssmKey, true, true, "text", "SecureString", string(tokenBytes)).UpdateValue()
	if err != nil {
		return err
	}
	log.Println(message)
	return nil
}

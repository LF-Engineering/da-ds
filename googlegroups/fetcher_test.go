package googlegroups

import (
	"testing"
	"time"

	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
	JSONiter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/gmail/v1"
)

// TestFetcherSimpleFetchItem tests the working of the fetchItem function
func TestFetchMessage(t *testing.T) {
	type fields struct {
		DSName                string
		ElasticSearchProvider *elastic.ClientProvider
		BackendVersion        string
	}
	type args struct {
		gmailMessage *gmail.Message
		project      string
		now          time.Time
	}

	rawItem1String := `{"from":"Jone Dow <jone.dow@gmail.com>","date":"2021-03-05T03:35:19-08:00","to":["GAM for Google Workspace <google-apps-manager@googlegroups.com>"],"message_id":"<000621f3-1eac-4c3a-b61f-2a785169e4d4n@googlegroups.com>","in_reply_to":"<b3cc2d39-f266-4b53-abdb-88ce674972a0n@googlegroups.com>","references":"<272d9c9b-f0ec-418d-becf-89d3d29f14c0n@googlegroups.com> <CA+VVBp-HXjixtAftJ4fAbWG8gM3nj8BSRmQ_KTVj-sR3d98WXw@mail.gmail.com> <b3cc2d39-f266-4b53-abdb-88ce674972a0n@googlegroups.com>","subject":"Re: [GAM] Cannot update some users info","message_body":"Hi Jay, One more point to add, I can update the user account info I created on GSuite admin portal manually. But I cannot edit user account which is existing on GSuite already even it is student or","topic_id":"","topic":"","backend_version":"0.0.1","uuid":"f9fb3a591ee4b9050f3e7e2a03b09c876e2e8413","origin":"https://groups.google.com/g/google-apps-manager","updated_on":0,"metadata__updated_on":"2021-03-05T03:35:19-08:00","backend_name":"GoogleGroupsFetch","metadata__timestamp":"2021-03-05T11:38:43.3631Z","timestamp":0,"project_slug":"project1","group_name":"google-apps-manager","project":"project1","changed_at":"2021-03-05T11:38:43.3631Z","timezone":-8}`
	googleGroupsRaw1, err := toGoogleGroupsRaw(rawItem1String)
	if err != nil {
		t.Error(err)
	}

	gmailItem1String := `{"id":"1698138e68ca","threadId":"1698138e68ca","labelIds":["UNREAD","IMPORTANT","CATEGORY_PERSONAL","INBOX"],"snippet":"Hi Jay, One more point to add, I can update the user account info I created on GSuite admin portal manually. But I cannot edit user account which is existing on GSuite already even it is student or","historyId":"270427","internalDate":"1554492714000","payload":{"partId":"","mimeType":"text/plain","filename":"","headers":[{"name":"Delivered-To","value":"google-apps-manager@googlegroups.com"},{"name":"Return-Path","value":"<jone.dow@gmail.com>"},{"name":"From","value":"Jone Dow <jone.dow@gmail.com>"},{"name":"To","value":"GAM for Google Workspace <google-apps-manager@googlegroups.com>"},{"name":"Subject","value":"Re: [GAM] Cannot update some users info"},{"name":"Thread-Topic","value":"Plain text sample email"},{"name":"Thread-Index","value":"AdTr5jkL493BeKJkSt2I+4R5TWw=="},{"name":"Date","value":"Fri, 5 Apr 2019 19:31:54 +0000"},{"name":"Message-ID","value":"<000621f3-1eac-4c3a-b61f-2a785169e4d4n@googlegroups.com>"},{"name":"Accept-Language","value":"en-US"},{"name":"Content-Language","value":"en-US"},{"name":"authentication-results","value":"spf=none (sender IP is ) smtp.mail.from=outlook.tester@salesforceemail.com;"},{"name":"Content-Type","value":"text/plain; charset=\"us-ascii\""},{"name":"Content-Transfer-Encoding","value":"quoted-printable"}],"body":{"size":146,"data":"DQoNCG4gU21pdGgNCkNFTyBvZiBCaWdDbw0KQ2VsbCAtIDYxOS0zNDQtMzMyMg0KT2ZmaWNlIC0gNjE5LTM0NS0yMzMzDQpTYW"}},"sizeEstimate":6978}`
	gmailMessage1, err := toGmailMessage(gmailItem1String)
	if err != nil {
		t.Error(err)
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *RawMessage
		wantErr bool
	}{
		{
			name: "Test Case #1",
			fields: fields{
				DSName:                "GoogleGroups",
				ElasticSearchProvider: nil,
				BackendVersion:        "0.0.1",
			},
			args: args{
				gmailMessage: gmailMessage1,
				project:      "project1",
				now:          time.Time{},
			},
			want:    &googleGroupsRaw1,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Fetcher{
				DSName:                tt.fields.DSName,
				HTTPClientProvider:    nil,
				ElasticSearchProvider: tt.fields.ElasticSearchProvider,
				BackendVersion:        tt.fields.BackendVersion,
			}
			now := time.Now()
			defaultDate := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
			got, err := f.getMessage(tt.args.gmailMessage, &defaultDate, &now)

			if (err != nil) != tt.wantErr {
				t.Errorf("FetchItem() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.NotEqual(t, got, nil)
			assert.Equal(t, tt.want.MessageID, got.MessageID)
			assert.Equal(t, tt.want.From, got.From)
			assert.Equal(t, tt.want.Subject, got.Subject)
		})
	}
}

func toGmailMessage(b string) (*gmail.Message, error) {
	gmailMessage := gmail.Message{}
	err := JSONiter.Unmarshal([]byte(b), &gmailMessage)
	return &gmailMessage, err
}
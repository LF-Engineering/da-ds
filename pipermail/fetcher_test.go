package pipermail

import (
	"fmt"
	"testing"
	"time"

	"github.com/LF-Engineering/da-ds/pipermail/mocks"
	"github.com/LF-Engineering/dev-analytics-libraries/http"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestFetchAll(t *testing.T) {
	httpClientProviderMock := &mocks.HTTPClientProvider{}
	from, err := time.Parse("2006-01-02 15:04:05", "2020-01-01 03:00:00")
	if err != nil {
		fmt.Println(err)
	}

	url := "https://www.openembedded.org/pipermail/openembedded-architecture/"

	httpClient := http.NewClientProvider(time.Second * 600)
	httpClientProviderMock.On("Request", url, "GET",
		mock.Anything, mock.Anything, mock.Anything).Return(
		200, rawMessageBytes, nil)
	tt := []struct {
		name     string
		fields   fields
		expected []byte
		err      bool
	}{
		{
			name: "ok message",
			fields: fields{
				DSName:                "pipermail",
				IncludeArchived:       false,
				MultiOrigin:           false,
				HTTPClientProvider:    nil,
				ElasticSearchProvider: nil,
				BackendVersion:        "",
			},
			expected: rawMessageBytes,
			err:      false,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			expecRaw, err := toMessageRaw(tc.expected)
			if err != nil {
				t.Error(err)
			}

			params := &Params{
				FromDate:       from,
				BackendVersion: "0.0.1",
				Project:        "yocto",
				Debug:          2,
				ProjectSlug:    "yocto",
				GroupName:      "openembedded-architecture",
			}
			srv := NewFetcher(params, httpClient, nil)
			var rawMessage interface{}
			err = jsoniter.Unmarshal(rawMessageBytes, &rawMessage)
			if err != nil {
				t.Error(err)
			}
			var message *RawMessage
			message = srv.AddMetadata(rawMessage, url, params.ProjectSlug, params.GroupName)
			if err != nil {
				t.Error(err)
			}

			assert.NoError(t, err)
			assert.Equal(t, expecRaw.BackendVersion, message.BackendVersion)
			assert.Equal(t, expecRaw.Origin, message.Origin)
			assert.Equal(t, expecRaw.Data.MessageID, message.Data.MessageID)
		})
	}

}

func toMessageRaw(b []byte) (output RawMessage, err error) {
	err = jsoniter.Unmarshal(b, &output)
	return
}

var rawMessageBytes = []byte(`
{
   "backend_version":"0.0.1",
   "data":{
      "Content-Type":"text/plain",
      "Date":"2016-02-26T19:15:43Z",
      "From":"jon.doe at gmail.com (Jon Doe)",
      "In-Reply-To":"<CAMKF1spgKSosWNxwUM1suPRZD2VRbNVDsFjr55sv6GMYWCVGtw@mail.gmail.com>",
      "MBox-Bytes-Length":742,
      "MBox-N-Bodies":1,
      "MBox-N-Lines":19,
      "MBox-Project-Name":"openembedded-architecture",
      "MBox-Valid":true,
      "MBox-Warn":false,
      "Message-ID":"<CAE4k23_0=nw4caHLXPDCygvFd47dJ2s9muxDFGPEibb2yQKh2Q@mail.gmail.com>",
      "References":"<2956615.y8hyrZheM7@peggleto-mobl.m.m.try.com><CAMKF1spgKSosWNxwUM1suPRZD2VRbNVDsFjr55sv6GMYWCVGtw@mail.gmail.com>",
      "Subject":"[Openembedded-architecture] Removing Hob for 2.1",
      "data":{
         "text":{
            "plain":[
               {
                  "data":"On Fri, 26 Feb 2016, 03:54 Jon Doe <jon.doe at gmail.com> wrote: Go ahead Yes, go ahead."
               }
            ]
         }
      },
      "date_in_tz":"2016-02-26T19:15:43Z",
      "date_tz":0
   },
   "tag":"https://www.openembedded.org/pipermail/openembedded-architecture/",
   "uuid":"acb13ade6f1540ceb6b72b085c94c32c7a6a540b",
   "search_fields":{
      "name":"openembedded-architecture",
      "item_id":"<CAE4k23_0=nw4caHLXPDCygvFd47dJ2s9muxDFGPEibb2yQKh2Q@mail.gmail.com>"
   },
   "origin":"https://www.openembedded.org/pipermail/openembedded-architecture/",
   "updated_on":1456514143,
   "metadata__updated_on":"2016-02-26T19:15:43.000000+00:00",
   "backend_name":"pipermail",
   "metadata__timestamp":"2020-12-24T20:15:09.322548+00:00",
   "timestamp":1.608840909322548E9,
   "category":"message",
   "project_slug":"yocto",
   "group_name":"openembedded-architecture",
   "project":"yocto",
   "changed_at":"0001-01-01T00:00:00Z",
   "Message-ID":"<CAE4k23_0=nw4caHLXPDCygvFd47dJ2s9muxDFGPEibb2yQKh2Q@mail.gmail.com>",
   "date":"2016-02-26T19:15:43Z"
}
`)

type fields struct {
	DSName                string
	IncludeArchived       bool
	MultiOrigin           bool
	HTTPClientProvider    HTTPClientProvider
	ElasticSearchProvider ESClientProvider
	BackendVersion        string
	Debug                 int
	DateFrom              time.Time
}

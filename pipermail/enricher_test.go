package pipermail

import (
	"database/sql"
	"testing"
	"time"

	"github.com/LF-Engineering/da-ds/affiliation"
	"github.com/LF-Engineering/da-ds/pipermail/mocks"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
)

func TestEnrichAll(t *testing.T) {
	tt := []struct {
		name        string
		fetchedData []byte
		expected    []byte
		err         bool
	}{
		{
			name:        "ok enriched message",
			fetchedData: rawMsgBytes,
			expected:    rawMessageBytes,
			err:         false,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := toRawMessage(tc.fetchedData)
			if err != nil {
				t.Error(err)
			}

			expectedEnrich, err := messageEnrich(tc.expected)
			if err != nil {
				t.Error(err)
			}

			identityProviderMock := &mocks.IdentityProvider{}
			unknown := "Unknown"
			zero := 0
			dd := "Intel Corporation"

			fakeAff1 := &affiliation.Identity{ID: sql.NullString{String: "756be8209f265138d271a6223fa0d85085e308db", Valid: true},
				UUID: sql.NullString{String: "756be8209f265138d271a6223fa0d85085e308db", Valid: true}, Name: sql.NullString{String: "Jon Doe", Valid: true}, IsBot: false,
				Domain: sql.NullString{String: "", Valid: false}, OrgName: sql.NullString{}, Username: sql.NullString{String: "", Valid: false}, GenderACC: &zero,
				MultiOrgNames: nil, Gender: sql.NullString{String: unknown, Valid: true},
			}

			fakeAff2 := &affiliation.Identity{ID: sql.NullString{String: "a89364af9818412b8c59193ca83b30dd67b20e35", Valid: true},
				UUID: sql.NullString{String: "5d408e590365763c3927084d746071fa84dc8e52", Valid: true}, Name: sql.NullString{String: "akuster", Valid: true}, IsBot: false,
				Domain: sql.NullString{String: "gmail.com", Valid: true}, OrgName: sql.NullString{String: dd, Valid: true}, Username: sql.NullString{String: "", Valid: false}, GenderACC: &zero,
				MultiOrgNames: []string{"Intel Corporation"}, Gender: sql.NullString{String: unknown, Valid: true},
			}

			identityProviderMock.On("GetIdentity", "email", "jon.doe@gmail.com").Return(fakeAff1, nil)
			identityProviderMock.On("GetIdentity", "email", "jane.dow@gmail.com").Return(fakeAff2, nil)

			d, err := time.Parse(time.RFC3339, "2016-02-26T19:15:43Z")
			identityProviderMock.On("GetOrganizations", "756be8209f265138d271a6223fa0d85085e308db", d).Return(nil, nil)
			identityProviderMock.On("GetOrganizations", "50ffba4dfbedc6dc4390fc8bde7aeec0a7191056", d).Return(nil, nil)

			// Act
			srv := NewEnricher(identityProviderMock, "0.0.1", nil)

			enrich, err := srv.EnrichMessage(&raw, expectedEnrich.MetadataEnrichedOn.UTC())
			if err != nil {
				t.Error(err)
			}

			// Assert
			assert.Equal(t, expectedEnrich.UUID, enrich.UUID)
			assert.Equal(t, expectedEnrich.AuthorMultiOrgNames, enrich.AuthorMultiOrgNames)
		})
	}

}

var rawMsgBytes = []byte(`
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
   "uuid":"88c9c0b26e5d4dc64c7ee379e8e636fbe56b308b",
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

var enrichBytes = []byte(`
{
  "id" : "<CAE4k23_0=nw4caHLXPDCygvFd47dJ2s9muxDFGPEibb2yQKh2Q@mail.gmail.com>",
  "project_ts" : 0,
  "from_user_name" : "Jon Doe",
  "tz" : 0,
  "Message-ID" : "<CAE4k23_0=nw4caHLXPDCygvFd47dJ2s9muxDFGPEibb2yQKh2Q@mail.gmail.com>",
  "uuid" : "88c9c0b26e5d4dc64c7ee379e8e636fbe56b308b",
  "author_name" : "Jon Doe",
  "root" : false,
  "from_uuid" : "",
  "author_gender_acc" : 0,
  "from_name" : "Jon Doe",
  "author_org_name" : "Intel Corporation",
  "author_user_name" : "",
  "author_bot" : false,
  "body_extract" : "On Fri, 26 Feb 2016, 03:54 Jon Doe <jon.doe at gmail.com> wrote: Go ahead Yes, go ahead.",
  "author_id" : "0dcdd908a7dbe7ff04fc8fd9d0d365c04090b895",
  "subject_analyzed" : "[Openembedded-architecture] Removing Hob for 2.1",
  "from_bot" : false,
  "project" : "yocto",
  "mbox_author_domain" : "gmail.com",
  "date" : "2016-02-26T19:15:43Z",
  "is_pipermail_message" : 1,
  "from_gender" : "male",
  "from_multiple_org_names" : null,
  "from_org_name" : "",
  "from_domain" : "",
  "list" : "https://www.openembedded.org/pipermail/openembedded-architecture/",
  "author_uuid" : "0b9e0f5cf0c26b0356b6eae246e6d15e40bfe6fd",
  "author_multi_org_names" : [
	"Intel Corporation"
  ],
  "origin" : "https://www.openembedded.org/pipermail/openembedded-architecture/",
  "size" : 0,
  "tag" : "https://www.openembedded.org/pipermail/openembedded-architecture/",
  "subject" : "[Openembedded-architecture] Removing Hob for 2.1",
  "from_id" : "",
  "author_gender" : "",
  "from_gender_acc" : "",
  "email_date" : "2016-02-26T19:15:43Z",
  "metadata__timestamp" : "2020-12-24T20:15:09.322548+00:00",
  "metadata__backend_name" : "PipermailEnrich",
  "metadata__updated_on" : "2016-02-26T19:15:43.000000+00:00",
  "metadata__enriched_on" : "2020-12-24T17:16:32.20817Z",
  "backend_version" : "0.11.1",
  "project_slug" : "yocto",
  "changed_date" : "0001-01-01T00:00:00Z"
}
`)

func messageEnrich(b []byte) (*EnrichMessage, error) {
	expectedEnrich := &EnrichMessage{}
	err := jsoniter.Unmarshal(b, expectedEnrich)
	if err != nil {
		return nil, err
	}

	return expectedEnrich, err
}

func toRawMessage(b []byte) (RawMessage, error) {
	expectedRaw := RawMessage{}
	err := jsoniter.Unmarshal(b, &expectedRaw)
	return expectedRaw, err
}

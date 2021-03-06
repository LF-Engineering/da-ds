package pipermail

import (
	"database/sql"
	"testing"
	"time"

	"github.com/LF-Engineering/dev-analytics-libraries/uuid"

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
			expected:    enrichBytes,
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

			fakeAff1 := &affiliation.Identity{ID: sql.NullString{String: "7d1d57e8a95807aaa369a4b2a3e7247320f1f80c", Valid: true},
				UUID: sql.NullString{String: "7d1d57e8a95807aaa369a4b2a3e7247320f1f80c", Valid: true}, Name: sql.NullString{String: "Jon Doe", Valid: true}, IsBot: false,
				Domain: sql.NullString{String: "", Valid: false}, OrgName: sql.NullString{}, Username: sql.NullString{String: "", Valid: false}, GenderACC: &zero,
				MultiOrgNames: nil, Gender: sql.NullString{String: unknown, Valid: true},
			}

			fakeAff2 := &affiliation.Identity{ID: sql.NullString{String: "bda3bfad69ae6d09d903128a2813e0cac7a6d6e6", Valid: true},
				UUID: sql.NullString{String: "bda3bfad69ae6d09d903128a2813e0cac7a6d6e6", Valid: true}, Name: sql.NullString{String: "Jane Doe", Valid: true}, IsBot: false,
				Domain: sql.NullString{String: "gmail.com", Valid: true}, OrgName: sql.NullString{String: dd, Valid: true}, Username: sql.NullString{String: "", Valid: false}, GenderACC: &zero,
				MultiOrgNames: []string{"Intel Corporation"}, Gender: sql.NullString{String: unknown, Valid: true},
			}

			identityProviderMock.On("GetIdentity", "email", "jon.doe@gmail.com").Return(fakeAff1, nil)
			identityProviderMock.On("GetIdentity", "email", "jane.dow@gmail.com").Return(fakeAff2, nil)

			d, err := time.Parse(time.RFC3339, "2016-02-26T19:15:43.000000+00:00")
			identityProviderMock.On("GetOrganizations", "7d1d57e8a95807aaa369a4b2a3e7247320f1f80c", d).Return(nil, nil)
			identityProviderMock.On("GetOrganizations", "bda3bfad69ae6d09d903128a2813e0cac7a6d6e6", d).Return(nil, nil)

			// Act
			srv := NewEnricher(identityProviderMock, "0.0.1", nil, nil)

			enrich, err := srv.EnrichMessage(&raw, expectedEnrich.MetadataEnrichedOn)
			if err != nil {
				t.Error(err)
			}

			// Assert
			source := Pipermail
			email := "jon.doe@gmail.com"
			authorUUID, err := uuid.GenerateIdentity(&source, &email, &expectedEnrich.AuthorName, nil)
			if err != nil {
				t.Error(err)
			}

			messageUUID, err := uuid.Generate(Pipermail, expectedEnrich.MessageID)
			if err != nil {
				t.Error(err)
			}

			assert.Equal(t, authorUUID, enrich.AuthorUUID)
			assert.Equal(t, messageUUID, enrich.UUID)
			assert.Equal(t, expectedEnrich.MessageID, enrich.MessageID)
			assert.Equal(t, expectedEnrich.Subject, enrich.Subject)
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
   "uuid":"acb13ade6f1540ceb6b72b085c94c32c7a6a540b",
   "search_fields":{
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
  "uuid" : "acb13ade6f1540ceb6b72b085c94c32c7a6a540b",
  "author_name" : "Jon Doe",
  "root" : false,
  "from_uuid" : "7d1d57e8a95807aaa369a4b2a3e7247320f1f80c",
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
  "author_uuid" : "7d1d57e8a95807aaa369a4b2a3e7247320f1f80c",
  "author_multi_org_names" : [
	"Intel Corporation"
  ],
  "origin" : "https://www.openembedded.org/pipermail/openembedded-architecture/",
  "size" : 0,
  "tag" : "https://www.openembedded.org/pipermail/openembedded-architecture/",
  "subject" : "[Openembedded-architecture] Removing Hob for 2.1",
  "from_id" : "",
  "author_gender" : "male",
  "from_gender_acc" : 0,
  "email_date" : "2016-02-26T19:15:43Z",
  "metadata__timestamp" : "2020-12-24T20:15:09.322548+00:00",
  "metadata__backend_name" : "PipermailEnrich",
  "metadata__updated_on" : "2016-02-26T19:15:43.000000+00:00",
  "metadata__enriched_on" : "2020-12-24T17:16:32.20817Z",
  "backend_version" : "0.11.1",
  "project_slug" : "yocto",
  "changed_date" : "2020-12-24T17:16:32.20817Z"
}
`)

func messageEnrich(b []byte) (EnrichMessage, error) {
	expectedEnrich := EnrichMessage{}
	err := jsoniter.Unmarshal(b, &expectedEnrich)
	return expectedEnrich, err
}

func toRawMessage(b []byte) (RawMessage, error) {
	expectedRaw := RawMessage{}
	err := jsoniter.Unmarshal(b, &expectedRaw)
	return expectedRaw, err
}

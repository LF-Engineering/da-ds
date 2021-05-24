package pipermail

import (
	"testing"
	"time"

	"github.com/LF-Engineering/da-ds/pipermail/mocks"
	"github.com/LF-Engineering/dev-analytics-libraries/affiliation"
	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
)

func TestEnrichMessage(t *testing.T) {
	type fields struct {
		DSName                string
		ElasticSearchProvider *elastic.ClientProvider
		BackendVersion        string
	}
	type args struct {
		rawItem RawMessage
		project string
		now     time.Time
	}

	rawItem1String := `{"backend_version":"0.0.1","data":{"Content-Type":"text/plain","Date":"2016-02-26T19:15:43Z","From":"jon.doe at gmail.com (Jon Doe)","In-Reply-To":"<CAMKF1spgKSosWNxwUM1suPRZD2VRbNVDsFjr55sv6GMYWCVGtw@mail.gmail.com>","MBox-Bytes-Length":742,"MBox-N-Bodies":1,"MBox-N-Lines":19,"MBox-Project-Name":"openembedded-architecture","MBox-Valid":true,"MBox-Warn":false,"Message-ID":"<CAE4k23_0=nw4caHLXPDCygvFd47dJ2s9muxDFGPEibb2yQKh2Q@mail.gmail.com>","References":"<2956615.y8hyrZheM7@peggleto-mobl.m.m.try.com><CAMKF1spgKSosWNxwUM1suPRZD2VRbNVDsFjr55sv6GMYWCVGtw@mail.gmail.com>","Subject":"[Openembedded-architecture] Removing Hob for 2.1","data":{"text":{"plain":[{"data":"On Fri, 26 Feb 2016, 03:54 Jon Doe <jon.doe at gmail.com> wrote: Go ahead Yes, go ahead."}]}},"date_in_tz":"2016-02-26T19:15:43Z","date_tz":0},"tag":"https://www.openembedded.org/pipermail/openembedded-architecture/","uuid":"acb13ade6f1540ceb6b72b085c94c32c7a6a540b","search_fields":{"item_id":"<CAE4k23_0=nw4caHLXPDCygvFd47dJ2s9muxDFGPEibb2yQKh2Q@mail.gmail.com>"},"origin":"https://www.openembedded.org/pipermail/openembedded-architecture/","updated_on":1456514143,"metadata__updated_on":"2016-02-26T19:15:43.000000+00:00","backend_name":"pipermail","metadata__timestamp":"2020-12-24T20:15:09.322548+00:00","timestamp":1608840909.322548,"category":"message","project_slug":"yocto","group_name":"openembedded-architecture","project":"yocto","changed_at":"0001-01-01T00:00:00Z","Message-ID":"<CAE4k23_0=nw4caHLXPDCygvFd47dJ2s9muxDFGPEibb2yQKh2Q@mail.gmail.com>","date":"2016-02-26T19:15:43Z"}`
	pipermailRaw1, err := toPipermailRaw(rawItem1String)
	if err != nil {
		t.Error(err)
	}

	enrichItem1String := `{"id":"<CAE4k23_0=nw4caHLXPDCygvFd47dJ2s9muxDFGPEibb2yQKh2Q@mail.gmail.com>","project_ts":0,"from_user_name":"Jon Doe","tz":0,"Message-ID":"<CAE4k23_0=nw4caHLXPDCygvFd47dJ2s9muxDFGPEibb2yQKh2Q@mail.gmail.com>","uuid":"acb13ade6f1540ceb6b72b085c94c32c7a6a540b","author_name":"Jon Doe","root":false,"from_uuid":"7d1d57e8a95807aaa369a4b2a3e7247320f1f80c","author_gender_acc":0,"from_name":"Jon Doe","author_org_name":"Intel Corporation","author_user_name":"","author_bot":false,"body_extract":"On Fri, 26 Feb 2016, 03:54 Jon Doe <jon.doe at gmail.com> wrote: Go ahead Yes, go ahead.","author_id":"0dcdd908a7dbe7ff04fc8fd9d0d365c04090b895","subject_analyzed":"[Openembedded-architecture] Removing Hob for 2.1","from_bot":false,"project":"yocto","mbox_author_domain":"gmail.com","date":"2016-02-26T19:15:43Z","is_pipermail_message":1,"from_gender":"male","from_multiple_org_names":null,"from_org_name":"","from_domain":"","list":"https://www.openembedded.org/pipermail/openembedded-architecture/","author_uuid":"7d1d57e8a95807aaa369a4b2a3e7247320f1f80c","author_multi_org_names":["Intel Corporation"],"origin":"https://www.openembedded.org/pipermail/openembedded-architecture/","size":0,"tag":"https://www.openembedded.org/pipermail/openembedded-architecture/","subject":"[Openembedded-architecture] Removing Hob for 2.1","from_id":"","author_gender":"male","from_gender_acc":0,"email_date":"2016-02-26T19:15:43Z","metadata__timestamp":"2020-12-24T20:15:09.322548+00:00","metadata__backend_name":"PipermailEnrich","metadata__updated_on":"2016-02-26T19:15:43.000000+00:00","metadata__enriched_on":"2020-12-24T17:16:32.20817Z","backend_version":"0.11.1","project_slug":"yocto","changed_date":"2020-12-24T17:16:32.20817Z"}`
	pipermailEnrich1, err := toPipermailEnrich(enrichItem1String)
	if err != nil {
		t.Error(err)
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *EnrichedMessage
		wantErr bool
	}{
		{
			name: "Test Case #1",
			fields: fields{
				DSName:                "Pipermail",
				ElasticSearchProvider: nil,
				BackendVersion:        "0.0.1",
			},
			args: args{
				rawItem: pipermailRaw1,
				project: "project1",
				now:     time.Time{},
			},
			want:    &pipermailEnrich1,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			zero := int64(0)
			aff1UUID := "7d1d57e8a95807aaa369a4b2a3e7247320f1f80c"
			fakeAff1 := &affiliation.AffIdentity{ID: &aff1UUID,
				UUID: &aff1UUID, Name: "Jon Doe", IsBot: &zero,
				Domain: "", OrgName: nil, Username: "",
				MultiOrgNames: []string{},
			}

			fakeOrganizations1 := &[]affiliation.Enrollment{
				{
					ID: 1,
					Organization: struct {
						ID   int    `json:"id"`
						Name string `json:"name"`
					}{
						ID:   1,
						Name: "Org1",
					},
					OrganizationID: 1,
					Role:           "Contributor",
					UUID:           "7d1d57e8a95807aaa369a4b2a3e7247320f1f80c",
				},
			}

			affProviderMock := &mocks.AffiliationClient{}
			affProviderMock.On("GetIdentityByUser", "email", "jon.doe@gmail.com").Return(fakeAff1, nil)
			affProviderMock.On("GetOrganizations", "7d1d57e8a95807aaa369a4b2a3e7247320f1f80c", "yocto").Return(fakeOrganizations1, nil)

			e := &Enricher{
				DSName:                     tt.fields.DSName,
				ElasticSearchProvider:      tt.fields.ElasticSearchProvider,
				affiliationsClientProvider: affProviderMock,
			}
			//
			got, err := e.EnrichMessage(&tt.args.rawItem, tt.args.now)
			if (err != nil) != tt.wantErr {
				t.Errorf("EnrichItem() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.NotEqual(t, got, nil)
			assert.Equal(t, tt.want.UUID, got.UUID)
			assert.Equal(t, tt.want.MessageID, got.MessageID)
			assert.Equal(t, tt.want.AuthorUUID, got.AuthorUUID)
			assert.Equal(t, tt.want.Origin, got.Origin)
		})
	}
}

func toPipermailEnrich(b string) (EnrichedMessage, error) {
	expectedEnrich := EnrichedMessage{}
	err := jsoniter.Unmarshal([]byte(b), &expectedEnrich)
	return expectedEnrich, err
}

func toPipermailRaw(b string) (RawMessage, error) {
	expectedRaw := RawMessage{}
	err := jsoniter.Unmarshal([]byte(b), &expectedRaw)
	return expectedRaw, err
}

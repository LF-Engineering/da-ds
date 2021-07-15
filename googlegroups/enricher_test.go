package googlegroups

import (
	"testing"
	"time"

	"github.com/LF-Engineering/da-ds/googlegroups/mocks"
	"github.com/LF-Engineering/dev-analytics-libraries/affiliation"
	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	_ "github.com/stretchr/testify/assert"
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

	rawItem1String := `{"from":"Jone Dow <jone.dow@gmail.com>","date":"2021-03-05T03:35:19-08:00","to":["GAM for Google Workspace <google-apps-manager@googlegroups.com>"],"message_id":"<000621f3-1eac-4c3a-b61f-2a785169e4d4n@googlegroups.com>","in_reply_to":"<b3cc2d39-f266-4b53-abdb-88ce674972a0n@googlegroups.com>","references":"<272d9c9b-f0ec-418d-becf-89d3d29f14c0n@googlegroups.com> <CA+VVBp-HXjixtAftJ4fAbWG8gM3nj8BSRmQ_KTVj-sR3d98WXw@mail.gmail.com> <b3cc2d39-f266-4b53-abdb-88ce674972a0n@googlegroups.com>","subject":"Re: [GAM] Cannot update some users info","message_body":"Hi Jay, One more point to add, I can update the user account info I created on Gsuite admin portal manually. But I cannot edit user account which is existing on Gsuite already even it is student or","topic_id":"","topic":"","backend_version":"0.0.1","uuid":"f9fb3a591ee4b9050f3e7e2a03b09c876e2e8413","origin":"https://groups.google.com/g/google-apps-manager","updated_on":0,"metadata__updated_on":"2021-03-05T03:35:19-08:00","backend_name":"GoogleGroupsFetch","metadata__timestamp":"2021-03-05T11:38:43.3631Z","timestamp":0,"project_slug":"project1","group_name":"google-apps-manager","project":"project1","changed_at":"2021-03-05T11:38:43.3631Z","timezone":-8}`
	googleGroupsRaw1, err := toGoogleGroupsRaw(rawItem1String)
	if err != nil {
		t.Error(err)
	}

	enrichItem1String := `{"from":"Jone Dow","date":"2021-03-05T03:35:19-08:00","to":["GAM for Google Workspace <google-apps-manager@googlegroups.com>"],"message_id":"<000621f3-1eac-4c3a-b61f-2a785169e4d4n@googlegroups.com>","in_reply_to":"<b3cc2d39-f266-4b53-abdb-88ce674972a0n@googlegroups.com>","references":"<272d9c9b-f0ec-418d-becf-89d3d29f14c0n@googlegroups.com> <CA+VVBp-HXjixtAftJ4fAbWG8gM3nj8BSRmQ_KTVj-sR3d98WXw@mail.gmail.com> <b3cc2d39-f266-4b53-abdb-88ce674972a0n@googlegroups.com>","subject":"Re: [GAM] Cannot update some users info","topic":"","message_body":"Hi Jay, One more point to add, I can update the user account info I created on Gsuite admin portal manually. But I cannot edit user account which is existing on Gsuite already even it is student or","topic_id":"","backend_version":"0.0.1","uuid":"f9fb3a591ee4b9050f3e7e2a03b09c876e2e8413","origin":"https://groups.google.com/g/google-apps-manager","updated_on":0,"metadata__updated_on":"2021-03-05T03:35:19-08:00","backend_name":"GoogleGroupsEnrich","metadata__timestamp":"2021-03-05T11:38:43.3631Z","metadata__enriched_on":"2021-03-05T11:40:45.22395Z","timestamp":0,"project_slug":"project1","group_name":"google-apps-manager","project":"project1","root":false,"from_bot":false,"changed_at":"2021-03-05T11:38:43.3631Z","author_name":"Jone Dow","author_id":"20328dba9d970328af607179cd21b25039d85340","author_uuid":"20328dba9d970328af607179cd21b25039d85340","author_gender":"Unknown","author_org_name":"Unknown","author_user_name":"","author_bot":false,"author_gender_acc":0,"author_multi_org_names":["Unknown"],"mbox_author_domain":"g.lfis.edu.hk","is_google_group_message":1,"timezone":-8}`
	googleGroupsEnrich1, err := toGoogleGroupsEnrich(enrichItem1String)
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
				DSName:                "GoogleGroups",
				ElasticSearchProvider: nil,
				BackendVersion:        "0.0.1",
			},
			args: args{
				rawItem: googleGroupsRaw1,
				project: "project1",
				now:     time.Time{},
			},
			want:    &googleGroupsEnrich1,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			zero := int64(0)
			aff1UUID := "20328dba9d970328af607179cd21b25039d85340"
			fakeAff1 := &affiliation.AffIdentity{ID: &aff1UUID,
				UUID: &aff1UUID, Name: "Qian", IsBot: &zero,
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
					UUID:           "20328dba9d970328af607179cd21b25039d85340",
				},
			}
			userIdentity := affiliation.Identity{
				Name:   "Jone Dow",
				Source: "googlegroups",
				Email:  "jone.dow@gmail.com",
				ID:     "894b751382341e2d958ba48f235c37b75690b194",
			}
			affProviderMock := &mocks.AffiliationClient{}
			affProviderMock.On("GetIdentityByUser", "id", "894b751382341e2d958ba48f235c37b75690b194").Return(fakeAff1, nil)
			affProviderMock.On("GetOrganizations", "20328dba9d970328af607179cd21b25039d85340", "project1").Return(fakeOrganizations1, nil)
			affProviderMock.On("AddIdentity", &userIdentity).Return(true)

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

func toGoogleGroupsEnrich(b string) (EnrichedMessage, error) {
	expectedEnrich := EnrichedMessage{}
	err := jsoniter.Unmarshal([]byte(b), &expectedEnrich)
	return expectedEnrich, err
}

func toGoogleGroupsRaw(b string) (RawMessage, error) {
	expectedRaw := RawMessage{}
	err := jsoniter.Unmarshal([]byte(b), &expectedRaw)
	return expectedRaw, err
}

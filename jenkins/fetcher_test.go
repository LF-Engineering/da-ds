package jenkins

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/LF-Engineering/da-ds/utils"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"gopkg.in/h2non/gock.v1"
)

// TestFetcherSimpleFetchItem tests the working of the fetchItem function
func TestBasicFetchItem(t *testing.T) {
	gock.New("https://www.jenkins-mock.com").
		Get("/api/json").
		Reply(200).
		JSON(map[string]interface{}{
			"jobs": []map[string]interface{}{
				{
					"_class": "org.jenkinsci.plugins.workflow.job.WorkflowJob",
					"name":   "1.0.0",
					"url":    "https://www.jenkins-mock.com/job/1.0.0/",
					"color":  "blue",
				},
			},
		})
	gock.New("https://www.jenkins-mock.com/job/1.0.0/").
		Get("/api/json").
		Reply(200).
		JSON(map[string]interface{}{
			"builds": []map[string]interface{}{
				{
					"_class":            "org.jenkinsci.plugins.workflow.job.WorkflowRun",
					"building":          false,
					"description":       nil,
					"displayName":       "#2",
					"duration":          1343151,
					"estimatedDuration": 1081333,
					"executor":          nil,
					"fullDisplayName":   "iroha » iroha-hyperledger » 1.0.0 #2",
					"id":                "2",
					"keepLog":           false,
					"number":            2,
					"queueId":           404128,
					"result":            "SUCCESS",
					"timestamp":         1557215745020,
					"url":               "https://jenkins.soramitsu.co.jp/job/iroha/job/iroha-hyperledger/job/1.0.0/2/",
					"nextBuild":         nil,
					"previousBuild": map[string]interface{}{
						"number": 1,
						"url":    "https://jenkins.soramitsu.co.jp/job/iroha/job/iroha-hyperledger/job/1.0.0/1/",
					},
				},
				{
					"_class":            "org.jenkinsci.plugins.workflow.job.WorkflowRun",
					"building":          false,
					"description":       nil,
					"displayName":       "#1",
					"duration":          819514,
					"estimatedDuration": 1081333,
					"executor":          nil,
					"fullDisplayName":   "iroha » iroha-hyperledger » 1.0.0 #1",
					"id":                "1",
					"keepLog":           false,
					"number":            1,
					"queueId":           403836,
					"result":            "SUCCESS",
					"timestamp":         1557178382190,
					"url":               "https://jenkins.soramitsu.co.jp/job/iroha/job/iroha-hyperledger/job/1.0.0/1/",
					"nextBuild": map[string]interface{}{
						"number": 2,
						"url":    "https://jenkins.soramitsu.co.jp/job/iroha/job/iroha-hyperledger/job/1.0.0/2/",
					},
				},
			}})
	var expectedJenkinsRaw []BuildsRaw
	expectedRaw := `[{"backend_name":"jenkins","backend_version":"0.0.1","perceval_version":"","timestamp":0,"origin":"https://www.jenkins-mock.com","uuid":"0264b85b4465b98bf11fa5ec2aed63208f664245","updated_on":1557215745.02,"classified_fields_filtered":null,"category":"build","search_fields":{"item_id":"https://jenkins.soramitsu.co.jp/job/iroha/job/iroha-hyperledger/job/1.0.0/2/","number":2},"tag":"https://www.jenkins-mock.com","data":{"_class":"org.jenkinsci.plugins.workflow.job.WorkflowRun","actions":null,"artifacts":null,"building":false,"description":null,"displayName":"#2","duration":1343151,"estimatedDuration":1081333,"executor":null,"fullDisplayName":"iroha » iroha-hyperledger » 1.0.0 #2","id":"2","keepLog":false,"number":2,"queueId":404128,"result":"SUCCESS","timestamp":1557215745020,"url":"https://jenkins.soramitsu.co.jp/job/iroha/job/iroha-hyperledger/job/1.0.0/2/","builtOn":"","changeSet":{"_class":"","items":null,"kind":""},"culprits":null,"runs":null},"metadata__updated_on":"2020-12-22T19:21:30.884914298+05:30","metadata__timestamp":"2020-12-22T19:21:30.884914385+05:30"},{"backend_name":"jenkins","backend_version":"0.0.1","perceval_version":"","timestamp":0,"origin":"https://www.jenkins-mock.com","uuid":"37401f86e484931309c97de3f790aa3bb6ff5fac","updated_on":1557178382.19,"classified_fields_filtered":null,"category":"build","search_fields":{"item_id":"https://jenkins.soramitsu.co.jp/job/iroha/job/iroha-hyperledger/job/1.0.0/1/","number":1},"tag":"https://www.jenkins-mock.com","data":{"_class":"org.jenkinsci.plugins.workflow.job.WorkflowRun","actions":null,"artifacts":null,"building":false,"description":null,"displayName":"#1","duration":819514,"estimatedDuration":1081333,"executor":null,"fullDisplayName":"iroha » iroha-hyperledger » 1.0.0 #1","id":"1","keepLog":false,"number":1,"queueId":403836,"result":"SUCCESS","timestamp":1557178382190,"url":"https://jenkins.soramitsu.co.jp/job/iroha/job/iroha-hyperledger/job/1.0.0/1/","builtOn":"","changeSet":{"_class":"","items":null,"kind":""},"culprits":null,"runs":null},"metadata__updated_on":"2020-12-22T19:21:30.88491902+05:30","metadata__timestamp":"2020-12-22T19:21:30.884919102+05:30"}]`
	_ = json.Unmarshal([]byte(expectedRaw), &expectedJenkinsRaw)
	type fields struct {
		DSName                string
		IncludeArchived       bool
		MultiOrigin           bool
		HTTPClientProvider    HTTPClientProvider
		ElasticSearchProvider ESClientProvider
		BackendVersion        string
	}
	type args struct {
		params *Params
	}
	httpClientProvider := utils.NewHTTPClientProvider(time.Second * 600)
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []BuildsRaw
		wantErr bool
	}{
		{
			"Get the data",
			fields{
				DSName:                "jenkins",
				IncludeArchived:       false,
				HTTPClientProvider:    httpClientProvider,
				ElasticSearchProvider: nil,
				BackendVersion:        "0.0.1",
			},
			args{params: &Params{
				JenkinsURL:     "https://www.jenkins-mock.com",
				Depth:          1,
				BackendVersion: "0.0.1",
			}},
			expectedJenkinsRaw,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Fetcher{
				DSName:                tt.fields.DSName,
				IncludeArchived:       tt.fields.IncludeArchived,
				HTTPClientProvider:    tt.fields.HTTPClientProvider,
				ElasticSearchProvider: tt.fields.ElasticSearchProvider,
				BackendVersion:        tt.fields.BackendVersion,
			}
			_, err := f.FetchItem(tt.args.params)

			if (err != nil) != tt.wantErr {
				t.Errorf("FetchItem() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestFetchItem(t *testing.T) {
	gock.New("https://www.jenkins-mock.com").
		Get("/api/json").
		Reply(200).
		JSON(map[string]interface{}{
			"jobs": []map[string]interface{}{
				{
					"_class": "org.jenkinsci.plugins.workflow.job.WorkflowJob",
					"name":   "1.0.0",
					"url":    "https://www.jenkins-mock.com/job/1.0.0/",
					"color":  "blue",
				},
			},
		})
	// The empty job
	gock.New("https://www.jenkins-mock-fail.com").
		Get("/api/json").
		Reply(200).
		JSON(map[string]interface{}{
			"jobs": []map[string]interface{}{
				{
					"_class": "org.jenkinsci.plugins.workflow.job.WorkflowJob",
					"name":   "2.0.0",
					"url":    "https://www.jenkins-mock.com/job/2.0.0/",
					"color":  "blue",
				},
			},
		})
	gock.New("https://www.jenkins-mock.com/job/1.0.0/").
		Get("/api/json").
		Reply(200).
		JSON(map[string]interface{}{
			"builds": []map[string]interface{}{
				{
					"_class":            "org.jenkinsci.plugins.workflow.job.WorkflowRun",
					"building":          false,
					"description":       nil,
					"displayName":       "#2",
					"duration":          1343151,
					"estimatedDuration": 1081333,
					"executor":          nil,
					"fullDisplayName":   "iroha » iroha-hyperledger » 1.0.0 #2",
					"id":                "2",
					"keepLog":           false,
					"number":            2,
					"queueId":           404128,
					"result":            "SUCCESS",
					"timestamp":         1557215745020,
					"url":               "https://jenkins.soramitsu.co.jp/job/iroha/job/iroha-hyperledger/job/1.0.0/2/",
					"nextBuild":         nil,
					"previousBuild": map[string]interface{}{
						"number": 1,
						"url":    "https://jenkins.soramitsu.co.jp/job/iroha/job/iroha-hyperledger/job/1.0.0/1/",
					},
				},
				{
					"_class":            "org.jenkinsci.plugins.workflow.job.WorkflowRun",
					"building":          false,
					"description":       nil,
					"displayName":       "#1",
					"duration":          819514,
					"estimatedDuration": 1081333,
					"executor":          nil,
					"fullDisplayName":   "iroha » iroha-hyperledger » 1.0.0 #1",
					"id":                "1",
					"keepLog":           false,
					"number":            1,
					"queueId":           403836,
					"result":            "SUCCESS",
					"timestamp":         1557178382190,
					"url":               "https://jenkins.soramitsu.co.jp/job/iroha/job/iroha-hyperledger/job/1.0.0/1/",
					"nextBuild": map[string]interface{}{
						"number": 2,
						"url":    "https://jenkins.soramitsu.co.jp/job/iroha/job/iroha-hyperledger/job/1.0.0/2/",
					},
				},
			}})
	var expectedJenkinsRaw []BuildsRaw
	expectedRaw := `[{"backend_name":"jenkins","backend_version":"0.0.1","perceval_version":"","timestamp":0,"origin":"https://www.jenkins-mock.com","uuid":"0264b85b4465b98bf11fa5ec2aed63208f664245","updated_on":1557215745.02,"classified_fields_filtered":null,"category":"build","search_fields":{"item_id":"https://jenkins.soramitsu.co.jp/job/iroha/job/iroha-hyperledger/job/1.0.0/2/","number":2},"tag":"https://www.jenkins-mock.com","data":{"_class":"org.jenkinsci.plugins.workflow.job.WorkflowRun","actions":null,"artifacts":null,"building":false,"description":null,"displayName":"#2","duration":1343151,"estimatedDuration":1081333,"executor":null,"fullDisplayName":"iroha » iroha-hyperledger » 1.0.0 #2","id":"2","keepLog":false,"number":2,"queueId":404128,"result":"SUCCESS","timestamp":1557215745020,"url":"https://jenkins.soramitsu.co.jp/job/iroha/job/iroha-hyperledger/job/1.0.0/2/","builtOn":"","changeSet":{"_class":"","items":null,"kind":""},"culprits":null,"runs":null},"metadata__updated_on":"2020-12-22T19:21:30.884914298+05:30","metadata__timestamp":"2020-12-22T19:21:30.884914385+05:30"},{"backend_name":"jenkins","backend_version":"0.0.1","perceval_version":"","timestamp":0,"origin":"https://www.jenkins-mock.com","uuid":"37401f86e484931309c97de3f790aa3bb6ff5fac","updated_on":1557178382.19,"classified_fields_filtered":null,"category":"build","search_fields":{"item_id":"https://jenkins.soramitsu.co.jp/job/iroha/job/iroha-hyperledger/job/1.0.0/1/","number":1},"tag":"https://www.jenkins-mock.com","data":{"_class":"org.jenkinsci.plugins.workflow.job.WorkflowRun","actions":null,"artifacts":null,"building":false,"description":null,"displayName":"#1","duration":819514,"estimatedDuration":1081333,"executor":null,"fullDisplayName":"iroha » iroha-hyperledger » 1.0.0 #1","id":"1","keepLog":false,"number":1,"queueId":403836,"result":"SUCCESS","timestamp":1557178382190,"url":"https://jenkins.soramitsu.co.jp/job/iroha/job/iroha-hyperledger/job/1.0.0/1/","builtOn":"","changeSet":{"_class":"","items":null,"kind":""},"culprits":null,"runs":null},"metadata__updated_on":"2020-12-22T19:21:30.88491902+05:30","metadata__timestamp":"2020-12-22T19:21:30.884919102+05:30"}]`
	err := json.Unmarshal([]byte(expectedRaw), &expectedJenkinsRaw)
	fmt.Println(err)
	type fields struct {
		DSName                string
		IncludeArchived       bool
		MultiOrigin           bool
		HTTPClientProvider    HTTPClientProvider
		ElasticSearchProvider ESClientProvider
		BackendVersion        string
	}
	type args struct {
		params *Params
	}
	httpClientProvider := utils.NewHTTPClientProvider(time.Second * 600)
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []BuildsRaw
		wantErr bool
	}{
		{
			"Fetch Jenkins data",
			fields{
				DSName:                "jenkins",
				IncludeArchived:       false,
				HTTPClientProvider:    httpClientProvider,
				ElasticSearchProvider: nil,
				BackendVersion:        "0.0.1",
			},
			args{params: &Params{
				JenkinsURL:     "https://www.jenkins-mock.com",
				Depth:          1,
				BackendVersion: "0.0.1",
			}},
			expectedJenkinsRaw,
			false,
		},
		{
			"Fetch Jenkins data from unavailable URL",
			fields{
				DSName:                "jenkins",
				IncludeArchived:       false,
				HTTPClientProvider:    httpClientProvider,
				ElasticSearchProvider: nil,
				BackendVersion:        "0.0.1",
			},
			args{params: &Params{
				JenkinsURL:     "https://www.jenkins-mock-fail.com",
				Depth:          1,
				BackendVersion: "0.0.1",
			}},
			[]BuildsRaw{},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Fetcher{
				DSName:                tt.fields.DSName,
				IncludeArchived:       tt.fields.IncludeArchived,
				HTTPClientProvider:    tt.fields.HTTPClientProvider,
				ElasticSearchProvider: tt.fields.ElasticSearchProvider,
				BackendVersion:        tt.fields.BackendVersion,
			}
			got, err := f.FetchItem(tt.args.params)

			if (err != nil) != tt.wantErr {
				t.Errorf("FetchItem() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			for i := 0; i < len(got); i++ {
				assert.Equal(t, got[i].BackendVersion, tt.want[i].BackendVersion)
				assert.Equal(t, got[i].UUID, tt.want[i].UUID)
				assert.Equal(t, got[i].Origin, tt.want[i].Origin)
				assert.Equal(t, got[i].Data, tt.want[i].Data)
			}
		})
	}
}

func toJenkinsRaw(b string) (BuildsRaw, error) {
	expectedRaw := BuildsRaw{}
	err := jsoniter.Unmarshal([]byte(b), &expectedRaw)
	return expectedRaw, err
}

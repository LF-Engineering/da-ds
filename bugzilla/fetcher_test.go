package bugzilla

import (
	"fmt"
	"github.com/LF-Engineering/da-ds/dockerhub/mocks"
	"github.com/LF-Engineering/da-ds/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

func TestFetchItem(t *testing.T) {
	// Arrange

	params := &Params{
		BackendVersion: "0.0.1",
		Endpoint:       "https://bugzilla.yoctoproject.org",
	}
	httpClientProviderMock := &mocks.HttpClientProvider{}

	fakeResult := `bug_id,"product","component","assigned_to","bug_status","resolution","short_desc","changeddate"
13579,"Toaster","toaster","david.reyna","RESOLVED","FIXED","Enable Zeus branch in place of Thud","2020-01-02 15:45:20"
8248,"Toaster","toaster","toaster","NEW","---","Toaster recipe page: missing empty state","2020-01-03 03:59:54"
8434,"Toaster","toaster","toaster","NEW","---","Toaster: package classes (rpm, ipk or deb) should be display in the build configuration page","2020-01-03 04:00:04"
9117,"Toaster","toaster","toaster","NEW","---","Allow ""project builds"" and ""all builds"" to be sorted by time and recipe name","2020-01-03 04:00:12"
10281,"Toaster","toaster","toaster","NEW","---","Configuration variables: I can set a value for standard shell environment variable http_proxy","2020-01-03 04:00:25"
8424,"Toaster","toaster","toaster","NEW","---","Toaster: Compatible recipes tab should display the machine compatible recipes","2020-01-03 04:01:14"
8425,"Toaster","toaster","toaster","NEW","---","Toaster: A warning should be displayed when you delete a layer dependency","2020-01-03 04:01:25"
8685,"Toaster","toaster","toaster","NEW","---","Enter button doesn't apply your modification to bitbake variables","2020-01-03 04:01:34"
9839,"Toaster","toaster","unassigned","NEW","---","The ""clear search"" button on the  – ""Add | Remove packages table"" - does not clear","2020-01-03 04:01:43"
8577,"Toaster","toaster","unassigned","NEW","---","Add a global UI widget to show progress of builds","2020-01-03 04:01:54"
7294,"Toaster","toaster","unassigned","NEW","---","For QA Issues errors and warnings, toaster should recognize the error and provide suggestions to the user on how the error can be repaired","2020-01-03 04:02:27"
10205,"Toaster","toaster","toaster","NEW","---","The layer typeahead shows the project release branch for non-git layers","2020-01-03 04:02:35"
9977,"Toaster","toaster","toaster","NEW","---","Time, CPU and Disk I/O tables have the wrong core columns","2020-01-03 04:02:43"
9856,"Toaster","toaster","toaster","NEW","---","Migrate remaining tables to ToasterTable","2020-01-03 04:02:53"
9670,"Toaster","toaster","toaster","NEW","---","Toaster does not set a correct MIME type for tasks' logs files","2020-01-03 04:03:01"
9209,"Toaster","toaster","toaster","ACCEPTED","---","The unique name validation for custom images happens across projects","2020-01-03 04:03:10"
9435,"Toaster","toaster","toaster","NEW","---","Toaster throws an exception for images that have files on the / level","2020-01-03 04:03:46"
9118,"Toaster","toaster","toaster","NEW","---","Dead paths for meta-toaster-custom layer left behind in bblayers.conf","2020-01-03 04:03:55"
9141,"Toaster","toaster","toaster","NEW","---","toaster when xmlrpc/cooker dies the web didn't tell the user about it","2020-01-03 04:04:02"
8865,"Toaster","toaster","toaster","NEW","---","The project ""updated"" field shows the last time a project was saved, not its last activity","2020-01-03 04:04:11"`

	httpClientProviderMock.On("Request",
		fmt.Sprintf("https://bugzilla.yoctoproject.org/buglist.cgi?chfieldfrom=2020-01-01 12:00:00&ctype=csv&limit=20&order=changeddate"),
		"GET", mock.Anything, mock.Anything).Return(200, []byte(fakeResult), nil)

	esClientProviderMock := &mocks.ESClientProvider{}

	fmt.Println(httpClientProviderMock)
	httpClientProvider := utils.NewHttpClientProvider(time.Duration(50*time.Second))
	srv := NewFetcher(params, httpClientProvider, esClientProviderMock)

	// Act
	_, err := srv.FetchItem(time.Now(),1000)

	// Assert
	assert.NoError(t, err)

}

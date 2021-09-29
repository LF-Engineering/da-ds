package gitlab

import (
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/LF-Engineering/dev-analytics-libraries/affiliation"
	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
	"github.com/LF-Engineering/dev-analytics-libraries/uuid"
)

type AffiliationClient interface {
	GetIdentityByUser(key string, value string) (*affiliation.AffIdentity, error)
	AddIdentity(identity *affiliation.Identity) bool
	GetOrganizations(uuid string, projectSlug string) *[]affiliation.Enrollment
}

type Enricher struct {
	DSName                     string
	ElasticSearchProvider      *elastic.ClientProvider
	affiliationsClientProvider AffiliationClient
}

// NewEnricher initiates a new Enricher
func NewEnricher(esClientProvider *elastic.ClientProvider, affiliationsClientProvider *affiliation.Affiliation) *Enricher {
	return &Enricher{
		DSName:                     DATASOURCE,
		ElasticSearchProvider:      esClientProvider,
		affiliationsClientProvider: affiliationsClientProvider,
	}
}

func (e *Enricher) EnrichIssue(rawItem IssueRaw, now time.Time) (*IssueEnrich, error) {

	enrichedIssue := IssueEnrich{
		BackendName:         fmt.Sprintf("%sEnrich", strings.Title(e.DSName)),
		BackendVersion:      rawItem.BackendVersion,
		Title:               rawItem.Data.Title,
		UUID:                rawItem.UUID,
		ProjectSlug:         rawItem.ProjectSlug,
		Project:             rawItem.Project,
		MetadataUpdatedOn:   rawItem.MetadataUpdatedOn,
		MetadataTimestamp:   rawItem.MetadataTimestamp,
		MetadataEnrichedOn:  now,
		Body:                rawItem.Data.Description,
		BodyAnalyzed:        rawItem.Data.Description,
		CreatedAt:           rawItem.Data.CreatedAt,
		ClosedAt:            rawItem.Data.ClosedAt,
		UpdatedAt:           rawItem.Data.UpdatedAt,
		Origin:              rawItem.Repo,
		AuthorAvatarUrl:     rawItem.Data.Author.AvatarURL,
		AuthorMultiOrgNames: []string{Unknown},
		AuthorOrgName:       Unknown,
		AuthorLogin:         rawItem.Data.Author.Username,
		Type:                rawItem.Data.Type,
		Url:                 rawItem.Data.WebURL,
		UrlID:               getIssueUrlID(rawItem.Repo, rawItem.Data.IssueID),
		Repository:          rawItem.Repo,
		State:               rawItem.Data.State,
		Tag:                 rawItem.Repo,
		Category:            rawItem.Data.Type,
		ItemType:            rawItem.Data.Type,
		IssueID:             rawItem.Data.ID,
		IsGitlabIssue:       1,
		IdInRepo:            rawItem.Data.IssueID,
		ID:                  getIssueUrlID(rawItem.Repo, rawItem.Data.IssueID),
		GitlabRepo:          getIssueRepoShort(rawItem.Repo),
		Reponame:            rawItem.Repo,
		RepoShortname:       getProjectShortname(rawItem.Repo),
		NoOfAssignees:       len(rawItem.Data.Assignees),
		NoOfComments:        rawItem.Data.UserNotesCount,
		NoOfReactions:       rawItem.Data.Upvotes + rawItem.Data.Downvotes,
		NoOfTotalComments:   rawItem.Data.UserNotesCount,
		UserAvatarUrl:       rawItem.Data.Author.AvatarURL,
		UserLogin:           rawItem.Data.Author.Username,
		UserDataOrgName:     Unknown,
	}

	enrichedIssue.Labels = append(enrichedIssue.Labels, rawItem.Data.Labels...)

	source := DATASOURCE
	authorUsername := rawItem.Data.Author.Username
	authorName := rawItem.Data.Author.Name
	authorUUID, err := uuid.GenerateIdentity(&source, nil, &authorName, &authorUsername)
	if err != nil {
		return nil, err
	}

	userData, err := e.affiliationsClientProvider.GetIdentityByUser("id", authorUUID)
	if err != nil {
		errMessage := fmt.Sprintf("%+v : %+v", authorUUID, err)
		log.Println(errMessage)
	}

	if userData != nil {
		if userData.ID != nil {
			enrichedIssue.AuthorID = *userData.ID
			enrichedIssue.UserDataID = *userData.ID
		}
		if userData.Name != "" {
			enrichedIssue.AuthorName = userData.Name
			enrichedIssue.Username = userData.Name
			enrichedIssue.UserDataName = userData.Name
		}

		if userData.UUID != nil {
			enrichedIssue.AuthorUUID = *userData.UUID
			enrichedIssue.UserDataUUID = *userData.UUID
		}

		if userData.Domain != "" {
			enrichedIssue.AuthorDomain = userData.Domain
			enrichedIssue.UserDataDomain = userData.Domain
		}

		if userData.UUID != nil {
			slug := rawItem.ProjectSlug
			enrollments := e.affiliationsClientProvider.GetOrganizations(*userData.UUID, slug)
			if enrollments != nil {
				metaDataEpochMills := enrichedIssue.MetadataUpdatedOn.UnixNano() / 1000000
				organizations := make([]string, 0)
				for _, enrollment := range *enrollments {
					organizations = append(organizations, enrollment.Organization.Name)
				}

				foundEnrollment := false
				for _, enrollment := range *enrollments {
					affStartEpoch := enrollment.Start.UnixNano() / 1000000
					affEndEpoch := enrollment.End.UnixNano() / 1000000
					if affStartEpoch <= metaDataEpochMills && affEndEpoch >= metaDataEpochMills {
						enrichedIssue.AuthorOrgName = enrollment.Organization.Name
						enrichedIssue.UserDataOrgName = enrollment.Organization.Name
						foundEnrollment = true
						break
					}
				}

				if len(organizations) != 0 {
					enrichedIssue.AuthorMultiOrgNames = organizations
					enrichedIssue.UserDataMultiOrgNames = organizations
				}

				if !foundEnrollment && len(organizations) >= 1 {
					enrichedIssue.AuthorOrgName = organizations[0]
					enrichedIssue.UserDataOrgName = organizations[0]
				}
			}

		}

		if userData.IsBot != nil {
			if *userData.IsBot == 1 {
				enrichedIssue.AuthorBot = true
				enrichedIssue.UserDataBot = true
			}
		}
	} else {
		userIdentity := affiliation.Identity{
			LastModified: now,
			Name:         authorName,
			Source:       source,
			Username:     authorUsername,
			ID:           authorUUID,
		}

		if ok := e.affiliationsClientProvider.AddIdentity(&userIdentity); !ok {
			log.Printf("failed to add identity for [%+v]", authorUsername)
		}

		enrichedIssue.AuthorID = authorUUID
		enrichedIssue.AuthorUUID = authorUUID
		enrichedIssue.AuthorName = authorName

		enrichedIssue.UserDataID = authorUUID
		enrichedIssue.UserDataUUID = authorUUID
		enrichedIssue.UserDataName = authorName
	}

	return &enrichedIssue, nil

}

func (e *Enricher) EnrichMergeRequest(rawItem MergeRequestRaw, now time.Time) (*MergeReqestEnrich, error) {

	enrichedMergeRequest := MergeReqestEnrich{
		BackendName:            fmt.Sprintf("%sEnrich", strings.Title(e.DSName)),
		BackendVersion:         rawItem.BackendVersion,
		Title:                  rawItem.Data.Title,
		UUID:                   rawItem.UUID,
		ProjectSlug:            rawItem.ProjectSlug,
		Project:                rawItem.Project,
		MetadataUpdatedOn:      rawItem.MetadataUpdatedOn,
		MetadataTimestamp:      rawItem.MetadataTimestamp,
		MetadataEnrichedOn:     now,
		Body:                   rawItem.Data.Description,
		BodyAnalyzed:           rawItem.Data.Description,
		CreatedAt:              rawItem.Data.CreatedAt,
		ClosedAt:               rawItem.Data.ClosedAt,
		UpdatedAt:              rawItem.Data.UpdatedAt,
		Origin:                 rawItem.Repo,
		AuthorAvatarUrl:        rawItem.Data.Author.AvatarURL,
		AuthorMultiOrgNames:    []string{Unknown},
		AuthorOrgName:          Unknown,
		AuthorLogin:            rawItem.Data.Author.Username,
		Type:                   rawItem.Data.Type,
		Url:                    rawItem.Data.WebURL,
		UrlID:                  getIssueUrlID(rawItem.Repo, rawItem.Data.MergeRequestID),
		Repository:             rawItem.Repo,
		State:                  rawItem.Data.State,
		Tag:                    rawItem.Repo,
		Category:               rawItem.Data.Type,
		ItemType:               rawItem.Data.Type,
		MergeRequestID:         rawItem.Data.ID,
		IsGitlabMergeRequest:   1,
		IdInRepo:               rawItem.Data.MergeRequestID,
		ID:                     getIssueUrlID(rawItem.Repo, rawItem.Data.MergeRequestID),
		GitlabRepo:             getIssueRepoShort(rawItem.Repo),
		Reponame:               rawItem.Repo,
		RepoShortname:          getProjectShortname(rawItem.Repo),
		NoOfAssignees:          len(rawItem.Data.Assignees),
		NoOfRequestedReviewers: len(rawItem.Data.Reviewers),
		NoOfComments:           rawItem.Data.UserNotesCount,
		NoOfReactions:          rawItem.Data.Upvotes + rawItem.Data.Downvotes,
		NoOfTotalComments:      rawItem.Data.UserNotesCount,
		UserAvatarUrl:          rawItem.Data.Author.AvatarURL,
		UserLogin:              rawItem.Data.Author.Username,
		UserDataOrgName:        Unknown,
	}

	enrichedMergeRequest.Labels = append(enrichedMergeRequest.Labels, rawItem.Data.Labels...)

	source := DATASOURCE
	authorUsername := rawItem.Data.Author.Username
	authorName := rawItem.Data.Author.Name
	authorUUID, err := uuid.GenerateIdentity(&source, nil, &authorName, &authorUsername)
	if err != nil {
		return nil, err
	}

	userData, err := e.affiliationsClientProvider.GetIdentityByUser("id", authorUUID)
	if err != nil {
		errMessage := fmt.Sprintf("BOOM: %+v : %+v", authorUUID, err)
		log.Println(errMessage)
	}

	if userData != nil {
		if userData.ID != nil {
			enrichedMergeRequest.AuthorID = *userData.ID
			enrichedMergeRequest.UserDataID = *userData.ID
		}
		if userData.Name != "" {
			enrichedMergeRequest.AuthorName = userData.Name
			enrichedMergeRequest.Username = userData.Name
			enrichedMergeRequest.UserDataName = userData.Name
		}

		if userData.UUID != nil {
			enrichedMergeRequest.AuthorUUID = *userData.UUID
			enrichedMergeRequest.UserDataUUID = *userData.UUID
		}

		if userData.Domain != "" {
			enrichedMergeRequest.AuthorDomain = userData.Domain
			enrichedMergeRequest.UserDataDomain = userData.Domain
		}

		if userData.UUID != nil {
			slug := rawItem.ProjectSlug
			enrollments := e.affiliationsClientProvider.GetOrganizations(*userData.UUID, slug)
			if enrollments != nil {
				metaDataEpochMills := enrichedMergeRequest.MetadataUpdatedOn.UnixNano() / 1000000
				organizations := make([]string, 0)
				for _, enrollment := range *enrollments {
					organizations = append(organizations, enrollment.Organization.Name)
				}

				foundEnrollment := false
				for _, enrollment := range *enrollments {
					affStartEpoch := enrollment.Start.UnixNano() / 1000000
					affEndEpoch := enrollment.End.UnixNano() / 1000000
					if affStartEpoch <= metaDataEpochMills && affEndEpoch >= metaDataEpochMills {
						enrichedMergeRequest.AuthorOrgName = enrollment.Organization.Name
						enrichedMergeRequest.UserDataOrgName = enrollment.Organization.Name
						foundEnrollment = true
						break
					}
				}

				if len(organizations) != 0 {
					enrichedMergeRequest.AuthorMultiOrgNames = organizations
					enrichedMergeRequest.UserDataMultiOrgNames = organizations
				}

				if !foundEnrollment && len(organizations) >= 1 {
					enrichedMergeRequest.AuthorOrgName = organizations[0]
					enrichedMergeRequest.UserDataOrgName = organizations[0]
				}
			}

		}

		if userData.IsBot != nil {
			if *userData.IsBot == 1 {
				enrichedMergeRequest.AuthorBot = true
				enrichedMergeRequest.UserDataBot = true
			}
		}
	} else {
		userIdentity := affiliation.Identity{
			LastModified: now,
			Name:         authorName,
			Source:       source,
			Username:     authorUsername,
			ID:           authorUUID,
		}

		if ok := e.affiliationsClientProvider.AddIdentity(&userIdentity); !ok {
			log.Printf("failed to add identity for [%+v]", authorUsername)
		}

		enrichedMergeRequest.AuthorID = authorUUID
		enrichedMergeRequest.AuthorUUID = authorUUID
		enrichedMergeRequest.AuthorName = authorName

		enrichedMergeRequest.UserDataID = authorUUID
		enrichedMergeRequest.UserDataUUID = authorUUID
		enrichedMergeRequest.UserDataName = authorName
	}

	return &enrichedMergeRequest, nil
}

func getProjectShortname(repoUrl string) (projectURL string) {
	repoInChunks := strings.Split(repoUrl, "/")

	return repoInChunks[len(repoInChunks)-1]
}

func getIssueUrlID(repo string, issueID int) (urlID string) {
	u, err := url.Parse(repo)
	if err != nil {
		fmt.Println("URL Parsing Error:", err)
	}

	path := strings.TrimLeft(u.Path, "/")
	urlID = fmt.Sprintf("%s/%s", path, strconv.Itoa(issueID))

	return urlID
}

func getIssueRepoShort(repo string) (projectURL string) {
	u, err := url.Parse(repo)
	if err != nil {
		fmt.Println("URL Parsing Error:", err)
	}

	return strings.TrimLeft(u.Path, "/")
}

package pipermail

import "time"

const (
	ModMboxThreadStr     = "/thread"
	Pipermail            = "pipermail"
	PiperBackendVersion  = "0.11.1"
	ArchiveDownloadsPath = "/Users/code/.dads/mailinglists/"
	MessageDateField     = "date"
	Message              = "message"
	MessageIDField       = "Message-ID"
)

var (
	CompressedTypes     = []string{".gz", ".bz2", ".zip", ".tar", ".tar.gz", ".tar.bz2", ".tgz", ".tbz"}
	AcceptedTypes       = []string{".mbox", ".txt"}
	CombinedTypes       []string
	MONTHS              = map[string]int{"January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6, "July": 7, "August": 8, "September": 9, "October": 10, "November": 11, "December": 12}
	DefaultDateTime     = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	DefaultLastDateTime = time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	MessageSeparator    = []byte("\nFrom")
	PiperRawMapping     = []byte(`{"dynamic":true,"properties":{"metadata__updated_on":{"type":"date"},"data":{"properties":{"body":{"dynamic":false,"properties":{}}}}}}`)
	PiperRichMapping    = []byte(`{"properties":{"metadata__updated_on":{"type":"date"},"Subject_analyzed":{"type":"text","fielddata":true,"index":true},"body":{"type":"text","index":true}}}`)
)

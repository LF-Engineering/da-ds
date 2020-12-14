package pipermail

import "time"

const (
	ModMboxThreadStr            = "/thread"
	Pipermail                   = "pipermail"
	ArchiveDownloadsPath        = "/Users/code/.perceval/mailinglists/"
	Separator            string = "\nFrom"

)

var (
	CompressedTypes     = []string{".gz", ".bz2", ".zip", ".tar", ".tar.gz", ".tar.bz2", ".tgz", ".tbz"}
	AcceptedTypes       = []string{".mbox", ".txt"}
	CombinedTypes       []string
	MONTHS              = map[string]int{"January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6, "July": 7, "August": 8, "September": 9, "October": 10, "November": 11, "December": 12}
	DefaultDateTime     = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	DefaultLastDateTime = time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
)

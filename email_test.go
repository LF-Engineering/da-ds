package dads

import (
	"net/mail"
	"os"
	"testing"

	lib "github.com/LF-Engineering/da-ds"
)

func TestParseAddresses(t *testing.T) {
	var ctx lib.Ctx
	lib.FatalOnError(os.Setenv("DA_DS", "ds"))
	ctx.Init()
	ctx.Debug = 2
	sameResult := func(a1, a2 []*mail.Address) bool {
		m1 := make(map[[2]string]struct{})
		m2 := make(map[[2]string]struct{})
		for _, a := range a1 {
			m1[[2]string{a.Name, a.Address}] = struct{}{}
		}
		for _, a := range a2 {
			m2[[2]string{a.Name, a.Address}] = struct{}{}
		}
		for k := range m1 {
			_, ok := m2[k]
			if !ok {
				return false
			}
		}
		for k := range m2 {
			_, ok := m1[k]
			if !ok {
				return false
			}
		}
		return true
	}
	var testCases = []struct {
		addr           string
		expectedEmails []*mail.Address
		expectedOK     bool
	}{
		{addr: "Lukasz Gryglicki <lgryglicki@cncf.io>", expectedOK: true, expectedEmails: []*mail.Address{{Name: "Lukasz Gryglicki", Address: "lgryglicki@cncf.io"}}},
		{addr: "Lukasz Gryglicki lgryglicki@cncf.io", expectedOK: false, expectedEmails: []*mail.Address{}},
		{addr: `"Lukasz Gryglicki" <lgryglicki@cncf.io>`, expectedOK: true, expectedEmails: []*mail.Address{{Name: "Lukasz Gryglicki", Address: "lgryglicki@cncf.io"}}},
		{addr: " Lukasz  Gryglicki\t  <lgryglicki@cncf.io>", expectedOK: true, expectedEmails: []*mail.Address{{Name: "Lukasz Gryglicki", Address: "lgryglicki@cncf.io"}}},
		{addr: " Lukasz  Gryglicki\t  <lgryglicki at cncf.io>", expectedOK: true, expectedEmails: []*mail.Address{{Name: "Lukasz Gryglicki", Address: "lgryglicki@cncf.io"}}},
		{addr: "Lukasz Gryglicki <lgryglicki_at_cncf.io>", expectedOK: true, expectedEmails: []*mail.Address{{Name: "Lukasz Gryglicki", Address: "lgryglicki@cncf.io"}}},
		{addr: "Lukasz Gryglicki <lgryglicki en cncf.io>", expectedOK: true, expectedEmails: []*mail.Address{{Name: "Lukasz Gryglicki", Address: "lgryglicki@cncf.io"}}},
		{
			addr:           "Lukasz Gryglicki<lgryglicki@cncf.io>,Justyna Gryglicka<jgryglicka@cncf.io>",
			expectedOK:     true,
			expectedEmails: []*mail.Address{{Name: "Lukasz Gryglicki", Address: "lgryglicki@cncf.io"}, {Name: "Justyna Gryglicka", Address: "jgryglicka@cncf.io"}},
		},
		{
			addr:           "Lukasz Gryglicki<lgryglicki@cncf.io>\t , \tJustyna Gryglicka<jgryglicka@cncf.io>",
			expectedOK:     true,
			expectedEmails: []*mail.Address{{Name: "Lukasz Gryglicki", Address: "lgryglicki@cncf.io"}, {Name: "Justyna Gryglicka", Address: "jgryglicka@cncf.io"}},
		},
		{addr: "a<b@c>,d<e@f>", expectedOK: true, expectedEmails: []*mail.Address{{Name: "d", Address: "e@f"}, {Name: "a", Address: "b@c"}}},
		{addr: `a"b<ab@my.com>`, expectedOK: true, expectedEmails: []*mail.Address{{Name: "ab", Address: "ab@my.com"}}},
		{addr: "me@domain.com", expectedOK: true, expectedEmails: []*mail.Address{{Name: "me", Address: "me@domain.com"}}},
		{addr: `'"mia"' <'me@domain.com'>`, expectedOK: true, expectedEmails: []*mail.Address{{Name: "mia", Address: "me@domain.com"}}},
		{addr: " luke\t \t <me@domain.com>\t", expectedOK: true, expectedEmails: []*mail.Address{{Name: "luke", Address: "me@domain.com"}}},
		{addr: " luke\t \t < me@domain.com\t>\t", expectedOK: true, expectedEmails: []*mail.Address{{Name: "luke", Address: "me@domain.com"}}},
		{addr: "\t i    have\twhitespace \t < \t me@domain.com\t \t>\t \t", expectedOK: true, expectedEmails: []*mail.Address{{Name: "i have whitespace", Address: "me@domain.com"}}},
		{addr: "<me@domain.com>", expectedOK: true, expectedEmails: []*mail.Address{{Name: "me", Address: "me@domain.com"}}},
		{addr: "=?76dea4628?&<mail@domain.com>", expectedOK: true, expectedEmails: []*mail.Address{{Name: "mail", Address: "mail@domain.com"}}},
		// Jeremy Selan <jeremy...@gmail.com> - some groups cut email addrss - we cannot parse this because we cannot guess what the cut value is, example group: SF+ocio-dev
		{addr: "bsloan <bsl...@gmail.com>", expectedOK: false, expectedEmails: []*mail.Address{}},
		{addr: "a, z<b@c>, d, y<e@f>", expectedOK: true, expectedEmails: []*mail.Address{{Name: "d, y", Address: "e@f"}, {Name: "a, z", Address: "b@c"}}},
		{addr: "<me@domain.com> , <you@domain.com>", expectedOK: true, expectedEmails: []*mail.Address{{Name: "me", Address: "me@domain.com"}, {Name: "you", Address: "you@domain.com"}}},
		{addr: "me@domain.com,<you@domain.com>", expectedOK: true, expectedEmails: []*mail.Address{{Name: "me", Address: "me@domain.com"}, {Name: "you", Address: "you@domain.com"}}},
		{addr: "< me@domain.com >,  you@domain.com", expectedOK: true, expectedEmails: []*mail.Address{{Name: "me", Address: "me@domain.com"}, {Name: "you", Address: "you@domain.com"}}},
		{addr: "me@domain.com,you@domain.com", expectedOK: true, expectedEmails: []*mail.Address{{Name: "me", Address: "me@domain.com"}, {Name: "you", Address: "you@domain.com"}}},
		// we don't support such messy addresses, original code didn't support this neither
		{addr: "=?iso-8859-2?Q?Michal_=C8marada?= <michal.cmarada@pantheon.tech>", expectedOK: false, expectedEmails: []*mail.Address{}},
		{addr: "=?Windows-1252?Q?Ivan_Hra=9Ako?= <ivan.hrasko@pantheon.tech>", expectedOK: false, expectedEmails: []*mail.Address{}},
		{addr: "=?iso-8859-2?Q?Radek_Krej=E8a?= <ops-dev@lists.openswitch.net>", expectedOK: false, expectedEmails: []*mail.Address{}},
		{addr: `robert.konc@controlmatik.eu <robert.konc@controlmatik.eu>`, expectedOK: true, expectedEmails: []*mail.Address{{Name: "robert.konc", Address: "robert.konc@controlmatik.eu"}}},
		{
			addr:           ` =?windows-1257?Q?B=B8e=2C_Sebastian?= <Sebastian.Boe@nordicsemi.no>,"robert.konc@controlmatik.eu" <robert.konc@controlmatik.eu>,"devel@lists.zephyrproject.org" <devel@lists.zephyrproject.org>`,
			expectedOK:     true,
			expectedEmails: []*mail.Address{{Name: "robert.konc", Address: "robert.konc@controlmatik.eu"}, {Name: "devel", Address: "devel@lists.zephyrproject.org"}},
		},
	}
	for index, test := range testCases {
		gotEmails, gotOK := lib.ParseAddresses(&ctx, test.addr)
		if gotOK != test.expectedOK {
			t.Errorf("test number %d, expected '%s' ok %v, got %v", index+1, test.addr, test.expectedOK, gotOK)
		} else {
			if !sameResult(gotEmails, test.expectedEmails) {
				t.Errorf("test number %d, expected '%s' to parse to %+v, got %+v", index+1, test.addr, test.expectedEmails, gotEmails)
			}
		}
	}
}

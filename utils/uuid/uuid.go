package uuid

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

func trimQuotes(s string) string {
	if len(s) >= 2 {
		r := regexp.MustCompile(`(["'])`)
		s = r.ReplaceAllString(s, `\$1`)
	}
	return s
}

func ToUnicode(s string) (string, error) {
	dst := make([]byte, len(s)+100)

	t := transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
	nDst, _, err := t.Transform(dst, []byte(s), true)
	if err != nil {
		return "", err
	}
	return string(dst[:nDst]), nil
}

func Generate(args ...string) (string, error) {
	for i := range args {
		// strip spaces
		args[i] = strings.TrimSpace(args[i])

		// check empty args
		if args[i] == "" {
			return "", errors.New("args cannot be empty")
		}
	}

	data := strings.Join(args, ":")

	hash := sha1.New()
	_, err := hash.Write([]byte(data))
	if err != nil {
		return "", err
	}
	hashed := fmt.Sprintf("%x", hash.Sum(nil))

	return hashed, nil

}

/*
Get the UUID related to the identity data.

Based on the input data, the function will return the UUID associated
to an identity. On this version, the UUID will be the SHA1 of
"source:email:name:username" string. This string is case insensitive,
which means same values for the input parameters in upper
or lower case will produce the same UUID.

The value of 'name' will converted to its unaccent form which means
same values with accent or unnacent chars (i.e 'ö and o') will
generate the same UUID.

For instance, these combinations will produce the same UUID:

('scm', 'jsmith@example.com', 'John Smith', 'jsmith'),
('scm', 'jsmith@example,com', 'Jöhn Smith', 'jsmith'),
('scm', 'jsmith@example.com', 'John Smith', 'JSMITH'),
('scm', 'jsmith@example.com', 'john Smith', 'jsmith')

:param source: data source
:param email: email of the identity
:param name: full name of the identity
:param username: user name used by the identity

:returns: a universal unique identifier for Sorting Hat

:raises ValueError: when source is None or empty; each one of the
parameters is None; parameters are empty.
*/
func GenerateIdentity(source, email, name, username *string) (string, error) {

	if source == nil || *source == "" {
		return "", errors.New("source cannot be an empty string")
	}

	if (email == nil || *email == "") && (name == nil || *name == "") && (username == nil || *username == "") {
		return "", errors.New("identity data cannot be None or empty")
	}

	args := make([]string, 4)
	args[0] = *source

	if email == nil {
		args[1] = "none"
	} else {
		args[1] = *email
	}

	if name == nil {
		args[2] = "none"
	} else {
		args[2] = *name
	}

	if username == nil {
		args[3] = "none"
	} else {
		args[3] = *username
	}

	for i := range args {

		output := ""
		ss := args[i]
		for len(ss) > 0 {
			r, size := utf8.DecodeRuneInString(ss)
			if unicode.IsSymbol(r) {
				output += string(rune(ss[0]))
			} else {
				output += string(r)
			}
			ss = ss[size:]
		}
		args[i] = output

		// strip spaces
		args[i] = strings.TrimSpace(args[i])

		// remove surrogates
		output, err := strconv.Unquote(`"` + trimQuotes(args[i]) + `"`)
		if err != nil {
			return "", err
		}

		args[i] = output
	}

	data := strings.Join(args, ":")

	// to unicode
	output, err := ToUnicode(data)
	if err != nil {
		return "", err
	}
	data = strings.ToLower(output)

	fmt.Println("uuid", data)

	hash := sha1.New()
	_, err = hash.Write([]byte(data))
	if err != nil {
		return "", err
	}
	hashed := fmt.Sprintf("%x", hash.Sum(nil))

	return hashed, nil

}

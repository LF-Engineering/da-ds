package uuid

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

func trimQuotes(s string) string {
	if len(s) >= 2 {
		r := regexp.MustCompile(`(["'])`)
		s = r.ReplaceAllString(s, `\$1`)
	}
	return s
}

func Generate(args ...string) (string, error) {
	for i, arg := range args {
		// strip spaces
		args[i] = strings.TrimSpace(arg)

		//// remove surrogates
		//output, err := strconv.Unquote(`"` + trimQuotes(args[i]) + `"`)
		//if err != nil {
		//	return "", err
		//}

		//args[i] = output

		// check empty args
		if arg == "" {
			return "", errors.New("args cannot be empty")
		}
	}

	data := strings.Join(args, ":")
	fmt.Println("uuid", data)

	hash := sha1.New()
	_, err := hash.Write([]byte(data))
	if err != nil {
		return "", err
	}
	hashed := fmt.Sprintf("%x", hash.Sum(nil))

	return hashed, nil

}

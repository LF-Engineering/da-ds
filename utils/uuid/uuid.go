package uuid

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"strings"
)


func Generate(args ...string) (string, error) {
	for i, arg := range args {
		// strip spaces
		args[i] = strings.TrimSpace(arg)

		// check empty args
		if arg == "" {
			return "", errors.New("args cannot be empty")
		}
	}

	data := strings.Join(args, ":")

	hash := sha1.New()
	_, err := hash.Write([]byte(data))
	if err != nil {
		return "", err
	}
	hashed := hex.EncodeToString(hash.Sum(nil))

	return hashed, nil

}

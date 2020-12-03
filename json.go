package dads

import (
	jsoniter "github.com/json-iterator/go"
)

// PrettyPrintJSON - pretty formats raw JSON bytes
func PrettyPrintJSON(jsonBytes []byte) []byte {
	var jsonObj interface{}
	FatalOnError(jsoniter.Unmarshal(jsonBytes, &jsonObj))
	pretty, err := jsoniter.MarshalIndent(jsonObj, "", "  ")
	FatalOnError(err)
	return pretty
}

// JSONEscape - escape string for JSON to avoid injections
func JSONEscape(str string) string {
	b, _ := jsoniter.Marshal(str)
	return string(b[1 : len(b)-1])
}

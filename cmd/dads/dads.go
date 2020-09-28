package main

import (
	"time"

	lib "github.com/LF-Engineering/da-ds"
	// jsoniter "github.com/json-iterator/go"
	// yaml "gopkg.in/yaml.v2"
)

func main() {
	var ctx lib.Ctx
	dtStart := time.Now()
	ctx.Init()
	dtEnd := time.Now()
	lib.Printf("Took: %v\n", dtEnd.Sub(dtStart))
}

package utils

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestCreate(t *testing.T) {
	// Build the request body.
	var b strings.Builder
	b.WriteString(`{"title" : "`)
	b.WriteString("ayman-test-")
	b.WriteString(`"}`)

	params := &Params{URL: "http://localhost:9200",
		Username: "elastic",
		Password: "changeme"}
	srv, err := NewProvider(params)
	if err != nil {
		fmt.Println("1111")
		fmt.Println(err.Error())
		t.Fatal()
	}

	res, err := srv.Add("test", fmt.Sprintf("test-%v", time.Now().Unix()), []byte(b.String()))
	if err != nil {
		fmt.Println("2222")
		fmt.Println(err.Error())
		t.Fatal()
	}

	fmt.Println(res)

	fmt.Println("3333")
	t.SkipNow()

}

package utils

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestAdd(t *testing.T) {
	// Build the request body.
	var b strings.Builder
	b.WriteString(`{"title" : "`)
	b.WriteString("ayman-test-")
	b.WriteString(`"}`)

	params := &ESParams{URL: "http://localhost:9200",
		Username: "elastic",
		Password: "changeme"}
	srv, err := NewESClientProvider(params)
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

func TestESClientProvider_CreateIndex(t *testing.T) {
	// mapping
	mapping := []byte(`{"mappings": {"dynamic":true,"properties":{"metadata__updated_on":{"type":"date"},"data":{"properties":{"description":{"type":"text","index":true},"full_description":{"type":"text","index":true}}}}}}`)

	params := &ESParams{URL: "http://localhost:9200",
		Username: "elastic",
		Password: "changeme"}
	srv, err := NewESClientProvider(params)
	if err != nil {
		t.Error(err.Error())
	}

	res, err := srv.CreateIndex("test", mapping)
	if err != nil {
		t.Errorf("error: %v", err)
		return
	}

	t.Logf("result %s", res)

}

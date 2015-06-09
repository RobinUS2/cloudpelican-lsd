// Random data generator used for testing
// @author Robin Verlangen

package main

import (
	"bytes"
	"encoding/base64"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)

var uri string
var usr string
var pwd string

func init() {
	flag.StringVar(&uri, "uri", "", "Supervisor url")
	flag.StringVar(&usr, "usr", "cloud", "Supervisor username")
	flag.StringVar(&pwd, "pwd", "pelican", "Supervisor password")
	flag.Parse()
}

func main() {
	uri = strings.TrimSpace(uri)
	if len(uri) < 1 {
		log.Fatal("Please provide supervisor URL with --uri=http://host:1525/")
	}
	log.Printf("URI: %s\n", uri)

	for {
		msgs := make([]string, 0)
		msgs = append(msgs, fmt.Sprintf("test %s", time.Now()))
		write("e120207e-ef59-4cad-809c-5e650177759d", msgs) // @todo random filter id detected from list
		time.Sleep(100 * time.Millisecond)                  // @todo random
	}
}

func write(filterId string, data []string) bool {
	// Client
	client := &http.Client{}

	// Body
	var bodyBytes bytes.Buffer
	for _, line := range data {
		bodyBytes.WriteString(fmt.Sprintf("%s\n", line))
	}
	buf := bytes.NewReader(bodyBytes.Bytes())

	// Request
	req, err := http.NewRequest("PUT", fmt.Sprintf("%sfilter/%s/result", uri, filterId), buf)
	if err != nil {
		return false
	}

	// Auth header
	req.Header.Add("Authorization", fmt.Sprintf("Basic %s", _getBasicAuthToken()))

	// Execute
	resp, respErr := client.Do(req)
	if respErr != nil {
		return false
	}

	// Status
	if resp.StatusCode >= 400 {
		return false
	}

	// Read body
	defer resp.Body.Close()
	contents, readErr := ioutil.ReadAll(resp.Body)
	if readErr != nil {
		return false
	}
	str := string(contents)
	log.Println(str)
	return true
}

func _getBasicAuthToken() string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", usr, pwd)))
}

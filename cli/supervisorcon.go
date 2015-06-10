package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
)

type SupervisorCon struct {
}

type Filter struct {
	Regex      string `json:"regex"`
	Name       string `json:"name"`
	ClientHost string `json:"client_host"`
	Id         string `json:"id"`
}

func (s *SupervisorCon) Connect() bool {
	if verbose {
		log.Printf("Connecting to %s", session["supervisor_uri"])
	}
	_, err := s._get("filter")
	if err == nil {
		if !silent {
			fmt.Printf("Connected to %s\n", session["supervisor_uri"])
		}
	} else {
		fmt.Printf("Failed to connect: %s", err)
		return false
	}
	return true
}

func (s *SupervisorCon) Ping() {
	_, err := s._get("ping")
	if err == nil {
		fmt.Printf("Pong\n")
	}
}

func (s *SupervisorCon) CreateFilter(name string, regex string) (*Filter, error) {
	if verbose {
		log.Printf("Creating filter '%s' with regex '%s'", name, regex)
	}
	s._post(fmt.Sprintf("filter?name=%s&regex=%s", url.QueryEscape(name), url.QueryEscape(regex)))
	return s.FilterByName(name)
}

func (s *SupervisorCon) RemoveFilter(name string) bool {
	if verbose {
		log.Printf("Deleting filter '%s'", name)
	}
	filter, e := s.FilterByName(name)
	if e != nil {
		return false
	}
	s._delete(fmt.Sprintf("filter/%s", url.QueryEscape(filter.Id)))
	verify, _ := s.FilterByName(name)
	return verify == nil
}

func (s *SupervisorCon) FilterByName(name string) (*Filter, error) {
	filters, err := s.Filters()
	if err != nil {
		return nil, err
	}
	for _, filter := range filters {
		if strings.ToLower(filter.Name) == strings.ToLower(name) {
			return filter, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("Filter '%s' not found", name))
}

func (s *SupervisorCon) Filters() ([]*Filter, error) {
	data, err := s._get("filter")
	if err != nil {
		return nil, err
	}
	// Parse and create list
	var resp map[string]interface{}
	jErr := json.Unmarshal([]byte(data), &resp)
	if jErr != nil {
		return nil, jErr
	}
	list := make([]*Filter, 0)
	for _, v := range resp["filters"].([]interface{}) {
		elm := v.(map[string]interface{})
		filter := newFilter()
		filter.Regex = fmt.Sprintf("%s", elm["regex"])
		filter.Name = fmt.Sprintf("%s", elm["name"])
		filter.ClientHost = fmt.Sprintf("%s", elm["client_host"])
		filter.Id = fmt.Sprintf("%s", elm["id"])
		list = append(list, filter)
	}

	return list, nil
}

func (s *SupervisorCon) _get(uri string) (string, error) {
	return s._doRequest("GET", uri)
}

func (s *SupervisorCon) _post(uri string) (string, error) {
	return s._doRequest("POST", uri)
}

func (s *SupervisorCon) _delete(uri string) (string, error) {
	return s._doRequest("DELETE", uri)
}

func (s *SupervisorCon) _doRequest(method string, uri string) (string, error) {
	// Client
	client := s._getHttpClient()

	// Request
	req, err := http.NewRequest(method, fmt.Sprintf("%s%s", session["supervisor_uri"], uri), nil)
	if err != nil {
		return "", err
	}

	// Auth header
	req.Header.Add("Authorization", fmt.Sprintf("Basic %s", s._getBasicAuthToken()))

	// Execute
	resp, respErr := client.Do(req)
	if respErr != nil {
		return "", respErr
	}

	// Status
	if resp.StatusCode >= 400 {
		return "", errors.New(fmt.Sprintf("Status %d", resp.StatusCode))
	}

	// Read body
	defer resp.Body.Close()
	contents, readErr := ioutil.ReadAll(resp.Body)
	if readErr != nil {
		return "", readErr
	}
	str := string(contents)
	if verbose {
		log.Printf("Received body %s", str)
	}
	return str, nil
}

func (s *SupervisorCon) _getBasicAuthToken() string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", session["supervisor_username"], session["supervisor_password"])))
}

func (s *SupervisorCon) _getHttpClient() *http.Client {
	client := &http.Client{}
	return client
}

func NewSupervisorCon() *SupervisorCon {
	return &SupervisorCon{}
}

func newFilter() *Filter {
	return &Filter{}
}

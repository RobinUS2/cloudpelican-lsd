package main

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

type SupervisorCon struct {
}

func (s *SupervisorCon) Connect() {
	if verbose {
		log.Printf("Connecting to %s", session["supervisor_uri"])
	}
	_, err := s._get("filter")
	if err == nil {
		fmt.Printf("Connected to %s\n", session["supervisor_uri"])
	} else {
		fmt.Printf("Failed to connect: %s", err)
	}
}

func (s *SupervisorCon) _get(uri string) (string, error) {
	// Client
	client := s._getHttpClient()

	// Request
	req, err := http.NewRequest("GET", fmt.Sprintf("%s%s", session["supervisor_uri"], uri), nil)
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
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", session["supervisor_username"], session["supervisor_password"]))) // @todo dynamic
}

func (s *SupervisorCon) _getHttpClient() *http.Client {
	client := &http.Client{}
	return client
}

func NewSupervisorCon() *SupervisorCon {
	return &SupervisorCon{}
}

package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type SupervisorCon struct {
	filtersCache    []*Filter
	filtersCacheMux sync.RWMutex
}

type Filter struct {
	Regex      string `json:"regex"`
	Name       string `json:"name"`
	ClientHost string `json:"client_host"`
	Id         string `json:"id"`
}

// Get table name from filter
func (f *Filter) GetSearchTableName() string {
	// @todo support custom time range
	t := time.Now()
	date := t.Format("2006_01_02")
	newTable := fmt.Sprintf("cloudpelican_lsd_v1.%s_results_%s_v%d", strings.Replace(f.Id, "-", "_", -1), date, 1) // @todo configure bigquery dataset and table version
	return newTable
}

// Returns a map of metricId => timestamp => count
func (f *Filter) GetStats(window int64, rollup int64) (map[int]map[int64]int64, error) {
	// Request
	uri := fmt.Sprintf("filter/%s/stats", f.Id)
	data, err := supervisorCon._get(uri)
	if err != nil {
		return nil, err
	}

	// Parse JSON
	var d map[string]interface{}
	je := json.Unmarshal([]byte(data), &d)
	if je != nil {
		return nil, je
	}

	// Now
	var nowUnix int64 = time.Now().Unix()
	var minTs int64 = nowUnix - window

	// To map + rollup
	var res map[int]map[int64]int64 = make(map[int]map[int64]int64)
	for metricId, data := range d["stats"].(map[string]interface{}) {
		// Convert metric to integer
		i, _ := strconv.ParseInt(metricId, 10, 0)
		metric := int(i)
		if res[metric] == nil {
			res[metric] = make(map[int64]int64)
		}

		// Iterate timestamp-value pairs
		var lowestBucket int64 = math.MaxInt64
		var highestBucket int64 = math.MinInt64
		for tsStr, val := range data.(map[string]interface{}) {
			// Convert to integer
			tsI, _ := strconv.ParseInt(tsStr, 10, 64)
			ts := int64(tsI)

			// Honor window
			if ts < minTs {
				continue
			}

			// Determine bucket
			bucket := ts
			if rollup != -1 {
				bucket = ts - (ts % rollup)
			}

			// Store first bucket
			if bucket < lowestBucket {
				lowestBucket = bucket
			}
			if bucket > highestBucket {
				highestBucket = bucket
			}

			// Put in resultset
			res[metric][bucket] += int64(val.(float64))
		}

		// Fill any gaps
		// log.Printf("first key %d", lowestBucket)
		// log.Printf("last key %d", highestBucket)
		// log.Printf("step size %d", rollup)
		// log.Printf("len before %d", len(res[metric]))
		for i := lowestBucket; i < highestBucket; i += rollup {
			// Explictly set the value
			if res[metric][i] == 0 {
				res[metric][i] = 0
			}
		}
		// log.Printf("len after %d", len(res[metric]))

	}

	return res, nil
}

func (s *SupervisorCon) Search(q string) (string, error) {
	if verbose {
		log.Printf("Executing search query: %s", q)
	}
	// @todo setting to configure default search backend
	data, err := supervisorCon._postData("bigquery/query", q)
	return data, err
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

func (s *SupervisorCon) SetSupervisorConfig(k string, v string) (bool, error) {
	_, err := s._put(fmt.Sprintf("admin/config?key=%s&value=%s", url.QueryEscape(k), url.QueryEscape(v)))
	if err == nil {
		return false, err
	}
	return true, nil
}

func (s *SupervisorCon) Ping() {
	start := time.Now()
	_, err := s._get("ping")
	if err == nil {
		duration := time.Now().Sub(start)
		fmt.Printf("Pong, took %s\n", duration.String())
	}
}

func (s *SupervisorCon) CreateFilter(name string, regex string) (*Filter, error) {
	if verbose {
		log.Printf("Creating filter '%s' with regex '%s'", name, regex)
	}
	// Create
	s._post(fmt.Sprintf("filter?name=%s&regex=%s", url.QueryEscape(name), url.QueryEscape(regex)))

	// Clear cache
	s.filtersCacheMux.Lock()
	s.filtersCache = nil
	s.filtersCacheMux.Unlock()

	// Return instance
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

	// Clear cache
	s.filtersCacheMux.Lock()
	s.filtersCache = nil
	s.filtersCacheMux.Unlock()

	// Verify
	verify, _ := s.FilterByName(name)
	return verify == nil
}

func isUuid(in string) bool {
	isUuid, _ := regexp.MatchString("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", in)
	return isUuid
}

func (s *SupervisorCon) FilterByName(name string) (*Filter, error) {
	if isUuid(name) {
		return s.FilterById(name)
	}
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

func (s *SupervisorCon) FilterById(id string) (*Filter, error) {
	filters, err := s.Filters()
	if err != nil {
		return nil, err
	}
	for _, filter := range filters {
		if id == filter.Id {
			return filter, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("Filter '%s' not found", id))
}

func (s *SupervisorCon) Filters() ([]*Filter, error) {
	// Update function
	fetchData := func() ([]*Filter, error) {
		// List holder
		list := make([]*Filter, 0)

		// Fetch API
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
		for _, v := range resp["filters"].([]interface{}) {
			elm := v.(map[string]interface{})
			filter := newFilter()
			filter.Regex = fmt.Sprintf("%s", elm["regex"])
			filter.Name = fmt.Sprintf("%s", elm["name"])
			filter.ClientHost = fmt.Sprintf("%s", elm["client_host"])
			filter.Id = fmt.Sprintf("%s", elm["id"])

			// Tmp?
			if strings.HasPrefix(filter.Name, TMP_FILTER_PREFIX) {
				// Remove if they are older than x hours
				tsStr := filter.Name[len(TMP_FILTER_PREFIX):]
				tsVal, tsE := strconv.ParseInt(tsStr, 10, 64)
				if tsE != nil {
					tsVal = 0
				}
				tsNow := time.Now().Unix()
				maxHours := int64(1)
				tsMin := tsNow - (maxHours * 3600)
				if tsVal < tsMin {
					if verbose {
						log.Println(fmt.Sprintf("Removing stale filter %s", filter.Id))
					}
					go func(id string) {
						s._delete(fmt.Sprintf("filter/%s", url.QueryEscape(id)))
					}(filter.Id)
				}

				continue
			}

			// Append
			list = append(list, filter)
		}

		// Put in cache
		if len(list) > 0 {
			s.filtersCacheMux.Lock()
			s.filtersCache = list
			s.filtersCacheMux.Unlock()
		}

		// Return
		return list, nil
	}

	// From cache?
	s.filtersCacheMux.RLock()
	if s.filtersCache != nil {
		s.filtersCacheMux.RUnlock()
		go fetchData() // Async update
		return s.filtersCache, nil
	}
	s.filtersCacheMux.RUnlock()

	// Not from cache
	return fetchData()
}

func (s *SupervisorCon) _get(uri string) (string, error) {
	return s._doRequest("GET", uri, "")
}

func (s *SupervisorCon) _put(uri string) (string, error) {
	return s._doRequest("PUT", uri, "")
}

func (s *SupervisorCon) _post(uri string) (string, error) {
	return s._doRequest("POST", uri, "")
}

func (s *SupervisorCon) _putData(uri string, data string) (string, error) {
	return s._doRequest("PUT", uri, data)
}

func (s *SupervisorCon) _postData(uri string, data string) (string, error) {
	return s._doRequest("POST", uri, data)
}

func (s *SupervisorCon) _delete(uri string) (string, error) {
	return s._doRequest("DELETE", uri, "")
}

func (s *SupervisorCon) _doRequest(method string, uri string, data string) (string, error) {
	// Client
	client := s._getHttpClient()

	// Request
	var reqBody *bytes.Buffer
	if len(data) > 0 {
		reqBody = bytes.NewBuffer([]byte(data))
	} else {
		reqBody = bytes.NewBuffer(make([]byte, 0))
	}
	req, err := http.NewRequest(method, fmt.Sprintf("%s%s", session["supervisor_uri"], uri), reqBody)
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

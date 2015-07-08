// The supervisor acts as the glue between the cli and the storm topology
// - The storm topology communicates with the supervisor in order to determine settings, etc.
// - The cli communicates with the supervisor to modify settings, get results, etc.
// @author Robin Verlangen

package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/RobinUS2/golang-jresp"
	"github.com/julienschmidt/httprouter"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

var serverPort int
var slackServerPort int
var basicAuthUsr string
var basicAuthPwd string
var adminPwd string
var dbFile string
var filterManager *FilterManager
var maxMsgMemory int
var maxMsgBatch int
var verbose bool
var confPath string
var conf *Conf
var numCores int

func init() {
	flag.IntVar(&serverPort, "port", 1525, "Server port")
	flag.IntVar(&slackServerPort, "slack-port", 1526, "Slack server port")
	flag.StringVar(&basicAuthUsr, "auth-user", "cloud", "Username")
	flag.StringVar(&basicAuthPwd, "auth-password", "pelican", "Password")
	flag.StringVar(&adminPwd, "admin-password", "", "Password for admin operations (optional)")
	flag.StringVar(&dbFile, "db-file", "cloudpelican_lsd_supervisor.db", "Database file")
	flag.IntVar(&maxMsgMemory, "max-msg-memory", 10000, "Maximum amount of messages kept in memory")
	flag.IntVar(&maxMsgBatch, "max-msg-batch", 10000, "Maximum amount of messages sent in a single batch")
	flag.IntVar(&numCores, "cpu-cores", -1, "Amount of cores we can use (-1 = all available)")
	flag.StringVar(&confPath, "conf", "", "Path to additional configuration parameter file")
	flag.BoolVar(&verbose, "v", false, "Verbose, debug mode")
	flag.Parse()
}

func main() {
	// Set max procs
	if numCores == -1 {
		numCores = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(numCores)
	log.Printf("Starting with %d CPU cores", numCores)

	// Config
	conf = newConf(confPath)

	// Filter manager
	filterManager = NewFilterManager()

	// Routing
	router := httprouter.New()

	// Docs
	router.GET("/", GetHome)

	// Ping
	router.GET("/ping", GetPing)

	// Filters
	router.POST("/filter", PostFilter)                             // Create new filter
	router.GET("/filter/:id/result", GetFilterResult)              // Get results of a single filter
	router.GET("/filter/:id/stats", GetFilterStats)                // Get stats of a single filter
	router.PUT("/filter/:id/result", PutFilterResult)              // Store new results into a filter
	router.POST("/filter/:id/outlier", PostFilterOutlier)          // Create new record of a detected outlier
	router.PUT("/stats/filters", PutStatsFilters)                  // Store new statistics around filters
	router.GET("/filter", GetFilter)                               // Get all filters
	router.DELETE("/filter/:id", DeleteFilter)                     // Delete a filter
	router.DELETE("/admin/truncate/outliers", DeleteAdminOutliers) // Delete outliers
	router.PUT("/admin/config", PutAdminConfig)                    // Set configuration value
	router.POST("/bigquery/query", PostBigQueryExecute)            // Execute a query on bigquery, NOT JSON, response is TSV

	// Slack handler: see https://api.slack.com/slash-commands
	go func() {
		slackRouter := httprouter.New()
		slackRouter.POST("/slack", PostSlack)
		log.Println(fmt.Sprintf("Starting Slack service at port %d", slackServerPort))
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", slackServerPort), slackRouter))
	}()

	// Start webserver
	log.Println(fmt.Sprintf("Starting supervisor service at port %d", serverPort))
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", serverPort), router))
}

// Slack handler
func PostSlack(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// Validate
	token := r.PostFormValue("token")
	expectedToken := conf.GetNotEmpty("slack_token")
	if verbose {
		log.Printf("Slack token received %s", token)
	}
	if token != expectedToken {
		log.Println("Invalid Slack token")
		return
	}

	// Collect input
	input := r.PostFormValue("text")
	if verbose {
		log.Printf("Slack input: %s", input)
	}

	// Args
	args := make([]string, 0)
	if verbose {
		args = append(args, "-v")
	} else {
		args = append(args, "--silent=true")
	}
	args = append(args, "-e")
	args = append(args, input)

	// Format in blocks
	fmt.Fprint(w, "```")

	// Assemble JSON
	cmd := exec.Command("cloudpelican", args...)
	//cmd.Stderr = os.Stdout // Redirect std error
	stdout, _ := cmd.StdoutPipe()
	err := cmd.Start()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Waiting for command Slack to finish...")
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		fmt.Fprintln(w, scanner.Text())
	}
	stdout.Close()
	err = cmd.Wait()
	if err != nil {
		log.Printf("Command Slack finished with error: %v", err)
	} else {
		log.Printf("Command Slack finished")
	}

	// End block
	fmt.Fprint(w, "```")
	fmt.Fprint(w, "") // Trailing white space to finish request
}

// This is not a JSON response
func PostBigQueryExecute(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	if !basicAuth(w, r) {
		return
	}

	// Details
	bodyBytes, bodyErr := ioutil.ReadAll(r.Body)
	var bodyStr string = ""
	if bodyErr == nil {
		bodyStr = string(bodyBytes)
	}
	log.Printf("BigQuery: %s", bodyStr)

	// Find bigquery conf & put in map
	jsonData := make(map[string]string)
	backends := strings.Split(conf.GetNotEmpty("search_backends"), ",")
	for _, backendId := range backends {
		backendType := conf.Get(fmt.Sprintf("search_backends.%s.type", backendId))
		if backendType != "bigquery" {
			log.Println(fmt.Sprintf("Unsupported search backend %s", backendType))
			continue
		}
		jsonData["project_id"] = conf.GetNotEmpty(fmt.Sprintf("search_backends.%s.project_id", backendId))
		jsonData["dataset_id"] = conf.GetNotEmpty(fmt.Sprintf("search_backends.%s.dataset_id", backendId))
		jsonData["service_account_id"] = conf.GetNotEmpty(fmt.Sprintf("search_backends.%s.service_account_id", backendId))
		jsonData["pk12base64"] = conf.GetNotEmpty(fmt.Sprintf("search_backends.%s.pk12base64", backendId))

		if verbose {
			log.Println(backendId, backendType, jsonData["project_id"], jsonData["dataset_id"], jsonData["service_account_id"], jsonData["pk12base64"])
		}
	}

	// Query
	jsonData["query"] = bodyStr

	// Marshal JSON
	jsonBytes, _ := json.Marshal(jsonData)
	if verbose {
		log.Printf("%s", jsonBytes)
	}

	// Build base64 args
	base64Args := base64.StdEncoding.EncodeToString(jsonBytes)

	// Args
	args := make([]string, 0)
	args = append(args, "-jar")
	args = append(args, "bigquery-client/target/bigquery-client-0.1-jar-with-dependencies.jar")
	args = append(args, base64Args)

	// Assemble JSON
	cmd := exec.Command("java", args...)
	if verbose {
		cmd.Stderr = os.Stdout // Map the stdErr of the process to stdout as this is debug data
	}
	stdout, _ := cmd.StdoutPipe()
	err := cmd.Start()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Waiting for command BigQuery to finish...")
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		fmt.Fprintln(w, scanner.Text())
	}
	stdout.Close()
	err = cmd.Wait()
	if err != nil {
		log.Printf("Command BigQuery finished with error: %v", err)
	} else {
		log.Printf("Command BigQuery finished")
	}
	fmt.Fprint(w, "") // Trailing white space to finish request
}

func PutAdminConfig(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	if !basicAuth(w, r) {
		return
	}
	if !adminAuth(w, r) {
		return
	}
	jresp := jresp.NewJsonResp()
	conf.Set(r.URL.Query().Get("key"), r.URL.Query().Get("value"))
	res := conf.Save()
	jresp.Set("saved", res)
	jresp.OK()
	fmt.Fprint(w, jresp.ToString(false))
}

func DeleteAdminOutliers(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	if !basicAuth(w, r) {
		return
	}
	if !adminAuth(w, r) {
		return
	}
	jresp := jresp.NewJsonResp()
	res := filterManager.TruncateOutliers()
	jresp.Set("truncated", res)
	jresp.OK()
	fmt.Fprint(w, jresp.ToString(false))
}

func GetPing(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	if !basicAuth(w, r) {
		return
	}
	jresp := jresp.NewJsonResp()
	jresp.Set("pong", true)
	jresp.OK()
	fmt.Fprint(w, jresp.ToString(false))
}

func GetHome(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	if !basicAuth(w, r) {
		return
	}
	jresp := jresp.NewJsonResp()
	jresp.Set("hello", "This is the CloudPelican supervisor")
	jresp.OK()
	fmt.Fprint(w, jresp.ToString(false))
}

func PostFilter(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	if !basicAuth(w, r) {
		return
	}
	jresp := jresp.NewJsonResp()

	// Validate
	regex := strings.TrimSpace(r.URL.Query().Get("regex"))
	if len(regex) < 1 {
		jresp.Error("Please provide a regex")
		fmt.Fprint(w, jresp.ToString(false))
		return
	}
	name := strings.TrimSpace(r.URL.Query().Get("name"))
	if len(name) < 1 {
		jresp.Error("Please provide a name")
		fmt.Fprint(w, jresp.ToString(false))
		return
	}

	// Create filter
	id, err := filterManager.CreateFilter(name, r.RemoteAddr, regex)
	if err != nil {
		jresp.Error(fmt.Sprintf("Failed to create filter: %s", err))
		fmt.Fprint(w, jresp.ToString(false))
		return
	}

	// OK :)
	jresp.Set("filter_id", id)
	jresp.OK()
	fmt.Fprint(w, jresp.ToString(false))
}

func GetFilterResult(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	if !basicAuth(w, r) {
		return
	}
	jresp := jresp.NewJsonResp()
	id := strings.TrimSpace(ps.ByName("id"))
	if len(id) < 1 {
		jresp.Error("Please provide an ID")
		fmt.Fprint(w, jresp.ToString(false))
		return
	}
	filter := filterManager.GetFilter(id)
	if filter == nil {
		jresp.Error(fmt.Sprintf("Filter %s not found", id))
		fmt.Fprint(w, jresp.ToString(false))
		return
	}
	var clearResults bool = false
	filter.resultsMux.RLock()
	clearResults = len(filter.Results()) > 0
	jresp.Set("results", filter.Results())
	filter.resultsMux.RUnlock()
	jresp.OK()
	fmt.Fprint(w, jresp.ToString(false))

	// Clear results
	if clearResults {
		filter.resultsMux.Lock()
		filterManager.filterResults[filter.Id] = make([]string, 0)
		filter.resultsMux.Unlock()
	}
}

func GetFilterStats(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	if !basicAuth(w, r) {
		return
	}
	jresp := jresp.NewJsonResp()
	id := strings.TrimSpace(ps.ByName("id"))
	if len(id) < 1 {
		jresp.Error("Please provide an ID")
		fmt.Fprint(w, jresp.ToString(false))
		return
	}
	filter := filterManager.GetFilter(id)
	if filter == nil {
		jresp.Error(fmt.Sprintf("Filter %s not found", id))
		fmt.Fprint(w, jresp.ToString(false))
		return
	}
	stats := filter.GetStats()
	m := make(map[string]map[string]int64) // metricid => timebucket => value
	for metricId, metric := range stats.Metrics {
		ms := fmt.Sprintf("%d", metricId)
		m[ms] = make(map[string]int64)
		for ts, val := range metric.Data {
			m[ms][fmt.Sprintf("%d", ts)] = val
		}
	}
	jresp.Set("stats", m)
	jresp.OK()
	fmt.Fprint(w, jresp.ToString(false))
}

func PostFilterOutlier(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	if !basicAuth(w, r) {
		return
	}
	jresp := jresp.NewJsonResp()

	// Get filter
	id := strings.TrimSpace(ps.ByName("id"))
	if len(id) < 1 {
		jresp.Error("Please provide an ID")
		fmt.Fprint(w, jresp.ToString(false))
		return
	}
	filter := filterManager.GetFilter(id)
	if filter == nil {
		jresp.Error(fmt.Sprintf("Filter %s not found", id))
		fmt.Fprint(w, jresp.ToString(false))
		return
	}

	// Timestamp
	timestamp := strings.TrimSpace(r.URL.Query().Get("timestamp"))
	if len(timestamp) < 1 {
		jresp.Error("Please provide a timestamp")
		fmt.Fprint(w, jresp.ToString(false))
		return
	}
	ts, tsErr := strconv.ParseInt(timestamp, 10, 0)
	if tsErr != nil {
		jresp.Error(fmt.Sprintf("Please provide a valid timestamp: %s", tsErr))
		fmt.Fprint(w, jresp.ToString(false))
		return
	}

	// Score
	score := strings.TrimSpace(r.URL.Query().Get("score"))
	if len(score) < 1 {
		jresp.Error("Please provide a score")
		fmt.Fprint(w, jresp.ToString(false))
		return
	}
	scoreVal, scoreErr := strconv.ParseFloat(score, 64)
	if scoreErr != nil {
		jresp.Error(fmt.Sprintf("Please provide a valid score: %s", scoreErr))
		fmt.Fprint(w, jresp.ToString(false))
		return
	}

	// Details
	bodyBytes, bodyErr := ioutil.ReadAll(r.Body)
	var details string = ""
	if bodyErr == nil {
		details = string(bodyBytes)
	}

	// Create outlier
	log.Printf("Filter %s outlier at ts %d score %f details %s", filter.Id, ts, scoreVal, details)
	res := filter.AddOutlier(ts, scoreVal, details)
	jresp.Set("ack", res)

	// Done
	jresp.OK()
	fmt.Fprint(w, jresp.ToString(false))
}

func PutFilterResult(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	if !basicAuth(w, r) {
		return
	}
	jresp := jresp.NewJsonResp()

	// Get filter
	id := strings.TrimSpace(ps.ByName("id"))
	if len(id) < 1 {
		jresp.Error("Please provide an ID")
		fmt.Fprint(w, jresp.ToString(false))
		return
	}
	filter := filterManager.GetFilter(id)
	if filter == nil {
		jresp.Error(fmt.Sprintf("Filter %s not found", id))
		fmt.Fprint(w, jresp.ToString(false))
		return
	}

	// Read body
	scanner := bufio.NewScanner(r.Body)
	scanner.Split(bufio.ScanLines)
	var lines []string = make([]string, 0)
	var count int = 0
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		count++
		if count >= maxMsgBatch {
			log.Println("ERROR: Aborting message batch, reached limit %d", maxMsgBatch)
			break
		}
	}

	// Add results
	res := filter.AddResults(lines)
	jresp.Set("ack", res)
	jresp.Set("lines", len(lines))
	jresp.OK()
	fmt.Fprint(w, jresp.ToString(false))
}

func GetFilter(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	if !basicAuth(w, r) {
		return
	}
	jresp := jresp.NewJsonResp()
	filters := filterManager.GetFilters()
	jresp.Set("filters", filters)
	jresp.OK()
	fmt.Fprint(w, jresp.ToString(false))
}

func PutStatsFilters(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	if !basicAuth(w, r) {
		return
	}
	jresp := jresp.NewJsonResp()

	// Read body
	bodyBytes, bodyErr := ioutil.ReadAll(r.Body)
	if bodyErr != nil {
		jresp.Error("Invalid request body")
		fmt.Fprint(w, jresp.ToString(false))
		return
	}

	// JSON decode
	var data map[string]int64
	jsonErr := json.Unmarshal(bodyBytes, &data)
	if jsonErr != nil {
		jresp.Error(fmt.Sprintf("Invalid request JSON: %s", jsonErr))
		fmt.Fprint(w, jresp.ToString(false))
		return
	}

	// Store results
	var filterId string
	var metric int
	var timeBucket int64
	var updates int // Amount of acknowledged updates
	for k, count := range data {
		// Reset vars
		filterId = ""
		metric = 0
		timeBucket = 0

		// Split
		pairs := strings.Split(k, "_")
		for _, pair := range pairs {
			kv := strings.SplitN(pair, "=", 2)
			if len(kv) != 2 {
				log.Println("Invalid KV pair in PutStatsFilters")
				continue
			}
			if kv[0] == "f" {
				// Filter ID
				filterId = kv[1]
			} else if kv[0] == "m" {
				// Metric
				i, e := strconv.ParseInt(kv[1], 10, 0)
				if e != nil {
					log.Println("Invalid integer %s in PutStatsFilters", kv[1])
					continue
				}
				metric = int(i)
			} else if kv[0] == "b" {
				// Time bucket
				i, e := strconv.ParseInt(kv[1], 10, 64)
				if e != nil {
					log.Println("Invalid integer %s in PutStatsFilters", kv[1])
					continue
				}
				timeBucket = int64(i)
			}
		}

		// Filter?
		if len(filterId) == 0 {
			log.Println("Empty filter in PutStatsFilters")
			continue
		}

		// Load filter
		filter := filterManager.GetFilter(filterId)
		if filter == nil {
			log.Println("Filter %s not found in PutStatsFilters", filterId)
			continue
		}

		// Set results
		res := filter.AddStats(metric, timeBucket, count)
		if res {
			updates++
		}
	}

	// OK
	jresp.Set("updates", updates)
	jresp.OK()
	fmt.Fprint(w, jresp.ToString(false))
}

func DeleteFilter(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	if !basicAuth(w, r) {
		return
	}
	jresp := jresp.NewJsonResp()
	id := strings.TrimSpace(ps.ByName("id"))
	if len(id) < 1 {
		jresp.Error("Please provide an ID")
		fmt.Fprint(w, jresp.ToString(false))
		return
	}
	res := filterManager.DeleteFilter(id)
	jresp.Set("deleted", res)
	jresp.OK()
	fmt.Fprint(w, jresp.ToString(false))
}

func adminAuth(w http.ResponseWriter, r *http.Request) bool {
	if len(adminPwd) < 1 {
		return true // No password set
	}
	if r.URL.Query().Get("admin_password") != adminPwd {
		return false
	}
	return true
}

func basicAuth(w http.ResponseWriter, r *http.Request) bool {
	if r.Header["Authorization"] == nil || len(r.Header["Authorization"]) < 1 {
		log.Printf("%s", r.Header)
		http.Error(w, "bad syntax a", http.StatusBadRequest)
		return false
	}
	auth := strings.SplitN(r.Header["Authorization"][0], " ", 2)

	if len(auth) != 2 || auth[0] != "Basic" {
		log.Printf("%s", r.Header)
		http.Error(w, "bad syntax b", http.StatusBadRequest)
		return false
	}

	payload, _ := base64.StdEncoding.DecodeString(auth[1])
	pair := strings.SplitN(string(payload), ":", 2)

	if len(pair) != 2 || !validateAuth(pair[0], pair[1]) {
		http.Error(w, "authorization failed", http.StatusUnauthorized)
		return false
	}
	return true
}

func validateAuth(username, password string) bool {
	if username == basicAuthUsr && password == basicAuthPwd {
		return true
	}
	return false
}

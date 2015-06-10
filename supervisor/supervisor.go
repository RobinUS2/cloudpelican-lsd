// The supervisor acts as the glue between the cli and the storm topology
// - The storm topology communicates with the supervisor in order to determine settings, etc.
// - The cli communicates with the supervisor to modify settings, get results, etc.
// @author Robin Verlangen

package main

import (
	"bufio"
	"encoding/base64"
	"flag"
	"fmt"
	"github.com/RobinUS2/golang-jresp"
	"github.com/julienschmidt/httprouter"
	"log"
	"net/http"
	"strings"
)

var serverPort int
var basicAuthUsr string
var basicAuthPwd string
var dbFile string
var filterManager *FilterManager
var maxMsgMemory int
var maxMsgBatch int
var verbose bool

func init() {
	flag.IntVar(&serverPort, "port", 1525, "Server port")
	flag.StringVar(&basicAuthUsr, "auth-user", "cloud", "Username")
	flag.StringVar(&basicAuthPwd, "auth-password", "pelican", "Password")
	flag.StringVar(&dbFile, "db-file", "cloudpelican_lsd_supervisor.db", "Database file")
	flag.IntVar(&maxMsgMemory, "max-msg-memory", 10000, "Maximum amount of messages kept in memory")
	flag.IntVar(&maxMsgBatch, "max-msg-batch", 10000, "Maximum amount of messages sent in a single batch")
	flag.BoolVar(&verbose, "v", false, "Verbose, debug mode")
	flag.Parse()
}

func main() {
	// Filter manager
	filterManager = NewFilterManager()

	// Routing
	router := httprouter.New()

	// Docs
	router.GET("/", GetHome)

	// Ping
	router.GET("/ping", GetPing)

	// Filters
	router.POST("/filter", PostFilter)                // Create new filter
	router.GET("/filter/:id/result", GetFilterResult) // Get results of a single filter
	router.PUT("/filter/:id/result", PutFilterResult) // Store new results into a filter
	router.GET("/filter", GetFilter)                  // Get all filters
	router.DELETE("/filter/:id", DeleteFilter)        // Delete a filter

	// Start webserver
	log.Println(fmt.Sprintf("Starting supervisor service at port %d", serverPort))
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", serverPort), router))
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

func PutFilterResult(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
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

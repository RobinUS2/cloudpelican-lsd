// The supervisor acts as the glue between the cli and the storm topology
// - The storm topology communicates with the supervisor in order to determine settings, etc.
// - The cli communicates with the supervisor to modify settings, get results, etc.
// @author Robin Verlangen

package main

import (
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

func init() {
	flag.IntVar(&serverPort, "port", 1525, "Server port")
	flag.StringVar(&basicAuthUsr, "auth-user", "cloud", "Username")
	flag.StringVar(&basicAuthPwd, "auth-password", "pelican", "Password")
	flag.StringVar(&dbFile, "db-file", "cloudpelican_lsd_supervisor.db", "Database file")
	flag.Parse()
}

func main() {
	// Filter manager
	filterManager = NewFilterManager()

	// Routing
	router := httprouter.New()

	// Docs
	router.GET("/", GetHome)

	// Filters
	router.POST("/filter", PostFilter)
	router.GET("/filter/:id/result", GetFilterResult)
	router.PUT("/filter/:id/result", PutFilterResult)
	router.GET("/filter", GetFilter)
	router.DELETE("/filter", DeleteFilter)

	// Start webserver
	log.Println(fmt.Sprintf("Starting supervisor service at port %d", serverPort))
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", serverPort), router))
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

func GetFilterResult(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	if !basicAuth(w, r) {
		return
	}
	jresp := jresp.NewJsonResp()
	// @todo
	fmt.Fprint(w, jresp.ToString(false))
}

func PutFilterResult(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	if !basicAuth(w, r) {
		return
	}
	jresp := jresp.NewJsonResp()
	// @todo
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

func DeleteFilter(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	if !basicAuth(w, r) {
		return
	}
	jresp := jresp.NewJsonResp()
	// @todo
	fmt.Fprint(w, jresp.ToString(false))
}

func basicAuth(w http.ResponseWriter, r *http.Request) bool {
	if r.Header["Authorization"] == nil || len(r.Header["Authorization"]) < 1 {
		http.Error(w, "bad syntax", http.StatusBadRequest)
		return false
	}
	auth := strings.SplitN(r.Header["Authorization"][0], " ", 2)

	if len(auth) != 2 || auth[0] != "Basic" {
		http.Error(w, "bad syntax", http.StatusBadRequest)
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

// The supervisor acts as the glue between the cli and the storm topology
// - The storm topology communicates with the supervisor in order to determine settings, etc.
// - The cli communicates with the supervisor to modify settings, get results, etc.
// @author Robin Verlangen

package main

import (
	"flag"
	"fmt"
	"github.com/RobinUS2/golang-jresp"
	"github.com/julienschmidt/httprouter"
	"log"
	"net/http"
)

var serverPort int

func init() {
	flag.IntVar(&serverPort, "port", 1525, "Server port")
	flag.Parse()
}

func main() {
	// Routing
	router := httprouter.New()

	// Docs
	router.GET("/", GetHome)

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

func basicAuth(w http.ResponseWriter, r *http.Request) {
	auth := strings.SplitN(r.Header["Authorization"][0], " ", 2)

	if len(auth) != 2 || auth[0] != "Basic" {
		http.Error(w, "bad syntax", http.StatusBadRequest)
		return false
	}

	payload, _ := base64.StdEncoding.DecodeString(auth[1])
	pair := strings.SplitN(string(payload), ":", 2)

	if len(pair) != 2 || !Validate(pair[0], pair[1]) {
		http.Error(w, "authorization failed", http.StatusUnauthorized)
		return false
	}
	return true
}

// Command line interface
// @author Robin Verlangen

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"time"
)

var customConfPath string
var verbose bool

const CONSOLE_PREFIX string = "cloudpelican"
const CONSOLE_SEP string = "> "

var CONSOLE_KEYWORDS map[string]bool = make(map[string]bool)
var CONSOLE_KEYWORDS_OPTS map[string]int = make(map[string]int)

var session map[string]string = make(map[string]string)
var supervisorCon *SupervisorCon
var conf *Conf
var consecutiveInterruptCount int
var interrupted bool
var startupCommands string

func init() {
	flag.StringVar(&customConfPath, "c", "", "Path to configuration file (default in your home folder)")
	flag.StringVar(&startupCommands, "e", "", "Commands to execute, seperated by semi-colon")
	flag.BoolVar(&verbose, "v", false, "Verbose, debug mode")
	flag.Parse()
}

func main() {
	if verbose {
		log.Println("Starting CloudPelican Log Stream Dump (LSD)")
	}

	// Load config
	loadConf()

	// Startup commands
	if len(startupCommands) > 0 {
		startupCommandsList := strings.Split(startupCommands, ";")
		for _, startupCommand := range startupCommandsList {
			handleConsole(startupCommand)
		}
	}

	// Listen for user input
	startConsole()
}

func startConsole() {
	// Keyword defs
	CONSOLE_KEYWORDS["help"] = true
	CONSOLE_KEYWORDS["quit"] = true
	CONSOLE_KEYWORDS["exit"] = true
	CONSOLE_KEYWORDS["clear"] = true
	CONSOLE_KEYWORDS["save"] = true
	CONSOLE_KEYWORDS["ping"] = true
	CONSOLE_KEYWORDS["clearsession"] = true
	CONSOLE_KEYWORDS["clearhistory"] = true
	CONSOLE_KEYWORDS["history"] = true
	CONSOLE_KEYWORDS["show filters"] = true

	CONSOLE_KEYWORDS_OPTS["connect"] = 2 // connect + uri
	CONSOLE_KEYWORDS_OPTS["auth"] = 3    // auth + usr + pwd

	// Handle other signals
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for _ = range c {
			// sig is a ^C, handle it
			interrupted = true
			consecutiveInterruptCount++
			if consecutiveInterruptCount >= 2 {
				fmt.Printf("Exiting\n")
				os.Exit(0)
			}
		}
	}()

	// Console reader
	reader := bufio.NewReader(os.Stdin)
	var buffer bytes.Buffer
	printConsoleWait()
	for {
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error: ", err)
		} else {
			input = strings.TrimSpace(input)
			buffer.WriteString(input)
			bufStr := buffer.String()
			lowerStr := strings.ToLower(bufStr)
			splitLower := strings.Split(lowerStr, " ")

			// Semi colon?
			if strings.Contains(bufStr, ";") || CONSOLE_KEYWORDS[lowerStr] || CONSOLE_KEYWORDS_OPTS[splitLower[0]] == len(splitLower) {
				// Flush buffer
				handleConsole(bufStr)
				printConsoleWait()
				buffer.Reset()
			} else {
				// Whitespace after buffer
				buffer.WriteString(" ")
				printConsoleInputPad()
			}
		}
	}
}
func handleConsole(input string) {
	input = strings.TrimRight(input, " ;")
	if len(input) < 1 {
		return
	}
	consoleAddHistory(input)
	inputLower := strings.ToLower(input)
	if inputLower == "help" {
		printConsoleHelp()
	} else if inputLower == "quit" || inputLower == "exit" {
		os.Exit(0)
	} else if inputLower == "clear" {
		c := exec.Command("clear")
		c.Stdout = os.Stdout
		c.Run()
	} else if inputLower == "save" {
		save()
	} else if inputLower == "ping" {
		ping()
	} else if inputLower == "clearsession" {
		clearSession()
	} else if inputLower == "clearhistory" {
		clearhistory()
	} else if inputLower == "history" {
		printHistory()
	} else if inputLower == "show filters" {
		showFilters()
	} else if strings.Index(inputLower, "select ") == 0 {
		executeSelect(inputLower)
	} else if strings.Index(inputLower, "history ") == 0 {
		split := strings.SplitN(input, "history ", 2)
		if len(split) != 2 {
			printConsoleError(input)
			return
		}
		dispatchHistory(split[1])
	} else if strings.Index(inputLower, "connect ") == 0 {
		split := strings.SplitN(input, "connect ", 2)
		if len(split) != 2 {
			printConsoleError(input)
			return
		}
		connect(split[1])
	} else if strings.Index(inputLower, "auth ") == 0 {
		split := strings.Split(input, " ")
		if len(split) != 3 {
			printConsoleError(input)
			return
		}
		auth(split[1], split[2])
	} else {
		printConsoleError(input)
	}

}

func showFilters() {
	filters, err := supervisorCon.Filters()
	if err != nil {
		printConsoleError(fmt.Sprintf("%s", err))
		return
	}
	fmt.Printf("FILTER NAME\n")
	for _, filter := range filters {
		fmt.Printf("%s\n", filter.Name)
	}
}

func executeSelect(input string) {
	var filterName string = ""
	var limitStr string = ""
	tokens := strings.Split(input, " ")
	for i, token := range tokens {
		var previousToken string = ""
		if i != 0 {
			previousToken = tokens[i-1]
		}

		// Very simple parsing
		if previousToken == "from" {
			// Filter name
			filterName = token
		} else if previousToken == "limit" {
			limitStr = token
		}
	}

	// Limit
	var limit int64 = -1
	var limitErr error
	if len(limitStr) > 0 {
		limit, limitErr = strconv.ParseInt(limitStr, 10, 0)
		if limitErr != nil {
			printConsoleError(fmt.Sprintf("Invalid LIMIT %s", limitErr))
			return
		}
	}

	// Filter name
	if len(filterName) < 1 {
		printConsoleError(fmt.Sprintf("Filter '%s' not found", filterName))
		return
	}

	// Load filter
	filter, filterE := supervisorCon.FilterByName(filterName)
	if filterE != nil {
		printConsoleError(fmt.Sprintf("%s", filterE))
		return
	}

	// Stream data
	uri := fmt.Sprintf("filter/%s/result", filter.Id)
	var resultCount int64 = 0
outer:
	for {
		// Handle interrup
		if interrupted {
			interrupted = false
			consecutiveInterruptCount = 0
			fmt.Printf("Interrupted..\n")
			break
		}

		time.Sleep(200 * time.Millisecond) // @todo dynamic
		data, respErr := supervisorCon._get(uri)
		if respErr != nil {
			if verbose {
				fmt.Printf("Error while fetching results: %s", respErr)
			}
			continue
		}
		var res map[string]interface{}
		jE := json.Unmarshal([]byte(data), &res)
		if jE != nil {
			if verbose {
				fmt.Printf("Error while fetching results: %s", jE)
			}
			continue
		}
		list := res["results"].([]interface{})
		for _, elm := range list {
			fmt.Printf("%s\n", elm)
			resultCount++
			if limit != -1 && resultCount >= limit {
				break outer
			}
		}
	}
}

func dispatchHistory(id string) {
	i, err := strconv.ParseInt(id, 10, 0)
	if err != nil {
		printConsoleError(fmt.Sprintf("%s", err))
		return
	}
	if len(conf.CmdHistory[i]) < 1 {
		fmt.Printf("History #%d not found\n", i)
	}
	handleConsole(conf.CmdHistory[i])
}

func clearhistory() {
	conf.CmdHistory = make([]string, 0)
	conf.Save()
	fmt.Printf("Cleared history\n")
}

func printHistory() {
	for i, cmd := range conf.CmdHistory {
		fmt.Printf("%d %s\n", i, cmd)
	}
}

func consoleAddHistory(cmd string) {
	conf.CmdHistory = append(conf.CmdHistory, cmd)

	// Purge data
	max := 100
	histLen := len(conf.CmdHistory)
	if histLen > max {
		if verbose {
			log.Println("Cleaning console history")
		}
		conf.CmdHistory = conf.CmdHistory[1:]
	}
	conf.Save()
}

func clearSession() {
	session["supervisor_uri"] = ""
	session["supervisor_username"] = ""
	session["supervisor_password"] = ""
	fmt.Printf("Cleared session\n")
	conf.PersistentSession = session
	conf.Save()

}

func ping() {
	if !ensureConnected() {
		return
	}
	supervisorCon.Ping()
}

func ensureConnected() bool {
	if supervisorCon == nil {
		fmt.Printf("Not connected\n")
		return false
	}
	return true
}

func save() {
	conf.PersistentSession = session
	conf.Save()
	fmt.Printf("Saved session\n")
}

func auth(usr string, pwd string) {
	session["supervisor_username"] = usr
	session["supervisor_password"] = pwd
	fmt.Printf("Received authentication tokens\n")
}

func connect(uri string) {
	session["supervisor_uri"] = uri
	_connect()
}

func _connect() {
	supervisorCon = NewSupervisorCon()
	supervisorCon.Connect()
}

func printConsoleError(input string) {
	fmt.Printf("Unknown input '%s' type 'help' for explanation\n", input)
}

func printConsoleHelp() {
	fmt.Printf("\n")
	fmt.Printf("CMD\t\t\t\tDESCRIPTION\n")
	fmt.Printf("auth <usr> <pwd>\t\tSet authentication details\n")
	fmt.Printf("connect <host>\t\t\tConnect to supervisor on host\n")
	fmt.Printf("show filters\t\t\tDisplay list of filters configured\n")
	fmt.Printf("select\t\t\t\tExecute SQL-like queries, example: select * from <filter_name>\n")
	fmt.Printf("clear\t\t\t\tClears console\n")
	fmt.Printf("save\t\t\t\tSave session\n")
	fmt.Printf("history\t\t\t\tPrint recent command history\n")
	fmt.Printf("history <id>\t\t\tExecute command from history by id\n")
	fmt.Printf("clearsession\t\t\tClears session (connectino, settings, etc)\n")
	fmt.Printf("clearhistory\t\t\tClears history of commands\n")
	fmt.Printf("ping\t\t\t\tTest connection with supervisor\n")
	fmt.Printf("help\t\t\t\tPrints this documentation\n")
	fmt.Printf("quit\t\t\t\tExit the CloudPelican cli\n")
	fmt.Printf("\n")
}

func printConsoleWait() {
	fmt.Printf("%s%s", CONSOLE_PREFIX, CONSOLE_SEP)
}

func printConsoleInputPad() {
	fmt.Printf("%s%s", strings.Repeat(" ", len(CONSOLE_PREFIX)), CONSOLE_SEP)
}

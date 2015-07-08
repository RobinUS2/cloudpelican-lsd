// Command line interface
// @author Robin Verlangen

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	terminal "github.com/carmark/pseudo-terminal-go/terminal"
)

var customConfPath string
var verbose bool

const CONSOLE_PREFIX string = "cloudpelican"
const CONSOLE_SEP string = "> "
const TMP_FILTER_PREFIX string = "__tmp__"

var CONSOLE_KEYWORDS map[string]bool = make(map[string]bool)
var CONSOLE_KEYWORDS_OPTS map[string]int = make(map[string]int)

var session map[string]string = make(map[string]string)
var supervisorCon *SupervisorCon
var conf *Conf
var consecutiveInterruptCount int
var interrupted bool
var startupCommands string
var silent bool
var allowAutoCreateFilter bool
var stats *Statistics
var term *terminal.Terminal
var oldState *terminal.State
var cmdFinishChan chan bool
var multiLineInput bool = false
var terminalRaw bool

func init() {
	flag.StringVar(&customConfPath, "c", "", "Path to configuration file (default in your home folder)")
	flag.StringVar(&startupCommands, "e", "", "Commands to execute, seperated by semi-colon")
	flag.BoolVar(&verbose, "v", false, "Verbose, debug mode")
	flag.BoolVar(&terminalRaw, "raw-terminal", true, "Raw terminal mode")
	flag.BoolVar(&silent, "silent", true, "Silent, no helping output mode")
	flag.BoolVar(&allowAutoCreateFilter, "allow-temporary-filters", true, "Automatically create temporary filters from select statements")
	flag.Parse()
}

func main() {
	if verbose {
		log.Println("Starting CloudPelican Log Stream Dump (LSD)")
	}

	// Load config
	loadConf()
	if verbose {
		log.Println("Loaded conf")
	}

	// Stats
	stats = newStatistics()
	if verbose {
		log.Println("Init stats")
	}

	// Wait channel
	cmdFinishChan = make(chan bool, 1)

	// Startup commands
	if len(startupCommands) > 0 {
		terminalRaw = false // Disable terminal raw mode
		wait := handleConsole(startupCommands)
		if wait {
			<-cmdFinishChan
		}
		restoreTerminalAndExit(term, oldState)
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

	CONSOLE_KEYWORDS_OPTS["connect"] = 2              // connect + uri
	CONSOLE_KEYWORDS_OPTS["tail"] = 2                 // tail + filter name
	CONSOLE_KEYWORDS_OPTS["auth"] = 3                 // auth + usr + pwd
	CONSOLE_KEYWORDS_OPTS["stats"] = 2                // stats + filter name
	CONSOLE_KEYWORDS_OPTS["describe filter"] = 3      // describe filter + filter name
	CONSOLE_KEYWORDS_OPTS["configure supervisor"] = 3 // configure supervisor + k=v

	// Console reader
	if terminalRaw {
		var termErr error = nil
		term, termErr = terminal.NewWithStdInOut()
		if termErr != nil {
			log.Printf("WARN! Failed to init terminal. Error: %s", termErr)
		} else {
			var rawErr error = nil
			oldState, rawErr = terminal.MakeRaw(0)
			if rawErr != nil {
				log.Printf("WARN! Failed to set terminal in raw mode, some functions might not be supported. Error: %s", rawErr)
			} else {
				// Finish terminal start
				defer restoreTerminalAndExit(term, oldState)
				term.SetPrompt("")

				// Auto complete handler
				term.AutoCompleteCallback = processAutocomplete

				// Console wait
				fmt.Printf("%s", getConsoleWait())
			}
		}
	}

	// Main loop
	var lineBuffer bytes.Buffer
	for {
		line, err := term.ReadLine()
		if err == io.EOF {
			term.Write([]byte(line))
			fmt.Println()
			restoreTerminalAndExit(term, oldState)
		}
		if err != nil && strings.Contains(err.Error(), "control-c break") {
			handleInterrupt()
		} else {
			if err != nil {
				fmt.Println("Error: ", err)
			} else {
				lineBuffer.WriteString(fmt.Sprintf("%s ", line))
				input := lineBuffer.String()
				input = strings.TrimSpace(input)
				lowerStr := strings.ToLower(input)
				splitLower := strings.Split(lowerStr, " ")

				// Semi colon?
				if !multiLineInput || strings.Contains(input, ";") || CONSOLE_KEYWORDS[lowerStr] || CONSOLE_KEYWORDS_OPTS[splitLower[0]] == len(splitLower) {
					// Flush buffer
					lineBuffer.Reset()
					handleConsole(input)
					fmt.Printf("%s", getConsoleWait())
				} else {
					printConsoleInputPad()
				}
			}
		}
	}
}

func handleInterrupt() {
	// sig is a ^C, handle it
	interrupted = true
	consecutiveInterruptCount++
	if consecutiveInterruptCount >= 2 {
		fmt.Printf("Exiting\n")
		restoreTerminalAndExit(term, oldState)
	}
}

func _handleConsole(input string) bool {
	input = strings.TrimSpace(input)
	consoleAddHistory(input)
	inputLower := strings.ToLower(input)
	if inputLower == "help" {
		printConsoleHelp()
	} else if inputLower == "quit" || inputLower == "exit" {
		restoreTerminalAndExit(term, oldState)
	} else if inputLower == "clear" {
		clearConsole()
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
	} else if strings.Index(inputLower, "cat ") == 0 {
		executeGrepSQL(inputLower)
	} else if strings.Index(inputLower, "create filter ") == 0 {
		createFilter(inputLower)
	} else if strings.Index(inputLower, "drop filter ") == 0 {
		split := strings.SplitN(input, "drop filter ", 2)
		if len(split) != 2 {
			printConsoleError(input)
			return false
		}
		dropFilter(split[1])
	} else if strings.Index(inputLower, "history ") == 0 {
		split := strings.SplitN(input, "history ", 2)
		if len(split) != 2 {
			printConsoleError(input)
			return false
		}
		dispatchHistory(split[1])
	} else if strings.Index(inputLower, "connect ") == 0 {
		split := strings.SplitN(input, "connect ", 2)
		if len(split) != 2 {
			printConsoleError(input)
			return false
		}
		connect(split[1])
	} else if strings.Index(inputLower, "tail ") == 0 {
		split := strings.SplitN(input, "tail ", 2)
		if len(split) != 2 {
			printConsoleError(input)
			return false
		}
		executeSelect(fmt.Sprintf("select * from %s", split[1]))
		return true
	} else if strings.Index(inputLower, "search ") == 0 {
		split := strings.SplitN(input, "search ", 2)
		if len(split) != 2 {
			printConsoleError(input)
			return false
		}
		search(split[1])
		return true
	} else if strings.Index(inputLower, "stats ") == 0 {
		split := strings.SplitN(input, "stats ", 2)
		if len(split) < 2 {
			printConsoleError(input)
			return false
		}
		getStats(split[1])
		return false
	} else if strings.Index(inputLower, "describe filter ") == 0 {
		split := strings.SplitN(input, "describe filter ", 2)
		if len(split) != 2 {
			printConsoleError(input)
			return false
		}
		describeFilter(split[1])
		return false
	} else if strings.Index(inputLower, "configure supervisor ") == 0 {
		split := strings.SplitN(input, "configure supervisor ", 2)
		if len(split) != 2 {
			printConsoleError(input)
			return false
		}
		configureSupervisor(split[1])
		return false
	} else if strings.Index(inputLower, "auth ") == 0 {
		split := strings.Split(input, " ")
		if len(split) != 3 {
			printConsoleError(input)
			return false
		}
		auth(split[1], split[2])
	} else {
		printConsoleError(input)
	}
	return false
}

func executeGrepSQL(in string) {
	gsql := newGrepSQL(in)
	q, e := gsql.Parse()
	if e != nil {
		printConsoleError(fmt.Sprintf("%s", e))
		return
	}

	// Execute
	data, err := supervisorCon.Search(q)
	if err != nil {
		printConsoleError(fmt.Sprintf("Search failed '%s'", err))
		return
	}
	fmt.Print(data)
}

func search(q string) {
	// Smart query builder
	q = strings.ToLower(q)
	fromRegex := regexp.MustCompile("(?i)from([ ]+)[a-z0-9_]+")
	fromMatch := fromRegex.FindString(q)
	if len(fromMatch) > 0 {
		fromSplit := strings.SplitN(fromMatch, " ", 2)
		if len(fromSplit) >= 2 {
			filter, _ := supervisorCon.FilterByName(fromSplit[1])
			if filter != nil {
				newTable := filter.GetSearchTableName()
				q = strings.Replace(q, fromMatch, fmt.Sprintf("from %s", newTable), 1)
			}
		}
	}

	// Select * for now to fixed columns
	q = strings.Replace(q, "select *", "select _raw", 1)

	// Execute
	data, err := supervisorCon.Search(q)
	if err != nil {
		printConsoleError(fmt.Sprintf("Search failed '%s'", err))
		return
	}
	fmt.Print(data)
}

func configureSupervisor(kv string) {
	split := strings.SplitN(kv, "=", 2)
	if len(split) != 2 {
		printConsoleError(fmt.Sprintf("You must provide a key=value pair, provided '%s'", kv))
		return
	}
	_, err := supervisorCon.SetSupervisorConfig(split[0], split[1])
	if err != nil {
		printConsoleError(fmt.Sprintf("Failed to configure: %s", err))
	}
}

// Select execution, example input: "create filter <filter_name> as '<regex_here>' [with options {"key": "value"}]" [] indicates optional
func createFilter(input string) {
	// Basic parsing
	var filterName string = ""
	var regex string = ""
	tokens := strings.Split(input, " ")
	for i, token := range tokens {
		var previousToken string = ""
		if i != 0 {
			previousToken = tokens[i-1]
		}

		// Very simple parsing
		if previousToken == "filter" && tokens[i-2] == "create" {
			// Filter name
			filterName = strings.TrimSpace(token)
		} else if previousToken == "as" {
			regex = strings.TrimRight(strings.TrimLeft(token, "'"), "'")
		}
	}

	// Filter name
	if len(filterName) < 1 {
		printConsoleError(fmt.Sprintf("You must provide a filter name", filterName))
		return
	}
	if isUuid(filterName) {
		printConsoleError(fmt.Sprintf("Filter name can not be in form of a UUID", filterName))
		return
	}

	// Validate filter name
	matched, _ := regexp.MatchString("^([a-z0-9_]+)$", filterName)
	if !matched {
		printConsoleError("Filter name can only contain a-z, 0-9 and _")
		return
	}

	// Check duplicate name filter
	filterdup, _ := supervisorCon.FilterByName(filterName)
	if filterdup != nil {
		printConsoleError(fmt.Sprintf("There already exist a filter with the name %s", filterName))
		return
	}

	// Create
	_, filterErr := supervisorCon.CreateFilter(filterName, regex)
	if filterErr != nil {
		printConsoleError("Failed to create filter")
		return
	}
	fmt.Printf("Created filter '%s'\n", filterName)
}

// Drop filter (removing it)
func dropFilter(name string) {
	res := supervisorCon.RemoveFilter(name)
	if res {
		fmt.Printf("Removed filter '%s'\n", name)
	}
}

func handleConsole(input string) bool {
	input = strings.TrimRight(input, " ;\n\t")
	if len(input) < 1 {
		return false
	}
	cmds := strings.Split(input, ";")
	var wait bool
	for _, cmd := range cmds {
		subWait := _handleConsole(cmd)
		if subWait {
			wait = true
		}
	}
	return wait
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

// Select execution, example input: "select * from <filter_name> [limit 1234]" [] indicates optional
// example input from stream: "select * from stream:<stream_name> [limit 1234]" [] indicates optional
func executeSelect(input string) {
	// Basic parsing
	var filterName string = ""
	var where string = ".*"
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
			// Limit
			limitStr = token
		} else if previousToken == "where" {
			// WHERE statement
			where = strings.TrimRight(strings.TrimLeft(token, "'"), "'")
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
	var tmpFilterName string = ""
	filter, filterE := supervisorCon.FilterByName(filterName)
	if filterE != nil {
		if allowAutoCreateFilter == false {
			printConsoleError(fmt.Sprintf("%s", filterE))
			return
		} else {
			// Auto create filter from stream
			split := strings.Split(filterName, "stream:")
			if len(split) != 2 || split[1] != "default" {
				printConsoleError("Can not create temporary filter from stream, try 'select * from stream:default'")
				return
			}
			//streamName := split[1]

			// Create filter
			tmpFilterName = fmt.Sprintf("%s%d", TMP_FILTER_PREFIX, time.Now().Unix())
			supervisorCon.CreateFilter(tmpFilterName, where)
			filter, _ = supervisorCon.FilterByName(tmpFilterName)
		}
	}

	// Clear channel
	if len(cmdFinishChan) > 0 {
		<-cmdFinishChan
	}

	// Stream data
	uri := fmt.Sprintf("filter/%s/result", filter.Id)
	var resultCount int64 = 0
	go func() {
		fmt.Println()
	outer:
		for {
			// Handle interrup
			if interrupted {
				interrupted = false
				consecutiveInterruptCount = 0
				fmt.Printf("Interrupted..\n")
				if len(tmpFilterName) > 0 {
					supervisorCon.RemoveFilter(tmpFilterName)
				}
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
		fmt.Printf(getConsoleWait())
		cmdFinishChan <- true
	}()
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
		fmt.Printf("Not connected, use 'auth' and 'connect' (details see 'help')\n")
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
	if !silent {
		fmt.Printf("Received authentication tokens\n")
	}
}

func intFromTimeStr(input string, def int64) (int64, error) {
	var val int64 = def
	var err error
	if len(input) > 0 {
		// m=minute, h=hour, d=day suffixes
		var multiplier int64 = 1
		if strings.HasSuffix(input, "m") {
			multiplier = 60
			input = strings.TrimRight(input, "m")
		} else if strings.HasSuffix(input, "h") {
			multiplier = 3600
			input = strings.TrimRight(input, "h")
		} else if strings.HasSuffix(input, "d") {
			multiplier = 86400
			input = strings.TrimRight(input, "d")
		}
		val, err = strconv.ParseInt(input, 10, 0)
		if err != nil {
			printConsoleError(fmt.Sprintf("Invalid %s", err))
			return def, err
		}
		val *= multiplier
	}
	return val, nil
}

func describeFilter(filterName string) {
	// Get filter
	filter, filterE := supervisorCon.FilterByName(filterName)
	if filterE != nil {
		printConsoleError("Filter not found")
		return
	}
	fmt.Printf("NAME:\n%s\n\n", filter.Name)
	fmt.Printf("ID:\n%s\n\n", filter.Id)
	fmt.Printf("REGEX:\n%s\n\n", filter.Regex)
}

func getStats(input string) {
	// Basic parsing
	var filterName string = ""
	var windowStr string = ""
	var rollupStr string = ""
	var flags map[string]bool = make(map[string]bool)
	tokens := strings.Split(input, " ")
	for i, token := range tokens {
		var previousToken string = ""
		if i != 0 {
			previousToken = tokens[i-1]
		}

		// Very simple parsing
		if i == 0 {
			// Filter name
			filterName = token
		} else if previousToken == "window" {
			// Window (e.g. 10m)
			windowStr = token
		} else if previousToken == "rollup" {
			// Rollup (e.g. minutely, hourly)
			rollupStr = token
		}

		// Flags
		if token == "-regular" {
			flags["hide_regular"] = true
		} else if token == "-error" || token == "-errors" {
			flags["hide_error"] = true
		}
	}

	// Window
	window, _ := intFromTimeStr(windowStr, 86400)

	// Rollup
	rollup, _ := intFromTimeStr(rollupStr, 60)

	// Get filter
	filter, filterE := supervisorCon.FilterByName(filterName)
	if filterE != nil {
		printConsoleError("Filter not found")
		return
	}

	// Load
	data, statsE := filter.GetStats(window, rollup)
	if statsE != nil {
		printConsoleError(fmt.Sprintf("%s", statsE))
		return
	}
	if verbose {
		log.Printf("Stats %v", data)
	}

	// Get console width
	stats.loadTerminalDimensions()

	// Clear console
	clearConsole()

	// Render chart
	chart, chartE := stats.RenderChart(filter, data, flags)
	if chartE != nil {
		printConsoleError(fmt.Sprintf("%s", chartE))
		return
	}

	// Print chart
	fmt.Printf("\n")
	fmt.Printf("%s", chart)
}

func connect(uri string) {
	session["supervisor_uri"] = uri
	_connect(true)
}

func clearConsole() {
	c := exec.Command("clear")
	c.Stdout = os.Stdout
	c.Run()
}

func _connect(interactive bool) {
	supervisorCon = NewSupervisorCon()
	if supervisorCon.Connect() && interactive {
		fmt.Printf("Use 'save' to store authentication and connection details for future usage\n")
	}
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
	fmt.Printf("select\t\t\t\tExecute SQL-like queries, example: select * from <filter_name>;\n")
	fmt.Printf("tail <filter>\t\t\tTail stream of messages for a specific filter name\n")
	fmt.Printf("stats <filter>\t\t\tShow matching rate for a specific filter name\n")
	fmt.Printf("create filter\t\t\tCreate a new filter, example: create filter <filter_name> as '<regex>';\n")
	fmt.Printf("drop filter\t\t\tRemove a filter, example: drop filter <filter_name>;\n")
	fmt.Printf("clear\t\t\t\tClears console\n")
	fmt.Printf("save\t\t\t\tSave session\n")
	fmt.Printf("history\t\t\t\tPrint recent command history\n")
	fmt.Printf("history <id>\t\t\tExecute command from history by id\n")
	fmt.Printf("clearsession\t\t\tClears session (connectino, settings, etc)\n")
	fmt.Printf("clearhistory\t\t\tClears history of commands\n")
	fmt.Printf("configure supervisor <k>=<v>\tSet a configuration value in the supervisor\n")
	fmt.Printf("ping\t\t\t\tTest connection with supervisor\n")
	fmt.Printf("help\t\t\t\tPrints this documentation\n")
	fmt.Printf("quit\t\t\t\tExit the CloudPelican cli\n")
	fmt.Printf("\n")
}

func getConsoleWait() string {
	return fmt.Sprintf("%s%s", CONSOLE_PREFIX, CONSOLE_SEP)
}

func printConsoleInputPad() {
	fmt.Printf("%s%s", strings.Repeat(" ", len(CONSOLE_PREFIX)), CONSOLE_SEP)
}

func restoreTerminalAndExit(term *terminal.Terminal, oldState *terminal.State) {
	if oldState != nil {
		terminal.Restore(0, oldState)
	}
	if term != nil {
		term.ReleaseFromStdInOut()
	}
	os.Exit(0)
}

func processAutocomplete(line []byte, pos, key int) (newLine []byte, newPos int) {
	// Handle cursor interactions (ctrl+a = begin, ctrl+e = end)
	if key == 1 {
		// Begin of line
		return line, 0
	} else if key == 5 {
		// Begin of line
		return line, len(line)
	}

	// Only for tabs
	if key != 9 {
		return nil, pos
	}

	// String
	lineStr := strings.ToLower(strings.TrimSpace(string(line)))

	// Options placeholder
	opts := make([]string, 0)

	// Look for console keywords
	for k, _ := range CONSOLE_KEYWORDS {
		if strings.Index(k, lineStr) == 0 {
			opts = append(opts, k)
		}
	}

	// Look for console methods with options
	for k, _ := range CONSOLE_KEYWORDS_OPTS {
		if strings.Index(k, lineStr) == 0 {
			opts = append(opts, k)
		}
	}

	// Autocomplete filter names
	if strings.Index(lineStr, "stats") == 0 || strings.Index(lineStr, "tail") == 0 || strings.Index(lineStr, "describe filter") == 0 || strings.Index(lineStr, "cat") == 0 {
		split := strings.SplitN(lineStr, " ", 2)
		if len(split) == 2 {
			filters, _ := supervisorCon.Filters()
			if filters != nil {
				for _, filter := range filters {
					if strings.Index(filter.Name, split[1]) == 0 {
						opts = append(opts, fmt.Sprintf("%s %s", split[0], filter.Name))
					}
				}
			}
		}
	}

	// Only suggest if we have one option left
	if len(opts) == 1 && opts[0] != strings.TrimSpace(string(line)) {
		var opt = fmt.Sprintf("%s ", opts[0])
		return []byte(opt), pos + len(opt) - len(lineStr)
	}
	return nil, pos
}

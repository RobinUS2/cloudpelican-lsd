// Command line interface
// @author Robin Verlangen

package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
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

func init() {
	flag.StringVar(&customConfPath, "c", "", "Path to configuration file (default in your home folder)")
	flag.BoolVar(&verbose, "v", false, "Verbose, debug mode")
	flag.Parse()
}

func main() {
	if verbose {
		log.Println("Starting CloudPelican Log Stream Dump (LSD)")
	}

	// Load config
	loadConf()

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

	CONSOLE_KEYWORDS_OPTS["connect"] = 2 // connect + uri
	CONSOLE_KEYWORDS_OPTS["auth"] = 3    // auth + usr + pwd

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
	fmt.Printf("Unknown input '%s'\n", input)
}

func printConsoleHelp() {
	fmt.Printf("\n")
	fmt.Printf("CMD\t\t\t\tDESCRIPTION\n")
	fmt.Printf("auth <usr> <pwd>\t\tSet authentication details\n")
	fmt.Printf("connect <host>\t\t\tConnect to supervisor on host\n")
	fmt.Printf("clear\t\t\t\tClears console\n")
	fmt.Printf("save\t\t\t\tSave session\n")
	fmt.Printf("clearsession\t\t\tClears session\n")
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

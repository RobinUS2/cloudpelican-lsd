// Command line interface
// @author Robin Verlangen

package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"strings"
)

var customConfPath string
var verbose bool

const CONSOLE_PREFIX string = "cloudpelican"
const CONSOLE_SEP string = "> "

var CONSOLE_KEYWORDS map[string]bool = make(map[string]bool)

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

			// Semi colon?
			if strings.Contains(bufStr, ";") || CONSOLE_KEYWORDS[lowerStr] {
				// Flush buffer
				//fmt.Printf("%s\n", bufStr)
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
	} else if inputLower == "quit" {
		os.Exit(0)
	}
}

func printConsoleHelp() {
	fmt.Printf("\n")
	fmt.Printf("CMD\tDESCRIPTION\n")
	fmt.Printf("quit\tExit the CloudPelican cli\n")
	fmt.Printf("help\tPrints this documentation\n")
	fmt.Printf("\n")
}

func printConsoleWait() {
	fmt.Printf("%s%s", CONSOLE_PREFIX, CONSOLE_SEP)
}

func printConsoleInputPad() {
	fmt.Printf("%s%s", strings.Repeat(" ", len(CONSOLE_PREFIX)), CONSOLE_SEP)
}

func loadConf() (bool, error) {
	var confPath string
	if len(customConfPath) < 1 {
		usr, err := user.Current()
		if err != nil {
			log.Fatal(err)
			return false, nil
		}
		confPath = fmt.Sprintf("%s/.cloudpelican_lsd.conf", usr.HomeDir)
	} else {
		confPath = customConfPath
	}
	if verbose {
		log.Printf("Reading config from %s", confPath)
	}

	// Rad
	conf, confErr := ioutil.ReadFile(confPath)
	if confErr != nil {
		// Create file
		if verbose {
			log.Printf("Creating new empty config file")
		}
		ioutil.WriteFile(confPath, make([]byte, 0), 0600)
		conf, _ = ioutil.ReadFile(confPath)
	}

	// Print debug
	if verbose {
		log.Printf("Conf: %s", string(conf))
	}
	return true, nil
}

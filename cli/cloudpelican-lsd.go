// Command line interface
// @author Robin Verlangen

package main

import (
	"bufio"
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

func init() {
	flag.StringVar(&customConfPath, "c", "", "Path to configuration file (default in your home folder)")
	flag.BoolVar(&verbose, "v", false, "Verbose, debug mode")
	flag.Parse()
}

func main() {
	if verbose {
		log.Println("Starting CloudPelican Log Stream Dump (LSD)")
	}

	loadConf()

	// Listen for user input
	startConsole()
}

func startConsole() {
	reader := bufio.NewReader(os.Stdin)
	printConsoleWait()
	for {
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error: ", err)
		} else {
			fmt.Printf("%s\n", strings.TrimSpace(input))
			printConsoleWait()
		}
	}
}

func printConsoleWait() {
	fmt.Printf("cloudpelican> ")
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

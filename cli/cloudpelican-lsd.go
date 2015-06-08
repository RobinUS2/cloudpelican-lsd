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
)

var customConfPath string

func init() {
	flag.StringVar(&customConfPath, "c", "", "Path to configuration file (default in your home folder)")
	flag.Parse()
}

func main() {
	log.Println("CloudPelican Log Stream Dump (LSD)")

	loadConf()

	// Listen for user input
	reader := bufio.NewReader(os.Stdin)
	for {
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error: ", err)
		} else {
			log.Println(input)
		}
	}
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

	// Rad
	conf, confErr := ioutil.ReadFile(confPath)
	if confErr != nil {
		// Create file
		ioutil.WriteFile(confPath, make([]byte, 0), 0600)
	}

	// Print debug
	log.Println(string(conf))
	return true, nil
}

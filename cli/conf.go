// Config object
// @author Robin Verlangen

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os/user"
)

type Conf struct {
	Path              string            `json:"path"`
	PersistentSession map[string]string `json:"persistent_session"`
	CmdHistory        []string          `json:"cmd_history"`
}

func (c *Conf) Save() {
	b, je := json.Marshal(c)
	if je != nil {
		log.Fatal(fmt.Sprintf("Failed to save config %s", je))
	}
	ioutil.WriteFile(c.Path, b, 0600)
}

func (c *Conf) Load(str string) {
	if err := json.Unmarshal([]byte(str), c); err != nil {
		log.Fatal(fmt.Sprintf("Failed to load config %s", err))
	}
}

func NewConf() *Conf {
	return &Conf{
		PersistentSession: make(map[string]string),
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
	if verbose {
		log.Printf("Reading config from %s", confPath)
	}

	// Read
	confData, confErr := ioutil.ReadFile(confPath)
	if confErr != nil {
		// Create file
		if verbose {
			log.Printf("Creating new empty config file")
		}
		ioutil.WriteFile(confPath, make([]byte, 0), 0600)
		confData, _ = ioutil.ReadFile(confPath)
	}

	// Print debug
	confStr := string(confData)
	if verbose {
		log.Printf("Conf: %s", confStr)
	}

	// Init object
	conf = NewConf()
	conf.Load(confStr)
	conf.Path = confPath

	// Into new session?
	if conf.PersistentSession != nil {
		session = conf.PersistentSession
		if len(session["supervisor_uri"]) > 0 {
			fmt.Printf("Restored session to %s\n", session["supervisor_uri"])
			_connect()
		}
	}

	return true, nil
}

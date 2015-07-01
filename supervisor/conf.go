package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
)

const DEFAULT_CONF_PATH string = "/etc/cloudpelican_supervisor.conf"

type Conf struct {
	data    map[string]string
	dataMux sync.RWMutex
}

func (c *Conf) Get(k string) string {
	// Locking in base function
	return c.GetOrDefault(k, "")
}

func (c *Conf) Set(k string, v string) {
	c.dataMux.Lock()
	defer c.dataMux.Unlock()
	c.data[k] = v
	log.Printf("Set conf %s=%s", k, v)
}

func (c *Conf) Save() bool {
	c.dataMux.RLock()
	defer c.dataMux.RUnlock()
	b, je := json.Marshal(c.data)
	if je != nil {
		log.Printf("Failed saving conf: %s", je)
		return false
	}
	if len(confPath) < 1 {
		confPath = DEFAULT_CONF_PATH
	}
	log.Printf("Writing conf to %s", confPath)
	we := ioutil.WriteFile(confPath, b, 0600)
	if we != nil {
		log.Printf("Failed saving conf: %s", we)
		return false
	}
	return true
}

func (c *Conf) GetNotEmpty(k string) string {
	// Locking in base function
	val := c.GetOrDefault(k, "")
	if len(val) < 1 {
		panic(fmt.Sprintf("Value %s empty", k))
	}
	return val
}

func (c *Conf) GetOrDefault(k string, d string) string {
	c.dataMux.RLock()
	defer c.dataMux.RUnlock()
	if len(c.data[k]) == 0 {
		return d
	}
	return c.data[k]
}

func newConf(path string) *Conf {
	c := &Conf{}

	// Default conf read?
	if len(path) < 1 {
		if _, err := os.Stat(DEFAULT_CONF_PATH); err == nil {
			path = DEFAULT_CONF_PATH
			confPath = path
			log.Printf("Default conf from %s", DEFAULT_CONF_PATH)
		}
	}

	if len(path) > 0 {
		// Load file
		b, e := ioutil.ReadFile(path)
		if e != nil {
			log.Fatal(fmt.Sprintf("Failed to load conf: %s", e))
		}

		// Parse JSON
		var data map[string]string
		je := json.Unmarshal(b, &data)
		if je != nil {
			log.Fatal(fmt.Sprintf("Failed to parse conf: %s", je))
		}
		c.data = data
	} else {
		c.data = make(map[string]string)
	}
	return c
}

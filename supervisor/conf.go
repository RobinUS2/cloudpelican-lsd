package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
)

type Conf struct {
	data map[string]string
}

func (c *Conf) Get(k string) string {
	return c.GetOrDefault(k, "")
}

func (c *Conf) GetNotEmpty(k string) string {
	val := c.GetOrDefault(k, "")
	if len(val) < 1 {
		panic(fmt.Sprintf("Value %s empty", k))
	}
	return val
}

func (c *Conf) GetOrDefault(k string, d string) string {
	if len(c.data[k]) == 0 {
		return d
	}
	return c.data[k]
}

func newConf(path string) *Conf {
	c := &Conf{}
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
	}
	return c
}

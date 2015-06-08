// Filter manager
// @author Robin Verlangen

package main

import (
	"code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"log"
	"sync"
)

type FilterManager struct {
	db            *bolt.DB
	filterTable   string
	filterResults map[string][]string
}

type Filter struct {
	Regex      string `json:"regex"`
	Name       string `json:"name"`
	ClientHost string `json:"client_host"`
	Id         string `json:"id"`
	//Results    []string `json:"results"`
	resultsMux sync.RWMutex
}

func (f *Filter) Results() []string {
	if filterManager.filterResults[f.Id] == nil {
		filterManager.filterResults[f.Id] = make([]string, 0)
	}
	return filterManager.filterResults[f.Id]
}

func (f *Filter) ToJson() (string, error) {
	bytes, err := json.Marshal(f)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func (f *Filter) Save() bool {
	var res bool = false
	json, jsonEr := f.ToJson()
	if jsonEr != nil {
		log.Printf("Json error %s", jsonEr)
		return false
	}
	var wg sync.WaitGroup
	wg.Add(1)
	var err error = nil
	filterManager.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(filterManager.filterTable))
		err = b.Put([]byte(f.Id), []byte(json))
		if err == nil {
			log.Printf("Saved filter %s", f.Id)
			res = true
		}
		wg.Done()
		return err
	})
	wg.Wait()
	return res
}

func (f *Filter) AddResults(res []string) bool {
	f.resultsMux.Lock()

	// Init variable
	if filterManager.filterResults[f.Id] == nil {
		filterManager.filterResults[f.Id] = make([]string, 0)
	}

	// Exceed limit?
	newCount := len(res)
	currentCount := len(filterManager.filterResults[f.Id])
	newPlusCurrent := newCount + currentCount
	if newPlusCurrent > maxMsgMemory {
		log.Println("Truncating memory for filter %s, exceeding limit of %d messages", f.Id, maxMsgMemory)
		tmp := make([]string, 0)
		tooMany := maxMsgMemory - newPlusCurrent
		for i := tooMany; i < currentCount; i++ {
			tmp = append(tmp, filterManager.filterResults[f.Id][i])
		}
		filterManager.filterResults[f.Id] = tmp
	}

	// Add lines
	for _, line := range res {
		filterManager.filterResults[f.Id] = append(filterManager.filterResults[f.Id], line)
	}
	f.resultsMux.Unlock()
	return true
}

func NewFilterManager() *FilterManager {
	fm := &FilterManager{
		filterTable:   "filters",
		filterResults: make(map[string][]string),
	}
	fm.Open()
	return fm
}

func (fm *FilterManager) Open() {
	// Open DB
	db, err := bolt.Open(dbFile, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	fm.db = db

	// Create buckets
	var wg sync.WaitGroup
	wg.Add(1)
	fm.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(fm.filterTable))
		if err != nil {
			log.Fatal(fmt.Errorf("create bucket: %s", err))
		}
		wg.Done()
		return nil
	})

	// Wait until buckets are ready
	wg.Wait()
}

func (fm *FilterManager) GetFilters() []*Filter {
	var wg sync.WaitGroup
	var list []*Filter = make([]*Filter, 0)
	wg.Add(1)
	fm.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(fm.filterTable))
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			//fmt.Printf("key=%s, value=%s\n", k, v)
			elm := filterFromJson(v)
			if elm != nil {
				list = append(list, elm)
			}
		}
		wg.Done()
		return nil
	})
	wg.Wait()
	return list
}

func (fm *FilterManager) GetFilter(id string) *Filter {
	var wg sync.WaitGroup
	var elm *Filter = nil
	wg.Add(1)
	fm.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(fm.filterTable))
		res := b.Get([]byte(id))
		elm = filterFromJson(res)
		wg.Done()
		return nil
	})
	wg.Wait()
	return elm
}

func (fm *FilterManager) DeleteFilter(id string) bool {
	var val bool = false
	var wg sync.WaitGroup
	wg.Add(1)
	var err error = nil
	fm.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(fm.filterTable))
		err = b.Delete([]byte(id))
		if err == nil {
			val = true
		}
		wg.Done()
		return err
	})
	wg.Wait()
	return val
}

func (fm *FilterManager) CreateFilter(name string, clientHost string, regex string) (string, error) {
	var id string = uuid.New()
	var filter *Filter = newFilter()
	filter.Regex = regex
	filter.Name = name
	filter.ClientHost = clientHost
	filter.Id = id

	// To JSON
	json, jsonErr := filter.ToJson()
	if jsonErr != nil {
		log.Fatal(fmt.Sprintf("Failed JSON: %s", jsonErr))
	}

	// Create
	var wg sync.WaitGroup
	wg.Add(1)
	var err error = nil
	fm.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(fm.filterTable))
		err = b.Put([]byte(id), []byte(json))
		if err == nil {
			log.Printf("Created filter %s", id)
		}
		wg.Done()
		return err
	})
	wg.Wait()
	return id, err
}

func filterFromJson(b []byte) *Filter {
	f := newFilter()
	if err := json.Unmarshal(b, &f); err != nil {
		log.Printf("Failed json umarshal %s", err)
		return nil
	}
	return f
}

func newFilter() *Filter {
	return &Filter{}
}

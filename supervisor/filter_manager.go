// Filter manager
// @author Robin Verlangen

package main

import (
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"log"
	"sync"
)

type FilterManager struct {
	db       *bolt.DB
	dbBucket string
}

type Filter struct {
	Regex string `json:"regex"`
	Id    string `json:"id"`
}

func (f *Filter) ToJson() (string, error) {
	bytes, err := json.Marshal(f)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func NewFilterManager() *FilterManager {
	fm := &FilterManager{
		dbBucket: "filter_manager",
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

	// Create bucket
	fm.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(fm.dbBucket))
		if err != nil {
			log.Fatal(fmt.Errorf("create bucket: %s", err))
		}
		return nil
	})
}

func (fm *FilterManager) CreateFilter(regex string) (string, error) {
	var id string = "1234"
	var filter *Filter = newFilter()
	filter.Regex = regex
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
		b := tx.Bucket([]byte(fm.dbBucket))
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

func newFilter() *Filter {
	return &Filter{}
}

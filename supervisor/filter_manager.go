// Filter manager
// @author Robin Verlangen

package main

import (
	"fmt"
	"github.com/boltdb/bolt"
	"log"
)

type FilterManager struct {
	db       *bolt.DB
	dbBucket string
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

func (fm *FilterManager) CreateFilter() {
	fm.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(fm.dbBucket))
		err := b.Put([]byte("answer"), []byte("42"))
		return err
	})
}

// Filter manager
// @author Robin Verlangen

package main

import (
	"bytes"
	"code.google.com/p/go-uuid/uuid"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"log"
	"sync"
	"time"
)

type FilterManager struct {
	db                  *bolt.DB
	filterTable         string
	filterResults       map[string][]*FilterResult
	filterResultsMux    sync.RWMutex
	filterStatsTable    string
	filterStats         map[string]*FilterStats
	filterOutliersTable string

	// Result counters
	filterResultCounter    map[string]uint64
	filterResultCounterMux sync.RWMutex

	// Caches
	filtersCache    []*Filter
	filtersCacheMux sync.RWMutex
}

type FilterResult struct {
	id     uint64
	fields map[string]string
}

type FilterStats struct {
	Metrics map[int]*FilterTimeseries `json:"-"`
}

type FilterTimeseries struct {
	Data map[int64]int64 `json:"-"`
}

type Filter struct {
	Regex      string       `json:"regex"`
	Name       string       `json:"name"`
	ClientHost string       `json:"client_host"`
	Id         string       `json:"id"`
	Stats      *FilterStats `json:"-"`
	//Results    []string `json:"results"`
	statsMux sync.RWMutex
}

func (f *Filter) Results() []*FilterResult {
	filterManager.filterResultsMux.RLock()
	if filterManager.filterResults[f.Id] == nil {
		filterManager.filterResultsMux.RUnlock()
		filterManager.filterResultsMux.Lock()
		filterManager.filterResults[f.Id] = make([]*FilterResult, 0)
		filterManager.filterResultsMux.Unlock()
	} else {
		defer filterManager.filterResultsMux.RUnlock()
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

// @todo Support multiple adapters for storage of statistics, currently only in memory
func (f *Filter) AddStats(metric int, timeBucket int64, count int64) bool {
	// Lock
	f.statsMux.Lock()

	// Stats wrapper
	if f.Stats == nil {
		f.Stats = newFilterStats()
	}

	// Metric wrapper?
	if f.Stats.Metrics[metric] == nil {
		f.Stats.Metrics[metric] = newFilterTimeseries()
	}

	// Store
	f.Stats.Metrics[metric].Data[timeBucket] += count

	// Persist in filter manager
	filterManager.filterStats[f.Id] = f.Stats

	// Unlock
	f.statsMux.Unlock()

	// Encode
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	f.statsMux.RLock()
	enc.Encode(f.Stats)
	f.statsMux.RUnlock()

	// Lazy persist
	go func(filterId string, buf bytes.Buffer) {
		filterManager.PersistStats(filterId, buf)
	}(f.Id, buf)

	return true
}

func (fm *FilterManager) PersistStats(filterId string, buf bytes.Buffer) {
	// Write to database
	var wg sync.WaitGroup
	wg.Add(1)
	var err error = nil
	filterManager.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(filterManager.filterStatsTable))
		err = b.Put([]byte(filterId), buf.Bytes())
		if err == nil {
			if verbose {
				log.Printf("Persisted filter %s timeseries", filterId)
			}
		}
		wg.Done()
		return err
	})
	wg.Wait()
}

func (f *Filter) GetStats() *FilterStats {
	f.statsMux.RLock()
	defer f.statsMux.RUnlock()
	return f.Stats
}

type Outlier struct {
	FilterId  string  `json:"filter_id"`
	Score     float64 `json:"score"`
	Timestamp int64   `json:"timestamp"`
	Details   string  `json:"details"`
}

// Remove all outliers
func (fm *FilterManager) TruncateOutliers() bool {
	log.Println("Truncating outliers")

	keys := make([]string, 0)
	var wg sync.WaitGroup
	wg.Add(1)
	fm.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(fm.filterOutliersTable))
		c := b.Cursor()

		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			keys = append(keys, fmt.Sprintf("%s", k))
		}
		wg.Done()
		return nil
	})
	wg.Wait()

	// Remove keys
	var wgrm sync.WaitGroup
	deleteCount := 0
	for _, key := range keys {
		wgrm.Add(1)
		// Remove key
		fm.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(fm.filterOutliersTable))
			err := b.Delete([]byte(key))
			if err != nil {
				log.Printf("Failed to remove outlier: %s", err)
			}
			deleteCount++
			wgrm.Done()
			return nil
		})
		wgrm.Wait()
	}
	log.Printf("Removed %d outliers", deleteCount)

	return true
}

// Remove all stats
func (fm *FilterManager) TruncateStats() bool {
	log.Println("Truncating stats")

	keys := make([]string, 0)
	var wg sync.WaitGroup
	wg.Add(1)
	fm.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(fm.filterStatsTable))
		c := b.Cursor()

		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			keys = append(keys, fmt.Sprintf("%s", k))
		}
		wg.Done()
		return nil
	})
	wg.Wait()

	// Remove keys
	var wgrm sync.WaitGroup
	deleteCount := 0
	for _, key := range keys {
		wgrm.Add(1)
		// Remove key
		fm.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(fm.filterStatsTable))
			err := b.Delete([]byte(key))
			if err != nil {
				log.Printf("Failed to remove stat: %s", err)
			}
			deleteCount++
			wgrm.Done()
			return nil
		})
		wgrm.Wait()
	}
	log.Printf("Removed %d stats", deleteCount)

	return true
}

// Store outlier
func (f *Filter) AddOutlier(ts int64, score float64, details string) bool {
	// ID
	var id string = uuid.New()

	// Struct
	outlier := &Outlier{}
	outlier.FilterId = f.Id
	outlier.Timestamp = ts
	outlier.Score = score
	outlier.Details = details

	// To JSON
	json, jsonErr := json.Marshal(outlier)
	if jsonErr != nil {
		log.Printf("Failed to marshal to json: %s", jsonErr)
		return false
	}

	// Create
	var wg sync.WaitGroup
	wg.Add(1)
	var err error = nil
	filterManager.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(filterManager.filterOutliersTable))
		// f-<filterid> prefix to allow prefix scans
		err = b.Put([]byte(fmt.Sprintf("f-%s-%s", f.Id, id)), []byte(json))
		if err != nil {
			log.Printf("Failed to create outlier %s", err)
		}
		wg.Done()
		return err
	})
	wg.Wait()
	return err == nil
}

// New result for this filter
func (f *Filter) newFilterResult(raw string) *FilterResult {
	// Get auto-increment key
	filterManager.filterResultCounterMux.Lock()
	filterManager.filterResultCounter[f.Id]++
	id := filterManager.filterResultCounter[f.Id]
	filterManager.filterResultCounterMux.Unlock()

	// Init
	elm := &FilterResult{
		id:     id,
		fields: make(map[string]string),
	}
	elm.fields["_raw"] = raw
	return elm
}

// @todo Support multiple adapters for storage of results, currently only in memory
func (f *Filter) AddResults(res []string) bool {
	filterManager.filterResultsMux.Lock()

	// Init variable
	if filterManager.filterResults[f.Id] == nil {
		filterManager.filterResults[f.Id] = make([]*FilterResult, 0)
	}

	// Exceed limit?
	newCount := len(res)
	currentCount := len(filterManager.filterResults[f.Id])
	newPlusCurrent := newCount + currentCount
	if newPlusCurrent > maxMsgMemory {
		tooMany := newPlusCurrent - maxMsgMemory
		if verbose {
			log.Printf("Truncating memory for filter %s, exceeding limit of %d messages. Before %d. New %d. Too many %d", f.Id, maxMsgMemory, currentCount, newCount, tooMany)
		}
		tmp := make([]*FilterResult, 0)
		for i := tooMany; i < currentCount-1; i++ {
			// Debug out of bounds
			if i < 0 || i > currentCount-1 {
				if verbose {
					log.Printf("I %d out of bounds", i)
				}
				continue
			}

			// Append to TMP array
			tmp = append(tmp, filterManager.filterResults[f.Id][i])
		}
		filterManager.filterResults[f.Id] = tmp
	}

	// Add lines
	// @todo It is possible that there is a big resultset immediately overflow maxMsgMemory
	for _, line := range res {
		res := f.newFilterResult(line)
		filterManager.filterResults[f.Id] = append(filterManager.filterResults[f.Id], res)
	}
	filterManager.filterResultsMux.Unlock()
	return true
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
	wg.Add(1)
	fm.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(fm.filterStatsTable))
		if err != nil {
			log.Fatal(fmt.Errorf("create bucket: %s", err))
		}
		wg.Done()
		return nil
	})
	wg.Add(1)
	fm.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(fm.filterOutliersTable))
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
	// Cache
	fm.filtersCacheMux.RLock()
	if fm.filtersCache != nil {
		fm.filtersCacheMux.RUnlock()
		return fm.filtersCache
	}
	fm.filtersCacheMux.RUnlock()

	// Load
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

	// Save in cache
	fm.filtersCacheMux.Lock()
	fm.filtersCache = list
	fm.filtersCacheMux.Unlock()

	return list
}

func (fm *FilterManager) GetFilter(id string) *Filter {
	// Load from cache
	fm.filtersCacheMux.RLock()
	defer fm.filtersCacheMux.RUnlock()
	for _, filter := range fm.filtersCache {
		if filter.Id == id {
			return filter
		}
	}

	// Load from db
	var wg sync.WaitGroup
	var elm *Filter = nil
	wg.Add(1)
	// Load filter
	fm.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(fm.filterTable))
		res := b.Get([]byte(id))
		elm = filterFromJson(res)
		wg.Done()
		return nil
	})
	// Load filter stats
	wg.Add(1)
	var stats *FilterStats
	fm.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(fm.filterStatsTable))
		res := b.Get([]byte(id))
		if res != nil {
			var buf bytes.Buffer
			buf.Write(res)
			dec := gob.NewDecoder(&buf)
			de := dec.Decode(&stats)
			if de != nil {
				stats = nil
				log.Printf("Failed to load timeseries %s", de)
			}
		}
		wg.Done()
		return nil
	})
	wg.Wait()

	// Elm found?
	if elm == nil {
		return nil
	}

	// Load stats
	if stats != nil {
		elm.Stats = stats
	} else {
		elm.Stats = newFilterStats()
	}
	return elm
}

func (fm *FilterManager) DeleteFilter(id string) bool {
	// Remove
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

	// Invalidate cache
	fm.filtersCacheMux.Lock()
	fm.filtersCache = nil
	fm.filtersCacheMux.Unlock()

	return val
}

// This will cleanup the timeseries database every once in a while
func (fm *FilterManager) TimeseriesCleaner() {
	go func() {
		c := time.Tick(5 * time.Minute)
		for _ = range c {
			if verbose {
				log.Println("Cleaning timeseries database")
			}

			// Scan keys
			fm.db.View(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte(fm.filterStatsTable))
				c := b.Cursor()

				// Iterate
				for k, v := c.First(); k != nil; k, v = c.Next() {
					// Read data
					var stats *FilterStats
					var buf bytes.Buffer
					buf.Write(v)
					dec := gob.NewDecoder(&buf)
					de := dec.Decode(&stats)
					nowUnix := time.Now().Unix()
					maxUnixAge := nowUnix - (7 * 86400)
					dirty := false
					if de == nil && stats != nil {
						for _, timeseries := range stats.Metrics {
							for ts, _ := range timeseries.Data {
								if ts < maxUnixAge {
									delete(timeseries.Data, ts)
									dirty = true
									if verbose {
										log.Printf("Filter %s is dirty", string(k))
									}
								}
							}
						}
					}

					// Store (async, else it will block with the read transaction)
					if dirty {
						var writeBuf bytes.Buffer
						enc := gob.NewEncoder(&writeBuf)
						enc.Encode(stats)
						go func(filterId string, writeBuf bytes.Buffer) {
							filterManager.PersistStats(filterId, writeBuf)
						}(string(k), writeBuf)
					}
				}
				return nil
			})

			if verbose {
				log.Println("Cleaned timeseries database")
			}
		}
	}()
}

// Create a new filter
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

	// Invalidate cache
	fm.filtersCacheMux.Lock()
	fm.filtersCache = nil
	fm.filtersCacheMux.Unlock()

	return id, err
}

// Restore a filter from json bytes
func filterFromJson(b []byte) *Filter {
	f := newFilter()
	if err := json.Unmarshal(b, &f); err != nil {
		log.Printf("Failed json umarshal %s", err)
		return nil
	}
	return f
}

// Init the filter manager
func NewFilterManager() *FilterManager {
	fm := &FilterManager{
		filterTable:         "filters",
		filterStatsTable:    "filter_stats",
		filterOutliersTable: "filter_outliers",
		filterResults:       make(map[string][]*FilterResult),
		filterStats:         make(map[string]*FilterStats),
		filterResultCounter: make(map[string]uint64),
	}
	fm.Open()
	fm.TimeseriesCleaner()
	return fm
}

func newFilter() *Filter {
	return &Filter{
		Stats: newFilterStats(),
	}
}

func newFilterStats() *FilterStats {
	return &FilterStats{
		Metrics: make(map[int]*FilterTimeseries),
	}
}

func newFilterTimeseries() *FilterTimeseries {
	return &FilterTimeseries{
		Data: make(map[int64]int64),
	}
}

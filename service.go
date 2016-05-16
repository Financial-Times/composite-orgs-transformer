package main

import (
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/boltdb/bolt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

)

const cacheBucket = "combinedorg"
const cacheFileName = "cache.db"
const uppIdentifier = "http://api.ft.com/system/FT-UPP"

type orgsService interface {
	getOrgs() ([]listEntry, bool)
	getOrgByUUID(uuid string) (combinedOrg, bool, error)
	isInitialised() bool
	getBaseURI() string
}

type orgServiceImpl struct {
	fsURL            string
	v1URL            string
	concorder        concorder
	combinedOrgCache map[string]*combinedOrg
	list             []listEntry
	initialised      bool
	baseURI string
}

func (s *orgServiceImpl) getBaseURI() string {
	return s.baseURI
}

func (s *orgServiceImpl) isInitialised() bool {
	return s.initialised
}

func (s *orgServiceImpl) getOrgs() ([]listEntry, bool) {
	if len(s.list) > 0 {
		return s.list, true
	}
	return nil, false
}

func (orgHandler *orgServiceImpl) getOrgByUUID(uuid string) (combinedOrg, bool, error) {
	db, err := bolt.Open(cacheFileName, 0600, &bolt.Options{ReadOnly: true, Timeout: 10 * time.Second})
	if err != nil {
		log.Errorf("ERROR opening cache file for [%v]: %v", uuid, err.Error())
		return combinedOrg{}, false, err
	}
	defer db.Close()
	var cachedValue []byte
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(cacheBucket))
		if bucket == nil {
			return fmt.Errorf("Bucket %v not found!", cacheBucket)
		}
		cachedValue = bucket.Get([]byte(uuid))
		return nil
	})

	if err != nil {
		log.Errorf("ERROR reading from cache file for [%v]: %v", uuid, err.Error())
		return combinedOrg{}, false, err
	}
	if cachedValue == nil || len(cachedValue) == 0 {
		log.Infof("INFO No cached value for [%v]", uuid)
		return combinedOrg{}, false, nil
	}
	var org combinedOrg
	if err = json.Unmarshal(cachedValue, &org); err != nil {
		log.Errorf("ERROR unmarshalling cached value for [%v]: %v", uuid, err.Error())
		return combinedOrg{}, true, err
	}
	return org, true, nil
}

func (orgHandler *orgServiceImpl) load() {
	db, err := bolt.Open(cacheFileName, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Errorf("ERROR opening cache file for init: %v", err.Error())
		return
	}
	defer db.Close()
	if err = createCacheBucket(db); err != nil {
		log.Errorf("ERROR creating cache bucket: %v", err.Error())
		return
	}

	if err = orgHandler.concorder.load(); err != nil {
		log.Errorf("ERROR loading concordance data: %+v", err.Error())
		return
	}
	if err = orgHandler.loadCombinedOrgs(db); err != nil {
		log.Errorf("ERROR loading combined organisations: %+v", err.Error())
		return
	}
	orgHandler.initialised = true
}

func createCacheBucket(db *bolt.DB) error {
	return db.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket([]byte(cacheBucket))
		if err != nil {
			log.Warnf("Cache bucket [%v] could not be deleted\n", cacheBucket)
		}
		_, err = tx.CreateBucket([]byte(cacheBucket))
		if err != nil {
			return err
		}
		return nil
	})
}

func (orgHandler *orgServiceImpl) loadCombinedOrgs(db *bolt.DB) error {
	fsOrgs := make(chan combinedOrg)
	v1Orgs := make(chan combinedOrg)
	errs := make(chan error, 3)

	go fetchAllOrgsFromURL(orgHandler.fsURL, fsOrgs, errs)
	go fetchAllOrgsFromURL(orgHandler.v1URL, v1Orgs, errs)

	combineOrgChan := make(chan *combinedOrg)

	go func() {
		log.Printf("DEBUG - Combining results")
		orgHandler.combineOrganisations(combineOrgChan, fsOrgs, v1Orgs, errs)
	}()

	combinedOrgCache := make(map[string]*combinedOrg)
	threshold := 100000
	var wg sync.WaitGroup
	for {
		select {
		case err := <-errs:
			log.Errorf("Oh no! %+v", err.Error())
			return err
		case combinedOrgResult, ok := <-combineOrgChan:
			if !ok {
				log.Printf("Almost done. Waiting for subroutines to terminate")
				storeOrgToCache(db, combinedOrgCache, nil)
				wg.Wait()
				for k := range combinedOrgCache {
					delete(combinedOrgCache, k)
				}
				log.Printf("Finished composite org load: %v values", len(orgHandler.list))
				return nil
			}
			if len(orgHandler.list)%5000 == 1 {
				fmt.Printf("Progress: %v", len(orgHandler.list))
			}
			orgHandler.list = append(orgHandler.list, listEntry{fmt.Sprintf("%s/%s", orgHandler.baseURI, combinedOrgResult.UUID)})
			combinedOrgCache[combinedOrgResult.UUID] = combinedOrgResult

			if len(combinedOrgCache) > threshold {
				wg.Add(1)
				log.Printf("Added: %+v\n", wg)
				copyOfCombinedOrgCache := make(map[string]*combinedOrg)
				for k, v := range combinedOrgCache {
					copyOfCombinedOrgCache[k] = v
					delete(combinedOrgCache, k)
				}
				go storeOrgToCache(db, copyOfCombinedOrgCache, &wg)
				combinedOrgCache = make(map[string]*combinedOrg)
			}

		}
	}

}

func storeOrgToCache(db *bolt.DB, cacheToBeWritten map[string]*combinedOrg, wg *sync.WaitGroup) {
	start := time.Now()
	if wg != nil {
		defer func(startTime time.Time) {
			log.Printf("Done, elapsed time: %+v, size: %v\n", time.Since(startTime), len(cacheToBeWritten))
			wg.Done()
		}(start)
	} else {
		defer func(startTime time.Time) {
			log.Printf("Done, elapsed time: %+v, size: %v\n", time.Since(startTime), len(cacheToBeWritten))
		}(start)
	}

	err := db.Batch(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte(cacheBucket))
		if bucket == nil {
			return fmt.Errorf("Bucket %v not found!", cacheBucket)
		}
		for _, combinedOrgResult := range cacheToBeWritten {
			marshalledCombinedOrg, err := json.Marshal(combinedOrgResult)
			if err != nil {
				return err
			}
			err = bucket.Put([]byte(combinedOrgResult.UUID), marshalledCombinedOrg)
			if err != nil {
				return err
			}
			if combinedOrgResult.Identifiers != nil && len(combinedOrgResult.Identifiers) > 0 {
				for _, identifierVal := range combinedOrgResult.Identifiers {
					if identifierVal.Authority == uppIdentifier {
						err = bucket.Put([]byte(identifierVal.IdentifierValue), marshalledCombinedOrg)
						if err != nil {
							return err
						}
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		log.Errorf("ERROR store: %+v", err) //TODO how to handle?
	}

}

func fetchAllOrgsFromURL(listEndpoint string, orgs chan<- combinedOrg, errs chan<- error) {
	log.Printf("Starting fetching entries for %v", listEndpoint)
	resp, err := httpClient.Get(listEndpoint)
	if err != nil {
		errs <- err
		return
	}
	defer resp.Body.Close()
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()
	if resp.StatusCode == http.StatusServiceUnavailable {
		//TODO implement retry mechanism
	}

	var list []listEntry
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&list); err != nil {
		errs <- err
		return
	}
	log.Printf("Fetched %v entries for %v\n", len(list), listEndpoint)
	count := 0
	//TODO trying to avoid server overload
	var wg sync.WaitGroup
	var maxNrConcurrentGoroutines = 30
	concurrentGoroutines := make(chan struct{}, maxNrConcurrentGoroutines)
	defer close(concurrentGoroutines)
	// Fill the dummy channel with maxNbConcurrentGoroutines empty struct.
	for i := 0; i < maxNrConcurrentGoroutines; i++ {
		concurrentGoroutines <- struct{}{}
	}
	for _, entry := range list {
		wg.Add(1)
		go fetchOrgFromURL(entry.APIURL, orgs, errs, &wg, concurrentGoroutines)
		<-concurrentGoroutines
		//TODO delete this debug section if you can handle 5 million entries
		count++
		//if count > 10000 {
		//	break
		//}
	}
	wg.Wait()
	close(orgs)
}

func fetchOrgFromURL(url string, orgs chan<- combinedOrg, errs chan<- error, wg *sync.WaitGroup, limitGoroutinesChannel chan<- struct{}) {
	defer wg.Done()
	defer func(limitGoroutinesChannel chan<- struct{}) {
		limitGoroutinesChannel <- struct{}{}
	}(limitGoroutinesChannel)
	time.Sleep(50 * time.Millisecond)
	retryCount := 0
	var resp *http.Response
	var err error
	for {
		resp, err = httpClient.Get(url)
		if err != nil && retryCount > 2 {
			errs <- err
			return
		}
		if err != nil {
			log.Warnf("Error appeared to get %v, retrying\n", url)
			retryCount++
			time.Sleep(50 * time.Millisecond)
			continue
		}
		break
	}
	defer resp.Body.Close()
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()
	if resp.StatusCode == http.StatusServiceUnavailable {
		//TODO implement retry mechanism
	}
	if resp.StatusCode == http.StatusNotFound {
		//TODO skip or not?
		log.Warnf("Organisation went missing for url: %v\n", url)
		return
	}
	if resp.StatusCode != http.StatusOK {
		//TODO skip or not?
		log.Errorf("Calling url %v returned status %v\n", url, resp.StatusCode)
		errs <- err
		return
	}
	var org combinedOrg
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&org); err != nil {
		log.Errorf("Error decoding response from %v, response: %+v\n", url, resp)
		errs <- err
		return
	}
	orgs <- org
}

func (orgHandler *orgServiceImpl) combineOrganisations(combineOrgChan chan *combinedOrg, fsOrgs chan combinedOrg, v1Orgs chan combinedOrg, errs chan error) {
	fsDone := false
	v1Done := false
	for fsDone != true || v1Done != true {
		select {
		case fsOrg, ok := <-fsOrgs:
			if !ok {
				fsDone = true
			} else {
				orgHandler.handleFsOrg(fsOrg, combineOrgChan, errs)
			}
		case v1Org, ok := <-v1Orgs:
			if !ok {
				v1Done = true
			} else {
				orgHandler.handleV1Org(v1Org, combineOrgChan, errs)
			}
		}
	}
	log.Printf("Out from the loop")
	close(combineOrgChan)
}

func (orgHandler *orgServiceImpl) handleFsOrg(fsOrg combinedOrg, combineOrgChan chan *combinedOrg, errs chan error) {
	v1UUID, found, err := orgHandler.concorder.v2tov1(fsOrg.UUID)
	if err != nil {
		errs <- err
		return
	}
	if !found {
		fsOrg.Identifiers = append(fsOrg.Identifiers, identifier{Authority: uppIdentifier, IdentifierValue: fsOrg.UUID})
		combineOrgChan <- &fsOrg
	} else {
		combineOrgFromFs(&fsOrg, v1UUID)
		combineOrgChan <- &fsOrg
	}
}

func (orgHandler *orgServiceImpl) handleV1Org(v1Org combinedOrg, combineOrgChan chan *combinedOrg, errs chan error) {
	fsUUID, err := orgHandler.concorder.v1tov2(v1Org.UUID)
	if err != nil {
		errs <- err
		return
	}
	if fsUUID == "" {
		v1Org.Identifiers = append(v1Org.Identifiers, identifier{Authority: uppIdentifier, IdentifierValue: v1Org.UUID})
		combineOrgChan <- &v1Org
	} else {
		combineOrgFromV1(&v1Org, fsUUID)
		combineOrgChan <- &v1Org
	}
}

func combineOrgFromFs(fsOrg *combinedOrg, v1UUID map[string]struct{}) {
	var uuids []string
	for uuidString := range v1UUID {
		uuids = append(uuids, uuidString)
	}
	uuids = append(uuids, fsOrg.UUID)

	canonicalUUID, _ := canonical(uuids...)
	fsOrg.UUID = canonicalUUID
	for i := 0; i < len(uuids); i++ {
		fsOrg.Identifiers = append(fsOrg.Identifiers,
			identifier{Authority: uppIdentifier, IdentifierValue: uuids[i]})

	}
}

func combineOrgFromV1(v1Org *combinedOrg, fsUUID string) {
	canonicalUUID, _ := canonical(v1Org.UUID, fsUUID)

	v1Org.Identifiers = append(v1Org.Identifiers, identifier{Authority: uppIdentifier, IdentifierValue: v1Org.UUID})
	v1Org.Identifiers = append(v1Org.Identifiers, identifier{Authority: uppIdentifier, IdentifierValue: fsUUID})
	v1Org.UUID = canonicalUUID
}

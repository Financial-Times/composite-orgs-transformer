package main

import (
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/boltdb/bolt"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"
	"sync"
	"time"
)

const (
	compositeOrgsBucket = "combinedorg"
	cacheFileName       = "cache.db"
	uppIdentifier       = "http://api.ft.com/system/FT-UPP"
)

var uuidExtractRegex = regexp.MustCompile(".*([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$")
var concurrentGoroutines = make(chan struct{}, 300)

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
	baseURI          string
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

func (s *orgServiceImpl) getOrgByUUID(uuid string) (combinedOrg, bool, error) {
	db, err := bolt.Open(cacheFileName, 0600, &bolt.Options{ReadOnly: true, Timeout: 10 * time.Second})
	if err != nil {
		log.Errorf("ERROR opening cache file for [%v]: %v", uuid, err.Error())
		return combinedOrg{}, false, err
	}
	defer db.Close()
	var cachedValue []byte
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(compositeOrgsBucket))
		if bucket == nil {
			return fmt.Errorf("Bucket %v not found!", compositeOrgsBucket)
		}
		cachedValue = bucket.Get([]byte(uuid))
		return nil
	})

	if err != nil {
		log.Errorf("ERROR reading from cache file for [%v]: %v", uuid, err.Error())
		return combinedOrg{}, false, err
	}
	if len(cachedValue) == 0 {
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

func (s *orgServiceImpl) load() {
	db, err := bolt.Open(cacheFileName, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Errorf("ERROR opening cache file for init: %v", err.Error())
		return
	}
	defer db.Close()
	if err = createCacheBucket(compositeOrgsBucket, db); err != nil {
		log.Errorf("ERROR creating compositeOrgsBucket: %v", err.Error())
		return
	}

	if err = s.concorder.load(); err != nil {
		log.Errorf("ERROR loading concordance data: %+v", err.Error())
		return
	}
	if err = s.loadCombinedOrgs(db); err != nil {
		log.Errorf("ERROR loading combined organisations: %+v", err.Error())
		return
	}

	s.initialised = true
}

func createCacheBucket(bucketName string, db *bolt.DB) error {
	return db.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket([]byte(bucketName))
		if err != nil {
			log.Warnf("Cache bucket [%v] could not be deleted\n", bucketName)
		}
		_, err = tx.CreateBucket([]byte(bucketName))
		return err
	})
}

func (s *orgServiceImpl) loadCombinedOrgs(db *bolt.DB) error {
	fsOrgs := make(chan string)
	v1Orgs := make(chan string)
	errs := make(chan error, 3)

	go fetchAllOrgsFromURL(s.fsURL, fsOrgs, errs)
	go fetchAllOrgsFromURL(s.v1URL, v1Orgs, errs)

	combineOrgChan := make(chan *combinedOrg)

	go func() {
		log.Debugf("Combining results")
		s.combineOrganisations(combineOrgChan, fsOrgs, v1Orgs, errs)
	}()

	combinedOrgCache := make(map[string]*combinedOrg)
	threshold := 10000
	var wg sync.WaitGroup
	for {
		select {
		case err := <-errs:
			log.Errorf("Oh no! %+v", err.Error())
			return err
		case combinedOrgResult, ok := <-combineOrgChan:
			if !ok {
				log.Debug("Almost done. Waiting for subroutines to terminate")
				storeOrgToCache(db, combinedOrgCache, nil)
				wg.Wait()
				for k := range combinedOrgCache {
					delete(combinedOrgCache, k)
				}
				log.Debugf("Finished composite org load: %v values", len(s.list))
				return nil
			}
			if len(s.list)%100000 == 1 {
				log.Debugf("Progress: %v", len(s.list))
			}
			s.list = append(s.list, listEntry{fmt.Sprintf("%s/%s", s.baseURI, combinedOrgResult.UUID)})

			//save to cache only concorded orgs. For non concorded orgs combinedOrgResult will only contain UUID
			if combinedOrgResult.Type != "" {
				combinedOrgCache[combinedOrgResult.UUID] = combinedOrgResult
			}

			if len(combinedOrgCache) > threshold {
				wg.Add(1)
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
			log.Debugf("Done, elapsed time: %+v, size: %v\n", time.Since(startTime), len(cacheToBeWritten))
			wg.Done()
		}(start)
	} else {
		defer func(startTime time.Time) {
			log.Debugf("Done, elapsed time: %+v, size: %v\n", time.Since(startTime), len(cacheToBeWritten))
		}(start)
	}

	err := db.Batch(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte(compositeOrgsBucket))
		if bucket == nil {
			return fmt.Errorf("Bucket %v not found!", compositeOrgsBucket)
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

func fetchAllOrgsFromURL(listEndpoint string, orgs chan<- string, errs chan<- error) {
	log.Debugf("Starting fetching entries for %v", listEndpoint)
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

	for _, entry := range list {
		orgs <- uuidExtractRegex.FindStringSubmatch(entry.APIURL)[1]
	}
	close(orgs)
}

func (s *orgServiceImpl) fetchOrgFromURL(url string) (combinedOrg, error) {
	defer func(limitGoroutinesChannel chan struct{}) {
		<-limitGoroutinesChannel
	}(concurrentGoroutines)
	retryCount := 0
	var resp *http.Response
	var err error
	for {
		resp, err = httpClient.Get(url)
		if err != nil && retryCount > 2 {
			return combinedOrg{}, err
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
		return combinedOrg{}, err
	}
	if resp.StatusCode == http.StatusNotFound {
		//TODO skip or not?
		log.Warnf("Organisation went missing for url: %v\n", url)
		return combinedOrg{}, err
	}
	if resp.StatusCode != http.StatusOK {
		//TODO skip or not?
		log.Errorf("Calling url %v returned status %v\n", url, resp.StatusCode)
		return combinedOrg{}, err
	}
	var org combinedOrg
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&org); err != nil {
		log.Errorf("Error decoding response from %v, response: %+v\n", url, resp)
		return combinedOrg{}, err
	}
	return org, nil
}

func (s *orgServiceImpl) combineOrganisations(combineOrgChan chan *combinedOrg, fsOrgs chan string, v1Orgs chan string, errs chan error) {
	var cWait sync.WaitGroup
	fsDone := false
	v1Done := false
	for !fsDone || !v1Done {
		select {
		case fsOrgUUID, ok := <-fsOrgs:
			if !ok {
				fsDone = true
			} else {
				cWait.Add(1)
				go func() {
					s.handleFsOrg(fsOrgUUID, combineOrgChan, errs)
					cWait.Done()
				}()
			}
		case v1OrgUUID, ok := <-v1Orgs:
			if !ok {
				v1Done = true
			} else {
				cWait.Add(1)
				go func() {
					s.handleV1Org(v1OrgUUID, combineOrgChan, errs)
					cWait.Done()
				}()
			}
		}
	}
	cWait.Wait()
	close(concurrentGoroutines)
	close(combineOrgChan)
}

func (s *orgServiceImpl) handleFsOrg(fsOrgUUID string, combineOrgChan chan *combinedOrg, errs chan error) {
	v1UUID, found, err := s.concorder.v2tov1(fsOrgUUID)
	if err != nil {
		errs <- err
		return
	}
	if found {
		o, err := s.mergeOrgs(fsOrgUUID, v1UUID)
		if err != nil {
			errs <- err
		}
		combineOrgChan <- &o
	} else {
		combineOrgChan <- &combinedOrg{UUID: fsOrgUUID}
	}
}

func (s *orgServiceImpl) handleV1Org(v1OrgUUID string, combineOrgChan chan *combinedOrg, errs chan error) {
	fsUUID, err := s.concorder.v1tov2(v1OrgUUID)
	if err != nil {
		errs <- err
		return
	}
	if fsUUID == "" {
		combineOrgChan <- &combinedOrg{UUID: v1OrgUUID}
	}
}

//This is the function where condordance rules will be applied.
//This is still relying on the fact that v2-orgs-transformer returns concorded info like TME identifiers.
func (s *orgServiceImpl) mergeOrgs(fsOrgUUID string, v1UUID map[string]struct{}) (combinedOrg, error) {
	var uuids []string
	concurrentGoroutines <- struct{}{}
	v2Org, err := s.fetchOrgFromURL(s.fsURL + "/" + fsOrgUUID)
	if err != nil {
		return combinedOrg{}, err
	}

	for uuidString := range v1UUID {
		uuids = append(uuids, uuidString)
	}
	uuids = append(uuids, v2Org.UUID)

	canonicalUUID, _ := canonical(uuids...)
	v2Org.UUID = canonicalUUID
	for i := 0; i < len(uuids); i++ {
		v2Org.Identifiers = append(v2Org.Identifiers,
			identifier{Authority: uppIdentifier, IdentifierValue: uuids[i]})

	}
	return v2Org, nil
}

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
	orgsUrlsBucket      = "orgsuris"
	uppIdentifier       = "http://api.ft.com/system/FT-UPP"
)

var uuidExtractRegex = regexp.MustCompile(".*([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$")
var concurrentGoroutines = make(chan struct{}, 100)

type orgsService interface {
	getOrgs() ([]byte, error)
	getOrgByUUID(uuid string) (combinedOrg, bool, error)
	isInitialised() bool
	getBaseURI() string
	count() int
}

type orgServiceImpl struct {
	fsURL            string
	v1URL            string
	concorder        concorder
	client           httpClient
	combinedOrgCache map[string]*combinedOrg
	list             []listEntry
	initialised      bool
	baseURI          string
	cacheFileName    string
	c                int
}

func (s *orgServiceImpl) getBaseURI() string {
	return s.baseURI
}

func (s *orgServiceImpl) isInitialised() bool {
	return s.initialised
}

func (s *orgServiceImpl) count() int {
	return s.c
}

func (s *orgServiceImpl) getOrgs() (orgs []byte, err error) {
	db, err := bolt.Open(s.cacheFileName, 0600, &bolt.Options{ReadOnly: true, Timeout: 10 * time.Second})
	if err != nil {
		//log.Errorf("ERROR opening cache file for [%v]: %v", uuid, err.Error())
		return nil, err
	}
	defer db.Close()
	var cachedValue []byte
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(orgsUrlsBucket))
		if bucket == nil {
			return fmt.Errorf("Bucket %v not found!", orgsUrlsBucket)
		}
		cachedValue = bucket.Get([]byte("orgs"))
		return nil
	})

	if err != nil {
		return nil, err
	}
	if len(cachedValue) == 0 {
		log.Infof("INFO No cached value for [%v]", "orgs")
		return nil, nil
	}
	c := make([]byte, len(cachedValue))
	copy(c, cachedValue)

	return c, nil
}

func (s *orgServiceImpl) getOrgByUUID(uuid string) (combinedOrg, bool, error) {
	db, err := bolt.Open(s.cacheFileName, 0600, &bolt.Options{ReadOnly: true, Timeout: 10 * time.Second})
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

func (s *orgServiceImpl) load() error {
	db, err := bolt.Open(s.cacheFileName, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return fmt.Errorf("ERROR opening cache file for init: %v", err)
	}
	defer db.Close()
	if err = createCacheBucket(compositeOrgsBucket, db); err != nil {
		return fmt.Errorf("ERROR creating compositeOrgsBucket: %v", err)
	}

	if err = createCacheBucket(orgsUrlsBucket, db); err != nil {
		return fmt.Errorf("ERROR creating orgsUrlsBucket: %v", err)
	}

	if err = s.concorder.load(); err != nil {
		return fmt.Errorf("ERROR loading concordance data: %+v", err)
	}
	if err = s.loadCombinedOrgs(db); err != nil {
		return fmt.Errorf("ERROR loading combined organisations: %+v", err)
	}

	if err = s.storeOrgsUrls(db); err != nil {
		return fmt.Errorf("ERROR loading combined organisations: %+v", err)
	}

	s.initialised = true
	return nil
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

func (s *orgServiceImpl) storeOrgsUrls(db *bolt.DB) error {
	return db.Batch(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte(orgsUrlsBucket))
		if bucket == nil {
			return fmt.Errorf("Bucket %v not found!", orgsUrlsBucket)
		}
		marshalledOrgsUrls, err := json.Marshal(s.list)
		if err != nil {
			return err
		}
		s.list = nil
		err = bucket.Put([]byte("orgs"), marshalledOrgsUrls)
		return err
	})
}

func (s *orgServiceImpl) loadCombinedOrgs(db *bolt.DB) error {
	fsOrgs := make(chan string)
	v1Orgs := make(chan string)
	errs := make(chan error)
	done := make(chan struct{})
	go s.fetchAllOrgsFromURL(s.fsURL, fsOrgs, errs, done)
	go s.fetchAllOrgsFromURL(s.v1URL, v1Orgs, errs, done)

	combineOrgChan := make(chan *combinedOrg)
	s.list = make([]listEntry, 1)
	go func() {
		log.Debugf("Combining results")
		s.combineOrganisations(combineOrgChan, fsOrgs, v1Orgs, errs, done)
	}()

	combinedOrgCache := make(map[string]*combinedOrg)
	threshold := 10000
	var wg sync.WaitGroup
	for {
		select {
		case err := <-errs:
			close(done)
			return err
		case combinedOrgResult, ok := <-combineOrgChan:
			if !ok {
				log.Debug("Almost done. Waiting for subroutines to terminate")
				storeOrgToCache(db, combinedOrgCache, nil, errs)
				wg.Wait()
				for k := range combinedOrgCache {
					delete(combinedOrgCache, k)
				}
				log.Debugf("Finished composite org load: %v values", len(s.list))
				s.c = len(s.list)
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
				go storeOrgToCache(db, copyOfCombinedOrgCache, &wg, errs)
				combinedOrgCache = make(map[string]*combinedOrg)
			}

		}
	}

}

func storeOrgToCache(db *bolt.DB, cacheToBeWritten map[string]*combinedOrg, wg *sync.WaitGroup, errs chan<- error) {
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
		errs <- err
	}

}

func (s *orgServiceImpl) fetchAllOrgsFromURL(listEndpoint string, orgs chan<- string, errs chan<- error, done <-chan struct{}) {
	log.Debugf("Starting fetching entries for %v", listEndpoint)
	resp, err := s.client.Get(listEndpoint)
	if err != nil {
		errs <- err
		return
	}
	defer resp.Body.Close()
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		errs <- fmt.Errorf("Could not get orgs from %v. Returned %v", listEndpoint, resp.StatusCode)
	}

	var list []listEntry
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&list); err != nil {
		errs <- err
		return
	}
	defer close(orgs)
	for _, entry := range list {
		select {
		case orgs <- uuidExtractRegex.FindStringSubmatch(entry.APIURL)[1]:
		case <-done:
			return
		}
	}
}

func (s *orgServiceImpl) fetchOrgFromURL(url string) (combinedOrg, error) {
	defer func(limitGoroutinesChannel chan struct{}) {
		<-limitGoroutinesChannel
	}(concurrentGoroutines)
	resp, err := s.client.Get(url)
	if err != nil {
		return combinedOrg{}, fmt.Errorf("Could not connect to %v. Error %v", url, err)
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return combinedOrg{}, fmt.Errorf("Could not get orgs from %v. Returned %v", url, resp.StatusCode)
	}
	var org combinedOrg
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&org); err != nil {
		log.Errorf("Error decoding response from %v, response: %+v\n", url, resp)
		return combinedOrg{}, fmt.Errorf("Error decoding response from %v, error: %+v", url, err)
	}
	return org, nil
}

func (s *orgServiceImpl) combineOrganisations(combineOrgChan chan *combinedOrg, fsOrgs chan string, v1Orgs chan string, errs chan error, done <-chan struct{}) {
	var cWait sync.WaitGroup
	fsDone := false
	v1Done := false
	defer func() {
		close(combineOrgChan)
	}()
	for !fsDone || !v1Done {
		select {
		case fsOrgUUID, ok := <-fsOrgs:
			if !ok {
				fsDone = true
			} else {
				cWait.Add(1)
				go func() {
					s.handleFsOrg(fsOrgUUID, combineOrgChan, errs, done)
					cWait.Done()
				}()
			}
		case v1OrgUUID, ok := <-v1Orgs:
			if !ok {
				v1Done = true
			} else {
				cWait.Add(1)
				go func() {
					s.handleV1Org(v1OrgUUID, combineOrgChan, errs, done)
					cWait.Done()
				}()
			}
		case <-done:
			return
		}
	}
	cWait.Wait()
	close(concurrentGoroutines)
}

func (s *orgServiceImpl) handleFsOrg(fsOrgUUID string, combineOrgChan chan *combinedOrg, errs chan error, done <-chan struct{}) {
	v1UUID, found, err := s.concorder.v2tov1(fsOrgUUID)
	if err != nil {
		select {
		case errs <- err:
		default:
			return
		}
	}
	var org combinedOrg
	if found {
		org, err = s.mergeOrgs(fsOrgUUID, v1UUID)
		if err != nil {
			errs <- err
		}
	} else {
		org = combinedOrg{UUID: fsOrgUUID}
	}
	select {
	case combineOrgChan <- &org:
	case <-done:
		return
	}
}

func (s *orgServiceImpl) handleV1Org(v1OrgUUID string, combineOrgChan chan *combinedOrg, errs chan error, done <-chan struct{}) {
	fsUUID, err := s.concorder.v1tov2(v1OrgUUID)
	if err != nil {
		select {
		case errs <- err:
		default:
			return
		}
	}
	if fsUUID == "" {
		select {
		case combineOrgChan <- &combinedOrg{UUID: v1OrgUUID}:
		case <-done:
			return
		}
	}
}

//This is the function where condordance rules will be applied.
//This is still relying on the fact that v2-orgs-transformer returns concorded info like TME identifiers.
func (s *orgServiceImpl) mergeOrgs(fsOrgUUID string, v1UUID map[string]struct{}) (combinedOrg, error) {
	var uuids []string
	concurrentGoroutines <- struct{}{}
	v2Org, err := s.fetchOrgFromURL(s.fsURL + "/" + fsOrgUUID)
	if err != nil {
		return v2Org, err
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

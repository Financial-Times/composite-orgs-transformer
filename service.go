package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/boltdb/bolt"
	"regexp"
	"sync"
	"time"
)

const (
	compositeOrgsBucket = "combinedorg"
	orgsUrlsBucket      = "orgsuris"
	uppIdentifier       = "http://api.ft.com/system/FT-UPP"
	tmeIdentifier       = "http://api.ft.com/system/FT-TME"
)

var uuidExtractRegex = regexp.MustCompile(".*([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$")

type orgsService interface {
	getOrgs() ([]byte, error)
	getOrgByUUID(uuid string) (combinedOrg, bool, error)
	isInitialised() bool
	count() int
}

type orgServiceImpl struct {
	fsURL            string
	v1URL            string
	concorder        concorder
	orgsRepo         orgsRepo
	combinedOrgCache map[string]*combinedOrg
	list             []string
	initialised      bool
	cacheFileName    string
	c                int
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

	if err = s.storeOrgsIDS(db); err != nil {
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

func (s *orgServiceImpl) storeOrgsIDS(db *bolt.DB) error {
	return db.Batch(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte(orgsUrlsBucket))
		if bucket == nil {
			return fmt.Errorf("Bucket %v not found!", orgsUrlsBucket)
		}
		var b bytes.Buffer
		w := bufio.NewWriter(&b)
		enc := json.NewEncoder(w)
		for _, u := range s.list {
			enc.Encode(&idEntry{ID: u})
		}
		w.Flush()
		s.list = nil
		return bucket.Put([]byte("orgs"), b.Bytes())
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
	s.list = nil
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
			if combinedOrgResult.UUID == "" {
				break
			}
			if len(s.list)%100000 == 1 {
				log.Debugf("Progress: %v", len(s.list))
			}
			s.list = append(s.list, combinedOrgResult.UUID)

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
	list, err := s.orgsRepo.orgsFromURL(listEndpoint)
	if err != nil {
		errs <- err
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

func (s *orgServiceImpl) fetchOrgFromURLThrottled(url string, concurrentGoroutines chan struct{}) (combinedOrg, error) {
	defer func(limitGoroutinesChannel chan struct{}) {
		<-limitGoroutinesChannel
	}(concurrentGoroutines)
	return s.orgsRepo.orgFromURL(url)
}

func (s *orgServiceImpl) combineOrganisations(combineOrgChan chan *combinedOrg, fsOrgs chan string, v1Orgs chan string, errs chan error, done <-chan struct{}) {
	var cWait sync.WaitGroup
	fsDone := false
	v1Done := false
	defer func() {
		close(combineOrgChan)
	}()
	var concurrentGoroutines = make(chan struct{}, 100)
	defer close(concurrentGoroutines)

	for !fsDone || !v1Done {
		select {
		case fsOrgUUID, ok := <-fsOrgs:
			if !ok {
				fsDone = true
			} else {
				cWait.Add(1)
				go func() {
					s.handleFsOrg(fsOrgUUID, combineOrgChan, errs, done, concurrentGoroutines)
					cWait.Done()
				}()
			}
		case v1OrgUUID, ok := <-v1Orgs:
			if !ok {
				v1Done = true
			} else {
				cWait.Add(1)
				go func() {
					s.handleV1Org(v1OrgUUID, combineOrgChan, errs, done, concurrentGoroutines)
					cWait.Done()
				}()
			}
		case <-done:
			return
		}
	}
	cWait.Wait()

}

func (s *orgServiceImpl) handleFsOrg(fsOrgUUID string, combineOrgChan chan *combinedOrg, errs chan error, done <-chan struct{}, concurrentGoroutines chan struct{}) {
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
		org, err = s.mergeOrgs(fsOrgUUID, v1UUID, concurrentGoroutines)
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

func (s *orgServiceImpl) handleV1Org(v1OrgUUID string, combineOrgChan chan *combinedOrg, errs chan error, done <-chan struct{}, concurrentGoroutines chan struct{}) {
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
func (s *orgServiceImpl) mergeOrgs(fsOrgUUID string, v1UUID map[string]struct{}, concurrentGoroutines chan struct{}) (combinedOrg, error) {
	var uuids []string
	concurrentGoroutines <- struct{}{}
	v2Org, err := s.fetchOrgFromURLThrottled(s.fsURL+"/"+fsOrgUUID, concurrentGoroutines)
	if err != nil {
		return combinedOrg{}, err
	}
	if v2Org.UUID == "" {
		log.Warnf("Missing organisation from fs: %v. Skipping...", fsOrgUUID)
		return combinedOrg{}, nil
	}
	if err = s.mergeIdentifiers(&v2Org, v1UUID, concurrentGoroutines); err != nil {
		return combinedOrg{}, err
	}

	for uuidString := range v1UUID {
		uuids = append(uuids, uuidString)
	}
	uuids = append(uuids, fsOrgUUID)
	canonicalUUID, _ := canonical(uuids...)
	v2Org.UUID = canonicalUUID
	return v2Org, nil
}

func (s *orgServiceImpl) mergeIdentifiers(v2Org *combinedOrg, v1UUID map[string]struct{}, concurrentGoroutines chan struct{}) error {
	var identifiers []identifier
	for _, id := range v2Org.Identifiers {
		if id.Authority != tmeIdentifier {
			identifiers = append(identifiers, id)
		}
	}
	for uuidString := range v1UUID {
		concurrentGoroutines <- struct{}{}
		v1Org, err := s.fetchOrgFromURLThrottled(s.v1URL+"/"+uuidString, concurrentGoroutines)
		if err != nil {
			return err
		}
		identifiers = append(identifiers, v1Org.Identifiers...)
	}
	v2Org.Identifiers = identifiers
	return nil
}

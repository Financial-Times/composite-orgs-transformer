package main

import (
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/boltdb/bolt"
	"github.com/gorilla/mux"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

const cacheBucket = "combinedorg"
const cacheFileName = "cache.db"
const uppIdentifier = "http://api.ft.com/system/FT-UPP"

type orgLister struct {
	fsURL            string
	v1URL            string
	concorder        concorder
	combinedOrgCache map[string]*combinedOrg
	list             []listEntry
	baseURI          string
}

// /organisations endpoint
func (orgHandler *orgLister) getAllOrgs(w http.ResponseWriter, r *http.Request) {
	if len(orgHandler.list) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	writeJSONResponse(orgHandler.list, w)
}

// /organisations/{uuid} endpoint
func (orgHandler *orgLister) getOrgByUUID(writer http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	uuid := vars["uuid"]
	db, err := bolt.Open(cacheFileName, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal(err) //TODO how to handle?
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
		log.Fatal(err)
	}
	if cachedValue == nil || len(cachedValue) == 0 {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	var org combinedOrg
	err = json.Unmarshal(cachedValue, &org)
	if err != nil {
		log.Fatal(err)
	}
	if org.UUID != uuid {
		log.Printf("Uuid %v is not the canonical one: %v", uuid, org.UUID)
		writer.Header().Add("Location", orgHandler.baseURI+"/"+org.UUID)
		writer.WriteHeader(http.StatusMovedPermanently)
		return
	}
	writeJSONResponse(org, writer)
}

func (orgHandler *orgLister) load() {
	go func() {
		db, err := bolt.Open(cacheFileName, 0600, &bolt.Options{Timeout: 1 * time.Second})
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()
		err = db.Update(func(tx *bolt.Tx) error {
			tx.DeleteBucket([]byte(cacheBucket))

			_, err = tx.CreateBucket([]byte(cacheBucket))
			if err != nil {
				return err
			}
			return nil
		})

		err = orgHandler.concorder.load()
		if err != nil {
			log.Errorf("ERROR: %+v", err) //TODO how to handle?
		}
		err = orgHandler.loadCombinedOrgs(db)
		if err != nil {
			log.Errorf("ERROR: %+v", err) //TODO how to handle?
		}
	}()
}

func (orgHandler *orgLister) loadCombinedOrgs(db *bolt.DB) error {
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
	var list []listEntry
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&list); err != nil {
		errs <- err
		return
	}
	log.Printf("Fetched %v entries for %v\n", len(list), listEndpoint)
	//count := 0
	//TODO trying to avoid server overload
	var wg sync.WaitGroup
	var maxNbConcurrentGoroutines = 10
	concurrentGoroutines := make(chan struct{}, maxNbConcurrentGoroutines)
	defer close(concurrentGoroutines)
	// Fill the dummy channel with maxNbConcurrentGoroutines empty struct.
	for i := 0; i < maxNbConcurrentGoroutines; i++ {
		concurrentGoroutines <- struct{}{}
	}
	for _, entry := range list {
		wg.Add(1)
		go fetchOrgFromURL(entry.APIURL, orgs, errs, &wg, concurrentGoroutines)
		<-concurrentGoroutines
		//TODO delete this debug section if you can handle 5 million entries
		//count++
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
	time.Sleep(50)
	retryCount := 0
	var resp *http.Response
	var err error
	for {
		resp, err = httpClient.Get(url)
		if err != nil && retryCount > 2 {
			// TODO time="2016-05-09T11:06:32+03:00" level=error msg="Oh no!
			// Get http://ftaps39408-law1a-eu-p.osb.ft.com/transformers/organisations/1fa8b7c5-60a7-3ba3-9e7f-5269fd96829d:
			// dial tcp 10.170.37.194:80: bind: An operation on a socket could not be performed because the system
			// lacked sufficient buffer space or because a queue was full."
			errs <- err
			return
		}
		if err != nil {
			log.Warnf("Error appeared to get %v, retrying\n", url)
			retryCount++
			time.Sleep(50)
			continue
		}
		break
	}
	defer resp.Body.Close()
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()
	var org combinedOrg
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&org); err != nil {
		log.Errorf("Error decoding response from %v, response: %+v\n", url, resp)
		errs <- err
		return
	}
	orgs <- org
}

func (orgHandler *orgLister) combineOrganisations(combineOrgChan chan *combinedOrg, fsOrgs chan combinedOrg, v1Orgs chan combinedOrg, errs chan error) {
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

func (orgHandler *orgLister) handleFsOrg(fsOrg combinedOrg, combineOrgChan chan *combinedOrg, errs chan error) {
	v1UUID, found, err := orgHandler.concorder.v2tov1(fsOrg.UUID)
	if err != nil {
		errs <- err
		return
	}
	if !found {
		combineOrgChan <- &fsOrg
	} else {
		combineOrgFromFs(&fsOrg, v1UUID)
		combineOrgChan <- &fsOrg
	}
}

func (orgHandler *orgLister) handleV1Org(v1Org combinedOrg, combineOrgChan chan *combinedOrg, errs chan error) {
	fsUUID, err := orgHandler.concorder.v1tov2(v1Org.UUID)
	if err != nil {
		errs <- err
		return
	}
	if fsUUID == "" {
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

	canonicalUUID, indexOfCanonicalUUID := canonical(uuids...)
	fsOrg.UUID = canonicalUUID
	for i := 0; i < len(uuids); i++ {
		if i == indexOfCanonicalUUID {
			continue
		}
		fsOrg.Identifiers = append(fsOrg.Identifiers,
			identifier{Authority: uppIdentifier, IdentifierValue: uuids[i]})

	}
}

func combineOrgFromV1(v1Org *combinedOrg, fsUUID string) {
	canonicalUUID, indexOfCanonicalUUID := canonical(v1Org.UUID, fsUUID)
	var alternateUUID string
	if indexOfCanonicalUUID == 0 {
		alternateUUID = v1Org.UUID
	} else {
		alternateUUID = fsUUID
	}

	v1Org.Identifiers = append(v1Org.Identifiers, identifier{Authority: uppIdentifier, IdentifierValue: alternateUUID})
	v1Org.UUID = canonicalUUID
}

func writeJSONResponse(obj interface{}, writer http.ResponseWriter) {
	writer.Header().Add("Content-Type", "application/json")

	enc := json.NewEncoder(writer)
	if err := enc.Encode(obj); err != nil {
		log.Errorf("Error on json encoding=%v\n", err)
		writeJSONError(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}

func writeJSONError(w http.ResponseWriter, errorMsg string, statusCode int) {
	w.WriteHeader(statusCode)
	fmt.Fprintln(w, fmt.Sprintf("{\"message\": \"%s\"}", errorMsg))
}

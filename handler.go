package main

import (
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/boltdb/bolt"
	"github.com/gorilla/mux"
	"net/http"
	"time"
)

const CACHE_BUCKET = "combinedorg"
const CACHE_FILE_NAME = "cache.db"

type orgLister struct {
	fsURL            string
	v1URL            string
	concorder        concorder
	combinedOrgCache map[string]*combinedOrg
	list             []listEntry
	baseURI          string
}

// /organisations endpoint
func (ol *orgLister) getAllOrgs(w http.ResponseWriter, r *http.Request) {
	if len(ol.list) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	writeJSONResponse(ol.list, w)
}

// /organisations/{uuid} endpoint
func (ol *orgLister) getOrgByUUID(writer http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	uuid := vars["uuid"]
	db, err := bolt.Open(CACHE_FILE_NAME, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal(err) //TODO how to handle?
	}
	defer db.Close()
	var cachedValue []byte
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(CACHE_BUCKET))
		if bucket == nil {
			return fmt.Errorf("Bucket %v not found!", CACHE_BUCKET)
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
	if org.CanonicalUuid != uuid {
		log.Printf("uuid %v is not the canonical one: %v", uuid, org.CanonicalUuid)
		writer.Header().Add("Location", ol.baseURI+"/"+org.CanonicalUuid)
		writer.WriteHeader(http.StatusMovedPermanently)
		return
	}
	log.Printf("%+v", org)
	writeJSONResponse(org, writer)
}

// /reload endpoint
func (orgHandler *orgLister) reload(writer http.ResponseWriter, r *http.Request) {
	//TODO reset before; do we actually need this endpoint?
	orgHandler.load()
	writer.WriteHeader(http.StatusAccepted)
}

func (orgHandler *orgLister) load() {
	go func() {
		db, err := bolt.Open(CACHE_FILE_NAME, 0600, &bolt.Options{Timeout: 1 * time.Second})
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()
		err = db.Update(func(tx *bolt.Tx) error {
			tx.DeleteBucket([]byte(CACHE_BUCKET))

			_, err = tx.CreateBucket([]byte(CACHE_BUCKET))
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

func (ol *orgLister) loadCombinedOrgs(db *bolt.DB) error {
	fsUUIDs := make(chan string)
	v1UUIDs := make(chan string)
	errs := make(chan error, 3)

	go fetchUUIDs(ol.fsURL, fsUUIDs, errs)
	go fetchUUIDs(ol.v1URL, v1UUIDs, errs)

	combineOrgChan := make(chan *combinedOrg)

	go func() {
		log.Printf("DEBUG - Combining results")
		ol.combineOrganisations(combineOrgChan, fsUUIDs, v1UUIDs, errs)
	}()
	for {
		select {
		case err := <-errs:
			log.Errorf("Oh no! %+v", err.Error())
			return err
		case combinedOrgResult, ok := <-combineOrgChan:
			if !ok {
				break
			}
			if len(ol.list)%5000 == 1 {
				fmt.Printf("Progress: %v", len(ol.list))
			}
			ol.list = append(ol.list, listEntry{fmt.Sprintf("%s/%s", ol.baseURI, combinedOrgResult.CanonicalUuid)})

			storeOrgToCache(db, combinedOrgResult)
		}
	}
	log.Printf("Finished composite org load: %v values. Waiting for subroutines to terminate", len(ol.list))
	return nil

}

func storeOrgToCache(db *bolt.DB, combinedOrgResult *combinedOrg) {
	err := db.Batch(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(CACHE_BUCKET))
		if bucket == nil {
			return fmt.Errorf("Bucket %v not found!", CACHE_BUCKET)
		}
		marshalledCombinedOrg, err := json.Marshal(combinedOrgResult)
		if err != nil {
			return err
		}
		err = bucket.Put([]byte(combinedOrgResult.CanonicalUuid), marshalledCombinedOrg)
		if err != nil {
			return err
		}
		if combinedOrgResult.AliasUuids != nil {
			for _, uuid := range combinedOrgResult.AliasUuids {
				err = bucket.Put([]byte(uuid), marshalledCombinedOrg)
				if err != nil {
					return err
				}
			}
		}
		return err
	})
	if err != nil {
		log.Errorf("ERROR store: %+v", err) //TODO how to handle?
	}

}

func fetchUUIDs(listEndpoint string, UUIDs chan<- string, errs chan<- error) {
	log.Printf("Starting fetching entries for %v", listEndpoint)
	resp, err := httpClient.Get(listEndpoint)
	if err != nil {
		errs <- err
		return
	}
	defer resp.Body.Close()

	var list []listEntry
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&list); err != nil {
		errs <- err
		return
	}
	log.Printf("Fetched %v entries for %v\n", len(list), listEndpoint)
	count := 0
	for _, listEntry := range list {
		uuid := uuidRegex.FindString(listEntry.ApiUrl)
		//log.Print(uuid + " ")
		UUIDs <- uuid
		//TODO delete this debug section if you can handle 5 million entries
		count++
		//if count > 5000 {
		//	break
		//}
	}

	close(UUIDs)
}

func (ol *orgLister) combineOrganisations(combineOrgChan chan *combinedOrg, fsUUIDs chan string, v1UUIDs chan string, errs chan error) {
	fsDone := false
	v1Done := false
	for fsDone != true || v1Done != true {
		select {
		case fsUUID, ok := <-fsUUIDs:
			if !ok {
				fsDone = true
			} else {
				v1UUID, found, err := ol.concorder.v2tov1(fsUUID)
				if err != nil {
					errs <- err
					return
				}
				if !found {
					combineOrgChan <- &combinedOrg{CanonicalUuid: fsUUID}
				} else {
					alternateUUIDArray := make([]string, 0)
					for uuidString := range v1UUID {
						alternateUUIDArray = append(alternateUUIDArray, uuidString)
					}
					//TODO fix this, sometimes alternative uuids are the same as canonical ones
					alternateUUIDArray = append(alternateUUIDArray, fsUUID)
					canonicalUuid, indexOfCanonicalUuid := canonical(alternateUUIDArray...)
					alternateUUIDArray = append(alternateUUIDArray[:indexOfCanonicalUuid], alternateUUIDArray[indexOfCanonicalUuid+1:]...)
					combineOrgChan <- &combinedOrg{CanonicalUuid: canonicalUuid, AliasUuids: alternateUUIDArray}
				}
			}
		case v1UUID, ok := <-v1UUIDs:
			if !ok {
				v1Done = true
			} else {
				fsUUID, err := ol.concorder.v1tov2(v1UUID)
				if err != nil {
					errs <- err
					return
				}
				if fsUUID == "" {
					combineOrgChan <- &combinedOrg{CanonicalUuid: v1UUID}
				} else {
					canonicalUuid, index := canonical(v1UUID, fsUUID)
					var alternateUuid = make([]string, 1)
					if index == 0 {
						alternateUuid[0] = v1UUID
					} else {
						alternateUuid[0] = fsUUID
					}
					combineOrgChan <- &combinedOrg{CanonicalUuid: canonicalUuid, AliasUuids: alternateUuid}
				}
			}
		}
	}
	close(combineOrgChan)
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

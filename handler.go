package main

import (
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"net/http"
)

type orgLister struct {
	fsURL            string
	v1URL            string
	concorder        concorder
	combinedOrgCache map[string]*combinedOrg
	list             []listEntry
	baseURI          string
}

func (ol *orgLister) getAllOrgs(w http.ResponseWriter, r *http.Request) {

	w.Header().Add("Content-Type", "application/json")

	if len(ol.list) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	writeJSONResponse(ol.list, w)
}

func (orgHandler *orgLister) load() {
	go func() {
		err := orgHandler.concorder.load()
		if err != nil {
			log.Errorf("ERROR: %+v", err)
		}
		err = orgHandler.loadCombinedOrgs()
		if err != nil {
			log.Errorf("ERROR: %+v", err)
		}
	}()
}

func (orgHandler *orgLister) reload(writer http.ResponseWriter, r *http.Request) {
	//TODO reset before?
	orgHandler.load()

	writer.WriteHeader(http.StatusAccepted)
}

func (ol *orgLister) loadCombinedOrgs() error {
	fsUUIDs := make(chan string)
	v1UUIDs := make(chan string)

	errs := make(chan error, 3)

	go fetchUUIDs(ol.fsURL, fsUUIDs, errs)
	go fetchUUIDs(ol.v1URL, v1UUIDs, errs)

	combineOrgChan := make(chan *combinedOrg)

	go func() {
		log.Printf("Combining results")
		ol.combineOrganisations(combineOrgChan, fsUUIDs, v1UUIDs, errs)
	}()

	for {
		select {
		case err := <-errs:
			log.Errorf("Oh no! %+v", err.Error())
			//http.Error(w, err.Error(), http.StatusInternalServerError)
			return err
		case combinedOrgResult, ok := <-combineOrgChan:
			if !ok {
				log.Printf("Finished composite org load: %v values", len(ol.list))
				return nil
			}
			//TODO remove debugging section
			if len(ol.list)%1000 == 1 {
				fmt.Printf("%v ", len(ol.list))
			}
			ol.list = append(ol.list, listEntry{fmt.Sprintf("%s/%s", ol.baseURI, combinedOrgResult.CanonicalUuid)})
			//TODO store it with bolt (or other persistent db)
			ol.combinedOrgCache[combinedOrgResult.CanonicalUuid] = combinedOrgResult
			if combinedOrgResult.AliasUuids != nil {
				for _, uuid := range combinedOrgResult.AliasUuids {
					ol.combinedOrgCache[uuid] = combinedOrgResult
				}
			}
		}
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
	//TODO delete this debug section
	//count := 0
	for _, listEntry := range list {
		uuid := uuidRegex.FindString(listEntry.ApiUrl)
		//log.Print(uuid + " ")
		UUIDs <- uuid
		//TODO delete this debug section
		//count++
		//if count > 10 {
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
	log.Println("Out of for loop")
	close(combineOrgChan)
}

func (ol *orgLister) getOrgByUUID(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	uuid := vars["uuid"]
	value, found := ol.combinedOrgCache[uuid]

	if !found {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if value.CanonicalUuid != uuid {
		log.Printf("TODO return HTTP 301")
	}
	log.Printf("%+v", *value)
	writeJSONResponse(*value, w)
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

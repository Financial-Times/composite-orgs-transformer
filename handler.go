package main

import (
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"io"
	"io/ioutil"
	"net/http"
)

type orgsHandler struct {
	service orgsService
	client  httpClient
	v2URL   string
	v1URL   string
}

func newOrgsHandler(service orgsService, client httpClient, v1URL string, v2URL string) orgsHandler {
	return orgsHandler{service: service, client: client, v1URL: v1URL, v2URL: v2URL}
}

// /organisations endpoint
func (orgHandler *orgsHandler) getAllOrgs(w http.ResponseWriter, r *http.Request) {
	if !orgHandler.service.isInitialised() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	orgUris, err := orgHandler.service.getOrgs()

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if orgUris == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Write(orgUris)
}

// /organisations/{uuid} endpoint
func (orgHandler *orgsHandler) getOrgByUUID(writer http.ResponseWriter, req *http.Request) {
	if !orgHandler.service.isInitialised() {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	vars := mux.Vars(req)
	uuid := vars["uuid"]

	org, found, err := orgHandler.service.getOrgByUUID(uuid)
	if err != nil {
		writeJSONError(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	if found {
		if org.UUID != uuid {
			log.Printf("Uuid %v is not the canonical one: %v", uuid, org.UUID)
			writer.Header().Add("Location", "/transformers/organisations/"+org.UUID)
			writer.WriteHeader(http.StatusMovedPermanently)
			return
		}
		writeJSONResponse(org, writer)
		return
	}

	//fall back to v1/v2 orgs transformers if uuid not concorded
	ok := orgHandler.streamIfSuccess(orgHandler.v2URL+"/"+uuid, writer)
	if !ok {
		ok = orgHandler.streamIfSuccess(orgHandler.v1URL+"/"+uuid, writer)
		if !ok {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
	}
}

func (orgHandler *orgsHandler) count(writer http.ResponseWriter, req *http.Request) {
	if !orgHandler.service.isInitialised() {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	fmt.Fprintf(writer, "%d", orgHandler.service.count())
}

func (orgHandler *orgsHandler) streamIfSuccess(url string, writer http.ResponseWriter) bool {
	resp, err := orgHandler.client.Get(url)
	if err != nil {
		writeJSONError(writer, err.Error(), http.StatusInternalServerError)
		return true
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()
	if resp.StatusCode == http.StatusOK {
		streamJSONResponse(resp.Body, writer)
		return true
	}
	if resp.StatusCode != http.StatusNotFound {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return true
	}
	return false
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

func streamJSONResponse(reader io.Reader, writer http.ResponseWriter) {
	writer.Header().Add("Content-Type", "application/json")
	io.Copy(writer, reader)
}

func writeJSONError(w http.ResponseWriter, errorMsg string, statusCode int) {
	w.WriteHeader(statusCode)
	fmt.Fprintln(w, fmt.Sprintf("{\"message\": \"%s\"}", errorMsg))
}

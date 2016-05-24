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
	client  *http.Client
}

func newOrgsHandler(service orgsService, client *http.Client) orgsHandler {
	return orgsHandler{service: service, client: client}
}

// /organisations endpoint
func (orgHandler *orgsHandler) getAllOrgs(w http.ResponseWriter, r *http.Request) {
	if !orgHandler.service.isInitialised() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	list, found := orgHandler.service.getOrgs()

	if !found {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	writeJSONResponse(list, w)
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
			writer.Header().Add("Location", orgHandler.service.getBaseURI()+"/"+org.UUID)
			writer.WriteHeader(http.StatusMovedPermanently)
			return
		}
		writeJSONResponse(org, writer)
		return
	}

	//fall back to v1/v2 orgs transformers if uuid not concorded
	ok := orgHandler.streamIfSuccess("https://v2-organisations-transformer-up.ft.com/transformers/organisations/"+uuid, writer)
	if !ok {
		ok = orgHandler.streamIfSuccess("http://localhost:8081/transformers/organisations/"+uuid, writer)
		if !ok {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
	}
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

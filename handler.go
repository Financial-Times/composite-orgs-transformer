package main

import (
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"net/http"
)

type orgsHandler struct {
	service orgsService
}

func newOrgsHandler(service orgsService) orgsHandler {
	return orgsHandler{service: service}
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
	}
	if !found {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	if org.UUID != uuid {
		log.Printf("Uuid %v is not the canonical one: %v", uuid, org.UUID)
		writer.Header().Add("Location", orgHandler.service.getBaseURI()+"/"+org.UUID)
		writer.WriteHeader(http.StatusMovedPermanently)
		return
	}
	writeJSONResponse(org, writer)
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

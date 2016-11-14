package main

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
)

const knownUUID = "c7e492d9-b8f1-4318-aed8-8103df4e42a9"
const alternativeUUID = "6a5a939c-e166-4a38-b114-23fcd6cb1cf1"

type mockOrgsService struct {
	canonicalUuid    string
	nonCanonicalUuid string
	isFound          bool
	actualUUID       string
	err              error
	organisation     combinedOrg
}

type mockHttpClient struct {
}

func (httpClient mockHttpClient) Get(url string) (resp *http.Response, err error) {
	r := http.Response{}
	r.Body = ioutil.NopCloser(bytes.NewReader([]byte("foo")))
	r.StatusCode = http.StatusOK
	return &r, nil
}

type test struct {
	name            string
	req             *http.Request
	mockOrgsService orgsService
	mockHttpClient  httpClient
	statusCode      int
	body            string
	headers         map[string]string
}

func (service mockOrgsService) getOrgByUUID(uuid string) (organisation combinedOrg, found bool, err error) {
	returnOrg := combinedOrg{UUID: service.canonicalUuid, AlternativeIdentifiers: alternativeIdentifiers{Uuids: []string{service.canonicalUuid, service.nonCanonicalUuid}}}
	service.actualUUID = uuid
	return returnOrg, service.isFound, service.err
}

func (service mockOrgsService) getOrgs() ([]byte, error) {
	return make([]byte, 1), nil
}

func (service mockOrgsService) isInitialised() bool {
	return true
}

func (service mockOrgsService) count() int {
	return 1
}

func (service mockOrgsService) checkConnectivity() error {
	return nil
}

func TestGetHandler(t *testing.T) {
	assert := assert.New(t)
	tests := []test{
		{"Success",
			newRequest("GET", fmt.Sprintf("/transformers/organisations/%s", knownUUID), "application/json", nil),
			mockOrgsService{isFound: true, canonicalUuid: knownUUID, nonCanonicalUuid: alternativeUUID},
			mockHttpClient{},
			http.StatusOK,
			`{"uuid":"c7e492d9-b8f1-4318-aed8-8103df4e42a9", "type":"", "properName":"", "prefLabel":"", "alternativeIdentifiers":{"uuids":["c7e492d9-b8f1-4318-aed8-8103df4e42a9", "6a5a939c-e166-4a38-b114-23fcd6cb1cf1"]}}`,
			map[string]string{"Content-Type": "application/json"}},
		{"Redirect",
			newRequest("GET", fmt.Sprintf("/transformers/organisations/%s", alternativeUUID), "application/json", nil),
			mockOrgsService{isFound: true, canonicalUuid: knownUUID, nonCanonicalUuid: alternativeUUID},
			mockHttpClient{},
			http.StatusMovedPermanently,
			"",
			map[string]string{"Location": "/transformers/organisations/c7e492d9-b8f1-4318-aed8-8103df4e42a9"}},
		{"Not Found",
			newRequest("GET", fmt.Sprintf("/transformers/organisations/%s", "9999"), "application/json", nil),
			mockOrgsService{isFound: false},
			mockHttpClient{},
			http.StatusNotFound,
			"",
			nil},
	}

	for _, test := range tests {
		rec := httptest.NewRecorder()

		router(orgsHandler{
			test.mockOrgsService,
			test.mockHttpClient,
			"http://v1-transformer/transformers/organisations",
			"http://v2-transformer/transformers/organisations",
			"/transformers/organisations/"}).ServeHTTP(rec, test.req)
		assert.True(test.statusCode == rec.Code, fmt.Sprintf("%s: Wrong response code, was %d, should be %d", test.name, rec.Code, test.statusCode))
		if test.body != "" {
			assert.JSONEq(test.body, rec.Body.String(), fmt.Sprintf("%s: Wrong body", test.name))
		}

		for key, value := range test.headers {
			assert.Contains(rec.HeaderMap, key, fmt.Sprintf("Header map does not contain expected header: %s", key))
			assert.Equal(rec.HeaderMap.Get(key), value, fmt.Sprintf("Unexpected header value for %s, Expected was: %s but got: %s", key, test.headers[key], rec.Header().Get(key)))
		}

	}
}

func newRequest(method, url, contentType string, body []byte) *http.Request {
	req, err := http.NewRequest(method, url, bytes.NewBuffer(body))
	if err != nil {
		panic(err)
	}
	req.Header.Add("Content-Type", contentType)
	return req
}

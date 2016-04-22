package main

//TODO complete model
type combinedOrg struct {
	CanonicalUuid string   `json:"canonicalUuid"`
	AliasUuids    []string `json:"alternativeUuids,omitempty"`
}

type listEntry struct {
	ApiUrl string `json:"apiUrl"`
}

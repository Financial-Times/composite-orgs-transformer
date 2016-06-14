package main

import (
	"bytes"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"io"
	"sort"
	"strings"
	"testing"
)

const (
	v1TransURL = "http://v1-transformer/transformers/organisations"
	v2TransURL = "http://v2-transformer/transformers/organisations"

	v1UUID = "B325ED5E-41CF-37EA-A509-726FE9C0E19B"
	v2UUID = "d039dc83-eb00-3eeb-bbe8-7056d9da3058"

	concV1UUID1   = "535e0b7e-dce9-3c37-b061-6fb6ad2bdf89"
	concV1UUID2   = "3074c89d-c984-3617-b6ef-6b724add0035"
	concV2UUID    = "9eb50f88-5b6e-33f9-a3f7-30e2f3f6cc4e"
	canonicalUUID = concV1UUID2

	orgType = "Organisation"

	UPP = "http://api.ft.com/system/FT-UPP"
	TME = "http://api.ft.com/system/FT-TME"
	FS  = "http://api.ft.com/system/FACTSET"
)

type ByID []idEntry

func (s ByID) Len() int {
	return len(s)
}
func (s ByID) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s ByID) Less(i, j int) bool {
	return s[i].ID < s[j].ID
}

type ByIdentifier []identifier

func (s ByIdentifier) Len() int {
	return len(s)
}
func (s ByIdentifier) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s ByIdentifier) Less(i, j int) bool {
	return s[i].Authority+s[i].IdentifierValue < s[j].Authority+s[j].IdentifierValue
}

func TestGetOrganisations(t *testing.T) {

	con := &mockBerthaConcorder{
		uuidV1toUUIDV2: map[string]string{concV1UUID1: concV2UUID, concV1UUID2: concV2UUID},
		uuidV2toUUIDV1: map[string]map[string]struct{}{concV2UUID: map[string]struct{}{concV1UUID1: struct{}{}, concV1UUID2: struct{}{}}},
	}

	repo := &mockOrgsRepo{v1Orgs: map[string]combinedOrg{
		v1TransURL + "/" + v1UUID: combinedOrg{
			UUID:        v1UUID,
			ProperName:  "V1 Name 1",
			Type:        orgType,
			Identifiers: []identifier{identifier{Authority: UPP, IdentifierValue: v1UUID}, identifier{Authority: TME, IdentifierValue: v1UUID + "base64"}},
		},
		v1TransURL + "/" + concV1UUID1: combinedOrg{
			UUID:        concV1UUID1,
			ProperName:  "Conc V1 Name 1",
			Type:        orgType,
			Identifiers: []identifier{identifier{Authority: UPP, IdentifierValue: concV1UUID1}, identifier{Authority: TME, IdentifierValue: concV1UUID1 + "base64"}},
		},
		v1TransURL + "/" + concV1UUID2: combinedOrg{
			UUID:        concV1UUID2,
			ProperName:  "Conc V1 Name 2",
			Type:        orgType,
			Identifiers: []identifier{identifier{Authority: UPP, IdentifierValue: concV1UUID2}, identifier{Authority: TME, IdentifierValue: concV1UUID2 + "base64"}},
		},
	},
		v2Orgs: map[string]combinedOrg{
			v2TransURL + "/" + v2UUID: combinedOrg{
				UUID:        v2UUID,
				Type:        orgType,
				Identifiers: []identifier{identifier{Authority: UPP, IdentifierValue: v2UUID}, identifier{Authority: FS, IdentifierValue: v2UUID + "base64"}},
			},
			v2TransURL + "/" + concV2UUID: combinedOrg{
				UUID:        concV1UUID2,
				Type:        orgType,
				Identifiers: []identifier{identifier{Authority: UPP, IdentifierValue: concV2UUID}, identifier{Authority: FS, IdentifierValue: concV2UUID + "base64"}},
			},
		}}

	orgService := &orgServiceImpl{
		fsURL:            v2TransURL,
		v1URL:            v1TransURL,
		concorder:        con,
		orgsRepo:         repo,
		combinedOrgCache: make(map[string]*combinedOrg),
		initialised:      false,
		cacheFileName:    "test.db",
	}

	err := orgService.load()
	assert.Equal(t, nil, err, "Error should be nil")

	//list entries should contain only canonical uuids
	orgs, _ := orgService.getOrgs()
	var entries []idEntry
	dec := json.NewDecoder(bytes.NewReader(orgs))
	for {
		var ie idEntry
		err = dec.Decode(&ie)
		if err != nil {
			if err == io.EOF {
				break
			}
		}
		entries = append(entries, ie)
	}

	sort.Sort(ByID(entries))
	assert.EqualValues(t, []idEntry{idEntry{ID: canonicalUUID}, idEntry{ID: v1UUID}, idEntry{ID: v2UUID}}, entries, "List entries should contain only canonical uuids")

	//not concorded orgs should not be found
	v1Org, found, _ := orgService.getOrgByUUID(v1UUID)
	assert.Equal(t, combinedOrg{}, v1Org, "Org should be empty")
	assert.Equal(t, false, found, "Non concorded org should not be found")

	v2Org, found, _ := orgService.getOrgByUUID(v2UUID)
	assert.Equal(t, combinedOrg{}, v2Org, "Org should be empty")
	assert.Equal(t, false, found, "Non concorded org should not be found")

	for _, u := range []string{concV1UUID1, concV1UUID2, concV2UUID} {
		concordedOrg, found, _ := orgService.getOrgByUUID(u)
		assert.Equal(t, true, found, "Concorded org should be found")
		sort.Sort(ByIdentifier(concordedOrg.Identifiers))
		assert.EqualValues(t, combinedOrg{
			UUID: canonicalUUID,
			Type: orgType,
			Identifiers: []identifier{
				identifier{Authority: FS, IdentifierValue: concV2UUID + "base64"},
				identifier{Authority: TME, IdentifierValue: concV1UUID2 + "base64"},
				identifier{Authority: TME, IdentifierValue: concV1UUID1 + "base64"},
				identifier{Authority: UPP, IdentifierValue: concV1UUID2},
				identifier{Authority: UPP, IdentifierValue: concV1UUID1},
				identifier{Authority: UPP, IdentifierValue: concV2UUID},
			},
		}, concordedOrg, "Concorded org should have v1 and v2 identifiers")
	}
}

type mockBerthaConcorder struct {
	uuidV2toUUIDV1 map[string]map[string]struct{}
	uuidV1toUUIDV2 map[string]string
}

func (b *mockBerthaConcorder) v1tov2(uuid string) (string, error) {

	return b.uuidV1toUUIDV2[uuid], nil
}
func (b *mockBerthaConcorder) v2tov1(uuid string) (map[string]struct{}, bool, error) {
	value, found := b.uuidV2toUUIDV1[uuid]
	return value, found, nil
}

func (b *mockBerthaConcorder) load() error {
	return nil
}

type mockOrgsRepo struct {
	v1Orgs map[string]combinedOrg
	v2Orgs map[string]combinedOrg
}

func (r *mockOrgsRepo) orgsFromURL(u string) ([]listEntry, error) {
	if strings.Contains(u, v1TransURL) {
		return listEntries(r.v1Orgs)
	}
	return listEntries(r.v2Orgs)
}

func listEntries(orgs map[string]combinedOrg) ([]listEntry, error) {
	var entries []listEntry
	for k := range orgs {
		entries = append(entries, listEntry{APIURL: k})
	}
	return entries, nil
}

func (r *mockOrgsRepo) orgFromURL(u string) (combinedOrg, error) {
	if strings.Contains(u, v1TransURL) {
		return r.v1Orgs[u], nil
	}
	return r.v2Orgs[u], nil
}

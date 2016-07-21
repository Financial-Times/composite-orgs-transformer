package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
)

type berthaConcorder struct {
	client         httpClient
	loaded         bool
	lk             sync.Mutex
	uuidV2toUUIDV1 map[string]map[string]struct{}
	uuidV1toUUIDV2 map[string]string
	berthaURL string
}

//Concordance model
type Concordance struct {
	TMEID  string `json:"tmeid"`
	V2UUID string `json:"v2uuid"`
}

func (b *berthaConcorder) v1tov2(uuid string) (string, error) {
	b.lk.Lock()
	defer b.lk.Unlock()

	if !b.loaded {
		return "", errors.New("concordance not loaded yet")
	}
	return b.uuidV1toUUIDV2[uuid], nil
}
func (b *berthaConcorder) v2tov1(uuid string) (map[string]struct{}, bool, error) {
	b.lk.Lock()
	defer b.lk.Unlock()

	if !b.loaded {
		return nil, false, errors.New("concordance not loaded yet")
	}
	value, found := b.uuidV2toUUIDV1[uuid]
	return value, found, nil
}

func (b *berthaConcorder) load() error {
	b.lk.Lock()
	defer b.lk.Unlock()
	resp, err := b.client.Get(b.berthaURL)
	if err != nil {
		errMsg := fmt.Sprintf("Error while retrieving concordances: %v", err.Error())
		return errors.New(errMsg)
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		errMsg := fmt.Sprintf("Bertha responded with status: %d", resp.StatusCode)
		return errors.New(errMsg)
	}

	var concordances []Concordance
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	json.Unmarshal(body, &concordances)

	for _, con := range concordances {
		if isIncomplete(con) {
			continue
		}
		v1uuid := v1ToUUID(con.TMEID)
		v2uuid := con.V2UUID
		uuidSet, found := b.uuidV2toUUIDV1[v2uuid]
		if !found {
			uuidSet = make(map[string]struct{})
		}
		uuidSet[v1uuid] = struct{}{}
		b.uuidV2toUUIDV1[v2uuid] = uuidSet
		b.uuidV1toUUIDV2[v1uuid] = v2uuid
	}

	b.loaded = true
	return nil
}

func isIncomplete(con Concordance) bool {
	return con.TMEID == "" || con.V2UUID == ""
}

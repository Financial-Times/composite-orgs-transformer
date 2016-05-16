package main

import (
	"errors"
	log "github.com/Sirupsen/logrus"
	"github.com/tealeg/xlsx"
	"sync"
)

type xlsxConcorder struct {
	loaded         bool
	filename       string
	lk             sync.Mutex
	uuidV2toUUIDV1 map[string]map[string]struct{}
	uuidV1toUUIDV2 map[string]string
}

func (c *xlsxConcorder) v1tov2(uuid string) (string, error) {
	c.lk.Lock()
	defer c.lk.Unlock()

	if !c.loaded {
		return "", errors.New("concordance not loaded yet")
	}
	return c.uuidV1toUUIDV2[uuid], nil
}
func (c *xlsxConcorder) v2tov1(uuid string) (map[string]struct{}, bool, error) {
	c.lk.Lock()
	defer c.lk.Unlock()

	if !c.loaded {
		return nil, false, errors.New("concordance not loaded yet")
	}
	value, found := c.uuidV2toUUIDV1[uuid]
	return value, found, nil
}

func (c *xlsxConcorder) load() error {
	c.lk.Lock()
	defer c.lk.Unlock()

	log.Printf("Loading concordances from file: %v", c.filename)

	xlFile, err := xlsx.OpenFile(c.filename)
	if err != nil {
		return err
	}
	var count = 0
	for _, sheet := range xlFile.Sheets {
		for _, row := range sheet.Rows {
			//TODO maybe we want other information as well
			compositeID, err := row.Cells[0].String()
			if err != nil {
				return err
			}
			fsID, err := row.Cells[4].String()
			if err != nil {
				return err
			}

			v1uuid := v1ToUUID(compositeID)
			v2uuid := v2ToUUID(fsID)
			uuidSet, found := c.uuidV2toUUIDV1[v2uuid]
			if !found {
				uuidSet = make(map[string]struct{})
			}
			uuidSet[v1uuid] = struct{}{}
			c.uuidV2toUUIDV1[v2uuid] = uuidSet
			c.uuidV1toUUIDV2[v1uuid] = v2uuid
			count++
		}
	}

	c.loaded = true
	log.Printf("Finished loading concordances: %v values", count)
	return nil
}

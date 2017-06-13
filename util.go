package main

import (
	"github.com/pborman/uuid"
)

func v1ToUUID(v1id string) string {
	return uuid.NewMD5(uuid.UUID{}, []byte(v1id)).String()
}

func canonical(uuids ...string) (c string, index int) {
	for idx, s := range uuids {
		if c == "" || s < c {
			c = s
			index = idx
		}
	}
	return
}

func canonicalFromList(uuids []string) (c string, index int) {
	for idx, s := range uuids {
		if c == "" || s < c {
			c = s
			index = idx
		}
	}
	return
}

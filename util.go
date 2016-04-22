package main

import (
	"crypto/md5"
	"github.com/pborman/uuid"
)

var hasher = md5.New()
var emptyUUID = uuid.UUID{}

func v1ToUUID(v1id string) string {
	return uuid.NewMD5(uuid.UUID{}, []byte(v1id)).String()
}

func v2ToUUID(fsid string) string {
	md5data := md5.Sum([]byte(fsid))
	hasher.Reset()
	return uuid.NewHash(hasher, emptyUUID, md5data[:], 3).String()
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

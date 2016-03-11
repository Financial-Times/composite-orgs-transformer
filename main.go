package main

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jawher/mow.cli"
	"github.com/pborman/uuid"
	"github.com/tealeg/xlsx"
	"log"
	"net"
	"net/http"
	"os"
	"regexp"
	"sync"
	"time"
)

func main() {
	app := cli.App("composite-organisations-transformer", "A RESTful API for transforming combined organisations")
	concordanceFile := app.String(cli.StringOpt{
		Name:  "concordance-xlsx",
		Value: "",
		Desc:  "Filename for concordance xlsx",
	})
	v1URL := app.String(cli.StringOpt{
		Name:  "v1-transformer-url",
		Value: "",
		Desc:  "URL for v1 organisations transformer",
	})
	fsURL := app.String(cli.StringOpt{
		Name:  "fs-transformer-url",
		Value: "",
		Desc:  "URL for factset organisations transformer",
	})

	app.Action = func() {
		if err := runApp(*concordanceFile, *v1URL, *fsURL); err != nil {
			log.Fatal(err)
		}
	}

	app.Run(os.Args)
}

func runApp(concordanceFile, v1URL, fsURL string) error {
	if concordanceFile == "" {
		return errors.New("concordance file must be provided")
	}
	if v1URL == "" {
		return errors.New("v1 Organisation transformer URL must be provided")
	}
	if fsURL == "" {
		return errors.New("Factset Organisation transformer URL must be provided")
	}

	con := &xlsxConcorder{
		filename:       concordanceFile,
		uuidV1toUUIDV2: make(map[string]string),
		uuidV2toUUIDV1: make(map[string]string),
	}

	go con.load()

	ol := &orgLister{
		fsURL: fsURL,
		v1URL: v1URL,
		con:   con,
	}

	http.HandleFunc("/organisations", ol.listHandler)
	http.ListenAndServe(":8080", nil)

	return nil
}

type orgLister struct {
	fsURL string
	v1URL string
	con   concorder
}

type concorder interface {
	v1tov2(string) (string, error)
	v2tov1(string) (string, error)
}

type xlsxConcorder struct {
	loaded         bool
	filename       string
	lk             sync.Mutex
	uuidV2toUUIDV1 map[string]string
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
func (c *xlsxConcorder) v2tov1(uuid string) (string, error) {
	c.lk.Lock()
	defer c.lk.Unlock()

	if !c.loaded {
		return "", errors.New("concordance not loaded yet")
	}
	return c.uuidV2toUUIDV1[uuid], nil
}

func (c *xlsxConcorder) load() error {
	c.lk.Lock()
	defer c.lk.Unlock()

	log.Println("loading concordances")

	xlFile, err := xlsx.OpenFile(c.filename)
	if err != nil {
		return err
	}

	for _, sheet := range xlFile.Sheets {
		for _, row := range sheet.Rows {

			compositeID, err := row.Cells[0].String()
			if err != nil {
				log.Fatal(err)
			}
			fsId, err := row.Cells[4].String()
			if err != nil {
				return err
			}

			v1uuid := v1ToUUID(compositeID)
			v2uuid := v2ToUUID(fsId)
			c.uuidV2toUUIDV1[v2uuid] = v1uuid
			c.uuidV1toUUIDV2[v1uuid] = v2uuid
		}
	}

	c.loaded = true
	log.Println("finished loading concordances")
	return nil
}

func (ol *orgLister) listHandler(w http.ResponseWriter, r *http.Request) {
	fsUUIDs := make(chan string)
	v1UUIDs := make(chan string)

	errs := make(chan error, 3)

	go fetchUUIDs(ol.fsURL, fsUUIDs, errs)
	go fetchUUIDs(ol.v1URL, v1UUIDs, errs)

	combinedUUIDs := make(chan string)

	go func() {
		for fsUUIDs != nil && v1UUIDs == nil {
			select {
			case fsUUID, ok := <-fsUUIDs:
				if !ok {
					fsUUIDs = nil
				} else {
					v1UUID, err := ol.con.v2tov1(fsUUID)
					if err != nil {
						errs <- err
						return
					}
					if v1UUID == "" {
						combinedUUIDs <- fsUUID
					} else {
						combinedUUIDs <- canonical(v1UUID, fsUUID)
					}
				}
			case v1UUID, ok := <-v1UUIDs:
				if !ok {
					v1UUIDs = nil
				} else {
					fsUUID, err := ol.con.v1tov2(v1UUID)
					if err != nil {
						errs <- err
						return
					}
					if fsUUID == "" {
						combinedUUIDs <- v1UUID
					} else {
						combinedUUIDs <- canonical(v1UUID, fsUUID)
					}
				}
			}
		}
	}()

	var list []listEntry

	for {
		select {
		case err := <-errs:
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		case u, ok := <-combinedUUIDs:
			if !ok {
				return
			}
			list = append(list, listEntry{fmt.Sprintf("%s/%s\n", r.RequestURI, u)})
		}
	}

}

func v1ToUUID(v1id string) string {
	return uuid.NewMD5(uuid.UUID{}, []byte(v1id)).String()
}

var indHasher = md5.New()
var hasher = md5.New()
var emptyUUID = uuid.UUID{}

func v2ToUUID(fsid string) string {
	md5data := md5.Sum([]byte(fsid))
	hasher.Reset()
	return uuid.NewHash(hasher, emptyUUID, md5data[:], 3).String()
}

type listEntry struct {
	APIURL string `json:apiUrl`
}

func fetchUUIDs(listEndpoint string, UUIDs chan<- string, errs chan<- error) {

	resp, err := httpClient.Get(listEndpoint)
	if err != nil {
		errs <- err
		return
	}
	defer resp.Body.Close()

	var list []listEntry
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&list); err != nil {
		errs <- err
		return
	}

	for _, listEntry := range list {
		UUIDs <- uuidRegex.FindString(listEntry.APIURL)
	}

	close(UUIDs)
}

var uuidRegex = regexp.MustCompile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

func canonical(uuids ...string) (c string) {
	for _, s := range uuids {
		if c == "" || s < c {
			c = s
		}
	}
	return
}

var httpClient = &http.Client{
	Transport: &http.Transport{
		MaxIdleConnsPerHost: 32,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
	},
}

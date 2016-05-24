package main

import (
	"errors"
	"fmt"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	"github.com/rcrowley/go-metrics"
	"net"
	"net/http"
	"os"
	"time"
)

type concorder interface {
	v1tov2(string) (string, error)
	v2tov1(string) (map[string]struct{}, bool, error)
	load() error
}

var httpClient = &http.Client{
	Transport: &http.Transport{
		MaxIdleConnsPerHost: 512,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
	},
}

func init() {
	log.SetFormatter(new(log.JSONFormatter))
	log.SetLevel(log.DebugLevel)
}

func main() {
	app := cli.App("composite-organisations-transformer", "A RESTful API for transforming combined organisations")
	v1URL := app.String(cli.StringOpt{
		Name:   "v1-transformer-url",
		Value:  "",
		Desc:   "URL for v1 organisations transformer",
		EnvVar: "V1_TRANSFORMER_URL",
	})
	fsURL := app.String(cli.StringOpt{
		Name:   "fs-transformer-url",
		Value:  "",
		Desc:   "URL for factset organisations transformer",
		EnvVar: "FS_TRANSFORMER_URL",
	})
	port := app.Int(cli.IntOpt{
		Name:   "port",
		Value:  8080,
		Desc:   "Port to listen on",
		EnvVar: "PORT",
	})
	baseURL := app.String(cli.StringOpt{
		Name:   "base-url",
		Value:  "http://localhost:8080/organisations",
		Desc:   "Base url",
		EnvVar: "BASE_URL",
	})

	app.Action = func() {
		if err := runApp(*v1URL, *fsURL, *port, *baseURL); err != nil {
			log.Fatal(err)
		}
		log.Println("Started app")
	}

	app.Run(os.Args)
}

func runApp(v1URL, fsURL string, port int, baseURL string) error {
	if v1URL == "" {
		return errors.New("v1 Organisation transformer URL must be provided")
	}
	if fsURL == "" {
		return errors.New("Factset Organisation transformer URL must be provided")
	}

	con := &berthaConcorder{
		client:         httpClient,
		uuidV1toUUIDV2: make(map[string]string),
		uuidV2toUUIDV1: make(map[string]map[string]struct{}),
	}

	orgService := &orgServiceImpl{
		fsURL:            fsURL,
		v1URL:            v1URL,
		concorder:        con,
		combinedOrgCache: make(map[string]*combinedOrg),
		baseURI:          baseURL,
		initialised:      false,
	}

	go orgService.load()
	router := mux.NewRouter()

	orgHandler := newOrgsHandler(orgService, httpClient, v1URL, fsURL)

	router.HandleFunc("/transformers/organisations", orgHandler.getAllOrgs).Methods("GET")
	router.HandleFunc("/transformers/organisations/{uuid}", orgHandler.getOrgByUUID).Methods("GET")
	http.Handle("/", router)
	err := http.ListenAndServe(fmt.Sprintf(":%d", port), httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry,
		httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), router)))

	return err
}

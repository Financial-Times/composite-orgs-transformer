package main

import (
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	"github.com/rcrowley/go-metrics"
	"github.com/sethgrid/pester"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"time"
	"github.com/Financial-Times/go-fthealth/v1a"
	status "github.com/Financial-Times/service-status-go/httphandlers"
)

type concorder interface {
	v1tov2(string) (string, error)
	v2tov1(string) (map[string]struct{}, bool, error)
	load() error
}

type httpClient interface {
	Get(url string) (resp *http.Response, err error)
}

func init() {
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
	cacheFileName := app.String(cli.StringOpt{
		Name:   "cache-file-name",
		Value:  "cache.db",
		Desc:   "Cache file name",
		EnvVar: "CACHE_FILE_NAME",
	})
	berthaURL := app.String(cli.StringOpt{
		Name:   "bertha-url",
		Value:  "https://bertha.ig.ft.com/view/publish/gss/1k7GHf3311hyLBsNgoocRRkHs7pIhJit0wQVReFfD_6w/orgs",
		Desc:   "Bertha URL to concordance file",
		EnvVar: "BERTHA_URL",
	})

	app.Action = func() {
		if err := runApp(*v1URL, *fsURL, *port, *cacheFileName, *berthaURL); err != nil {
			log.Fatal(err)
		}
		log.Println("Started app")
	}

	app.Run(os.Args)
}

func runApp(v1URL, fsURL string, port int, cacheFile string, berthaURL string) error {
	if v1URL == "" {
		return errors.New("v1 Organisation transformer URL must be provided")
	}
	if fsURL == "" {
		return errors.New("Factset Organisation transformer URL must be provided")
	}

	httpClient := newResilientClient()

	con := &berthaConcorder{
		client:         httpClient,
		uuidV1toUUIDV2: make(map[string]string),
		uuidV2toUUIDV1: make(map[string]map[string]struct{}),
		berthaURL: berthaURL,
	}

	repo := &httpOrgsRepo{client: httpClient}

	orgService := &orgServiceImpl{
		fsURL:            fsURL,
		v1URL:            v1URL,
		concorder:        con,
		orgsRepo:         repo,
		combinedOrgCache: make(map[string]*combinedOrg),
		initialised:      false,
		cacheFileName:    cacheFile,
	}

	if err := orgService.init(); err != nil {
		panic(err)
	}
	defer orgService.shutdown()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		for {
			if err := orgService.load(); err != nil {
				log.Errorf("Error while initializing orgsService. Retrying. (%v)", err)
				time.Sleep(60 * time.Second)
			} else {
				break
			}
		}
	}()
	router := mux.NewRouter()

	orgHandler := newOrgsHandler(orgService, httpClient, v1URL, fsURL)

	// The top one of these feels more correct, but the lower one matches what we have in Dropwizard,
	// so it's what apps expect currently same as ping
	router.HandleFunc(status.PingPath, status.PingHandler)
	router.HandleFunc(status.PingPathDW, status.PingHandler)
	router.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)
	router.HandleFunc(status.BuildInfoPathDW, status.BuildInfoHandler)
	router.HandleFunc("/__health", v1a.Handler("Topics Transformer Healthchecks", "Checks for accessing TME", orgHandler.HealthCheck()))
	router.HandleFunc("/__gtg", orgHandler.GoodToGo)

	router.HandleFunc("/transformers/organisations/{uuid:([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})}", orgHandler.getOrgByUUID).Methods("GET")
	router.HandleFunc("/transformers/organisations/__count", orgHandler.count).Methods("GET")
	router.HandleFunc("/transformers/organisations/__ids", orgHandler.getAllOrgs).Methods("GET")

	var h http.Handler = router
	h = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), h)
	h = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, h)

	http.Handle("/", h)

	errs := make(chan error)
	go func() {
		errs <- http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	}()

	select {
	case err := <-errs:
		return err
	case <-c:
		log.Infoln("caught signal - exiting")
		return nil
	}
}

func newResilientClient() *pester.Client {
	tr := &http.Transport{
		MaxIdleConnsPerHost: 128,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
	}
	c := &http.Client{
		Transport: tr,
		Timeout:   30 * time.Second,
	}
	client := pester.NewExtendedClient(c)
	client.MaxRetries = 5
	client.Concurrency = 1
	return client
}

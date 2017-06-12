package main

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"time"

	"github.com/Financial-Times/go-fthealth/v1a"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	"github.com/sethgrid/pester"
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

	redirectLocationURL := app.String(cli.StringOpt{
		Name:   "redirect-base-url",
		Value:  "/transformers/organisations/",
		Desc:   "Redirect url",
		EnvVar: "REDIRECT_BASE_URL",
	})

	app.Action = func() {
		if err := runApp(*v1URL, *fsURL, *port, *cacheFileName, *berthaURL, *redirectLocationURL); err != nil {
			log.Fatal(err)
		}
		log.Println("Started app")
	}

	app.Run(os.Args)
}

func runApp(v1URL, fsURL string, port int, cacheFile string, berthaURL string, redirectURL string) error {
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
		berthaURL:      berthaURL,
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

	orgHandler := newOrgsHandler(orgService, httpClient, v1URL, fsURL, redirectURL)
	r := router(orgHandler)

	http.Handle("/", r)

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

func router(hh orgsHandler) http.Handler {
	servicesRouter := mux.NewRouter()
	// The top one of these feels more correct, but the lower one matches what we have in Dropwizard,
	// so it's what apps expect currently same as ping
	servicesRouter.HandleFunc(status.PingPath, status.PingHandler)
	servicesRouter.HandleFunc(status.PingPathDW, status.PingHandler)
	servicesRouter.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)
	servicesRouter.HandleFunc(status.BuildInfoPathDW, status.BuildInfoHandler)
	servicesRouter.HandleFunc("/__health", v1a.Handler("Composite Org Transformer Healthchecks", "Checks for the health of the service", hh.HealthCheck()))
	servicesRouter.HandleFunc("/__gtg", hh.GoodToGo)

	servicesRouter.HandleFunc("/transformers/organisations/{uuid:(?:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})}", hh.getOrgByUUID).Methods("GET")
	servicesRouter.HandleFunc("/transformers/organisations/__count", hh.count).Methods("GET")
	servicesRouter.HandleFunc("/transformers/organisations/__ids", hh.getAllOrgs).Methods("GET")

	return servicesRouter
}

func newResilientClient() *pester.Client {
	tr := &http.Transport{
		MaxIdleConnsPerHost: 128,
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

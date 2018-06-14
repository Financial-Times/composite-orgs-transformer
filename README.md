*DECOMISSIONED*
See [Aggregate Concept Transformer](https://github.com/Financial-Times/aggregate-concept-transformer) instead

# Composite Organisations transformer
[![Circle CI](https://circleci.com/gh/Financial-Times/composite-orgs-transformer/tree/master.png?style=shield)](https://circleci.com/gh/Financial-Times/composite-orgs-transformer/tree/master)

Fetches data from [v1-orgs-transformer](https://github.com/Financial-Times/v1-orgs-transformer) and [(v2-)orgs-transformer](http://git.svc.ft.com/projects/CP/repos/org-transformer/browse) and combines them via concordance data obtained [via Bertha](https://bertha.ig.ft.com/view/publish/gss/1k7GHf3311hyLBsNgoocRRkHs7pIhJit0wQVReFfD_6w/orgs).

At startup an initialization process is happening, when the app gets the concordance data via Bertha, and stores it in cache.
Furthermore retrieves all the uuids from both v1-orgs-transformer and v2-orgs-transformer and caches it locally in an embedded boltdb.
The organisations, which are present in the cached concordance data, are combined and cached in boltdb. Requests to these organisations are served from boltdb, whilst requests to non-concorded organisations are forwarded to v1- or v2-orgs transformer to retrieve data.

During the initialization process the app responds to any application specific endpoint request with **HTTP 503**.

# Endpoints   
   Application specific endpoints:
   
   * /transformers/organisations/{uuid}
   * /transformers/organisations/__count
   * /transformers/organisations/__ids
   
Admin specific endpoints:
   
   * /ping
   * /build-info
   * /__ping
   * /__build-info
   * /__health
   * /__gtg
   
# Usage

`go get -u github.com/Financial-Times/composite-orgs-transformer`

### Run in bash

`composite-orgs-transformer --v1-transformer-url="https://user:password@pub-pre-prod-up.ft.com/__v1-orgs-transformer/transformers/organisations" --fs-transformer-url="https://v2-organisations-transformer-up.ft.com/transformers/organisations" --cache-file-name="cache.db"`

### Run with Docker

`docker build -t coco/composite-orgs-transformer .`
`docker run -p 8080 \
    --env "V1_TRANSFORMER_URL=http://user:password@pub-pre-prod-up.ft.com/__v1-orgs-transformer/transformers/organisations" \
    --env "FS_TRANSFORMER_URL=https://v2-organisations-transformer-up.ft.com/transformers/organisations"  \
    --env "CACHE_FILE_NAME=cache.db" \
    coco/composite-orgs-transformer`

# Examples

### Request to organisation existing ONLY in v2

`curl https://user:password@pub-pre-prod-up.ft.com/__composite-orgs-transformer/transformers/organisations/f7a4f045-d55c-338d-b1b6-4d92c2e7d30c`

```
{
    "uuid": "f7a4f045-d55c-338d-b1b6-4d92c2e7d30c",
    "type": "Organisation",
    "properName": "DFS Furniture Holdings Plc",
    "prefLabel": "DFS Furniture Holdings Plc",
    "shortName": "DFS Furniture Holdings",
    "hiddenLabel": "DFS FURNITURE HOLDINGS PLC",
    "formerNames": [
        "DFS Furniture Holdings Ltd.",
        "Diamond Bidco Ltd."
    ],
    "aliases": [
        "DFS Furniture Holdings",
        "DFS FURNITURE HOLDINGS PLC",
        "Diamond Bidco Ltd.",
        "DFS Furniture Holdings Ltd.",
        "DFS Furniture Holdings Plc"
    ],
    "parentOrganisation": "d8061748-93da-35d8-be3e-9bc2e157aaa4",
    "alternativeIdentifiers": {
        "uuids": [
            "f7a4f045-d55c-338d-b1b6-4d92c2e7d30c"
        ],
        "factsetIdentifier": "09YTGX-E",
        "leiCode": "213800NP24NMT1XY4Z55"
    }
}
```

### Request to organisation existing ONLY in v1

`curl https://user:password@pub-pre-prod-up.ft.com/__composite-orgs-transformer/transformers/organisations/b47d4c17-bbde-3351-a58b-019d25b95052`

```
{
    "uuid": "b47d4c17-bbde-3351-a58b-019d25b95052",
    "properName": "AB System Technologies",
    "prefLabel": "AB System Technologies",
    "type": "Organisation",
    "alternativeIdentifiers": {
        "TME": [
            "TnN0ZWluX09OX0FGVE1fT05fMjAwNl8zMDQ5-T04="
        ],
        "uuids": [
            "b47d4c17-bbde-3351-a58b-019d25b95052"
        ]
    }
}
```

### Request to organisation existing in BOTH v1 & v2 (i.e. concorded)

`curl https://user:password@pub-pre-prod-up.ft.com/__composite-orgs-transformer/transformers/organisations/0e474c59-6d06-3c86-868b-6a9f1249f8e7`

```
{
    "uuid": "0e474c59-6d06-3c86-868b-6a9f1249f8e7",
    "type": "PublicCompany",
    "properName": "Chengtun Mining Group Co., Ltd.",
    "prefLabel": "Chengtun Mining Group Co., Ltd.",
    "legalName": "Chengtun Mining Group Co., Ltd.",
    "shortName": "Chengtun Mining Group",
    "hiddenLabel": "CHENGTUN MINING GROUP CO LTD",
    "formerNames": [
        "Xiamen Eagle Group Co. Ltd.",
        "Xiamen Eagle Mining Group Co., Ltd."
    ],
    "aliases": [
        "Xiamen Eagle Mining Group Co., Ltd.",
        "Chengtun Mining Group Company Ltd",
        "Chengtun Mining",
        "Chengtun Mining Group",
        "Xiamen Eagle Group Co. Ltd.",
        "Xiamen Eagle Mining Group",
        "CHENGTUN MINING GROUP CO LTD",
        "Chengtun Mining Group Company",
        "Xiamen Eagle Mining Group Co Ltd",
        "Chengtun Mining Group Co., Ltd.",
        "Chengtun Mining Group Co Ltd"
    ],
    "industryClassification": "2178330f-7cc3-3ab7-a756-bb94427700ba",
    "alternativeIdentifiers": {
        "TME": [
            "YzkxYzIwZGUtMzlhZi00YmFjLTg0ZTktMzg5ODVmYWVhZmVj-T04="
        ],
        "uuids": [
            "9ec1a844-4f46-3b8e-96ea-4d3fcd77d337",
            "0e474c59-6d06-3c86-868b-6a9f1249f8e7"
        ],
        "factsetIdentifier": "05WV40-E"
    }
}
```

Note: These fields of a combined org are brought from v2, except `alternativeIdentifiers.TME` which comes from v1; and `alternativeIdentifiers.uuids` which is the combination of v1 and v2 uuids. 

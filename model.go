package main

type combinedOrg struct {
	UUID                   string                 `json:"uuid"`
	PrimaryType            string                 `json:"type"`
	TypeHierarchy          []string               `json:"types"`
	ProperName             string                 `json:"properName"`
	PrefLabel              string                 `json:"prefLabel"`
	LegalName              string                 `json:"legalName,omitempty"`
	ShortName              string                 `json:"shortName,omitempty"`
	HiddenLabel            string                 `json:"hiddenLabel,omitempty"`
	TradeNames             []string               `json:"tradeNames,omitempty"`
	LocalNames             []string               `json:"localNames,omitempty"`
	FormerNames            []string               `json:"formerNames,omitempty"`
	Aliases                []string               `json:"aliases,omitempty"`
	IndustryClassification string                 `json:"industryClassification,omitempty"`
	ParentOrganisation     string                 `json:"parentOrganisation,omitempty"`
	AlternativeIdentifiers alternativeIdentifiers `json:"alternativeIdentifiers,omitempty"`
}

type alternativeIdentifiers struct {
	TME               []string `json:"TME,omitempty"`
	Uuids             []string `json:"uuids,omitempty"`
	FactsetIdentifier string   `json:"factsetIdentifier,omitempty"`
	LeiCode           string   `json:"leiCode,omitempty"`
}

type listEntry struct {
	APIURL string `json:"apiUrl"`
}

type idEntry struct {
	ID string `json:"id"`
}

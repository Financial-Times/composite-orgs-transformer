package main

type combinedOrg struct {
	UUID                   string       `json:"uuid"`
	Type                   string       `json:"type"`
	ProperName             string       `json:"properName"`
	LegalName              string       `json:"legalName,omitempty"`
	ShortName              string       `json:"shortName,omitempty"`
	HiddenLabel            string       `json:"hiddenLabel,omitempty"`
	TradeNames             []string     `json:"tradeNames,omitempty"`
	LocalNames             []string     `json:"localNames,omitempty"`
	FormerNames            []string     `json:"formerNames,omitempty"`
	Aliases                []string     `json:"aliases,omitempty"`
	IndustryClassification string       `json:"industryClassification,omitempty"`
	ParentOrganisation     string       `json:"parentOrganisation,omitempty"`
	Identifiers            []identifier `json:"identifiers,omitempty"`
}

type identifier struct {
	Authority       string `json:"authority"`
	IdentifierValue string `json:"identifierValue"`
}

type listEntry struct {
	APIURL string `json:"apiUrl"`
}

type idEntry struct {
	ID string `json:"id"`
}

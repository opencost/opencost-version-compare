package model

import (
	"fmt"
	"strings"
	"time"
)

type AssetsRequest struct {
	Window     string
	Aggregate  string
	Accumulate string
	Filter     string
}

func (ar AssetsRequest) QueryString() string {
	params := []string{}

	params = append(params, fmt.Sprintf("window=%s", ar.Window))
	params = append(params, fmt.Sprintf("aggregate=%s", ar.Aggregate))
	params = append(params, fmt.Sprintf("accumulate=%s", ar.Accumulate))
	params = append(params, fmt.Sprintf("filter=%s", ar.Filter))

	return fmt.Sprintf("?%s", strings.Join(params, "&"))
}

type AssetsResponse struct {
	Code int                             `json:"code"`
	Data []map[string]AssetsResponseItem `json:"data"`
}

type AssetsResponseItem struct {
	Properties *AssetsResponseItemProperties `json:"properties"`
	Labels     map[string]string             `json:"labels"`
	Window     Window                        `json:"window"`
	Start      time.Time                     `json:"start"`
	End        time.Time                     `json:"end"`
	Minutes    float64                       `json:"minutes"`
	Adjustment float64                       `json:"adjustment"`
	TotalCost  float64                       `json:"totalCost"`
}

type AssetsResponseItemProperties struct {
	Category   string `json:"category"`
	Provider   string `json:"provider"`
	Account    string `json:"account"`
	Project    string `json:"project"`
	Service    string `json:"service"`
	Cluster    string `json:"cluster"`
	Name       string `json:"name"`
	ProviderID string `json:"providerID"`
}

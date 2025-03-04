package model

import (
	"fmt"
	"strings"
)

type NetworkInsightsRequest struct {
	Accumulate string
	Aggregate  string
	Window     string
}

func (nir NetworkInsightsRequest) QueryString() string {
	params := []string{}

	params = append(params, fmt.Sprintf("accumulate=%s", nir.Accumulate))
	params = append(params, fmt.Sprintf("aggregate=%s", nir.Aggregate))
	params = append(params, fmt.Sprintf("window=%s", nir.Window))
	return fmt.Sprintf("?%s", strings.Join(params, "&"))
}

type NetworkInsightsResponse struct {
	Code int                     `json:"code"`
	Data *NetworkInsightResponse `json:"data"`
}

type NetworkInsightResponse struct {
	NetworkInsightSet []*NetworkInsightSet `json:"networkInsightSet"`
}

type NetworkInsightSet struct {
	NetworkInsights map[string]*NetworkInsight `json:"networkInsights"`
}

type NetworkInsight struct {
	NetworkCost    float64                    `json:"networkCost"`
	NetworkDetails map[string]*NetworkDetails `json:"networkDetails"`
}

type NetworkDetails struct {
	Cost float64 `json:"cost"`
}

package model

import (
	"fmt"
	"strings"
	"time"
)

type AllocationSummaryRequest struct {
	Accumulate                 string
	Aggregate                  string
	CostUnit                   string
	Filter                     string
	Idle                       string
	IdleByNode                 string
	IncludeSharedCostBreakdown string
	ShareCost                  string
	ShareIdle                  string
	ShareLabels                string
	ShareNamespaces            string
	ShareTenancyCosts          string
	Window                     string
}

func (asr AllocationSummaryRequest) QueryString() string {
	params := []string{}

	params = append(params, fmt.Sprintf("accumulate=%s", asr.Accumulate))
	params = append(params, fmt.Sprintf("aggregate=%s", asr.Aggregate))
	params = append(params, fmt.Sprintf("costUnit=%s", asr.CostUnit))
	params = append(params, fmt.Sprintf("filter=%s", asr.Filter))
	params = append(params, fmt.Sprintf("idle=%s", asr.Idle))
	params = append(params, fmt.Sprintf("idleByNode=%s", asr.IdleByNode))
	params = append(params, fmt.Sprintf("includeSharedCostBreakdown=%s", asr.IncludeSharedCostBreakdown))
	params = append(params, fmt.Sprintf("shareCost=%s", asr.ShareCost))
	params = append(params, fmt.Sprintf("shareIdle=%s", asr.ShareIdle))
	params = append(params, fmt.Sprintf("shareLabels=%s", asr.ShareLabels))
	params = append(params, fmt.Sprintf("shareNamespaces=%s", asr.ShareNamespaces))
	params = append(params, fmt.Sprintf("shareTenancyCosts=%s", asr.ShareTenancyCosts))
	params = append(params, fmt.Sprintf("window=%s", asr.Window))

	return fmt.Sprintf("?%s", strings.Join(params, "&"))
}

type AllocationSummaryResponse struct {
	Code int                           `json:"code"`
	Data AllocationSummaryResponseData `json:"data"`
}

type AllocationSummaryResponseData struct {
	Step   int64                          `json:"step"`
	Sets   []AllocationSummaryResponseSet `json:"sets"`
	Window Window                         `json:"window"`
}

type AllocationSummaryResponseSet struct {
	Items  map[string]AllocationSummaryResponseItem `json:"allocations"`
	Window Window                                   `json:"window"`
}

type AllocationSummaryResponseItems map[string]AllocationSummaryResponseItem

type AllocationSummaryResponseItem struct {
	Name                   string    `json:"name"`
	Start                  time.Time `json:"start"`
	End                    time.Time `json:"end"`
	CPUCoreRequestAverage  float64   `json:"cpuCoreRequestAverage"`
	CPUCoreUsageAverage    float64   `json:"cpuCoreUsageAverage"`
	CPUCost                float64   `json:"cpuCost"`
	CPUCostIdle            float64   `json:"cpuCostIdle"`
	GPUCost                float64   `json:"gpuCost"`
	GPUCostIdle            float64   `json:"gpuCostIdle"`
	NetworkCost            float64   `json:"networkCost"`
	LoadBalancerCost       float64   `json:"loadBalancerCost"`
	PersistentVolumeCost   float64   `json:"pvCost"`
	RAMBytesRequestAverage float64   `json:"ramByteRequestAverage"`
	RAMBytesUsageAverage   float64   `json:"ramByteUsageAverage"`
	RAMCost                float64   `json:"ramCost"`
	RAMCostIdle            float64   `json:"ramCostIdle"`
	SharedCost             float64   `json:"sharedCost"`
	TotalCost              float64   `json:"totalCost"`
	TotalEfficiency        float64   `json:"totalEfficiency"`
}

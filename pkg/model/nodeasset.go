package model

import (
	"fmt"
	"strings"
	"time"
)

type NodeAssetsRequest struct {
	Window     string
	Accumulate string
}

func (nar NodeAssetsRequest) QueryString() string {
	params := []string{
		"aggregate=name",
		`filter=assetType:"node"`,
	}

	params = append(params, fmt.Sprintf("window=%s", nar.Window))
	params = append(params, fmt.Sprintf("accumulate=%s", nar.Accumulate))

	return fmt.Sprintf("?%s", strings.Join(params, "&"))
}

type NodeAssetsResponse struct {
	Code int                                 `json:"code"`
	Data []map[string]NodeAssetsResponseItem `json:"data"`
}

type NodeAssetsResponseItem struct {
	Properties   *AssetsResponseItemProperties    `json:"properties"`
	Labels       map[string]string                `json:"labels"`
	Window       Window                           `json:"window"`
	Start        time.Time                        `json:"start"`
	End          time.Time                        `json:"end"`
	Minutes      float64                          `json:"minutes"`
	Adjustment   float64                          `json:"adjustment"`
	TotalCost    float64                          `json:"totalCost"`
	NodeType     string                           `json:"nodeType"`
	CPUCoreHours float64                          `json:"cpuCoreHours"`
	RAMByteHours float64                          `json:"ramByteHours"`
	GPUHours     float64                          `json:"GPUHours"` // sic
	CPUBreakdown *NodeAssetsResponseItemBreakdown `json:"cpuBreakdown"`
	RAMBreakdown *NodeAssetsResponseItemBreakdown `json:"ramBreakdown"`
	CPUCost      float64                          `json:"cpuCost"`
	GPUCost      float64                          `json:"gpuCost"`
	GPUCount     float64                          `json:"gpuCount"`
	RAMCost      float64                          `json:"ramCost"`
	Discount     float64                          `json:"discount"`
	Preemptible  float64                          `json:"preemptible"`
	Overhead     *NodeAssetsResponseItemOverhead  `json:"overhead"`
}

type NodeAssetsResponseItemBreakdown struct {
	Idle   float64 `json:"idle"`
	Other  float64 `json:"other"`
	System float64 `json:"system"`
	User   float64 `json:"user"`
}

type NodeAssetsResponseItemOverhead struct {
	CPUOverheadFraction  float64 `json:"CpuOverheadFraction"`  // sic
	RAMOverheadFraction  float64 `json:"RamOverheadFraction"`  // sic
	OverheadCostFraction float64 `json:"OverheadCostFraction"` // sic
}

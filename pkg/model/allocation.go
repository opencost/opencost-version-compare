package model

import (
	"fmt"
	"strings"
	"time"
)

type AllocationRequest struct {
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
	ShareSplit                 string
	ShareTenancyCosts          string
	Window                     string
}

func (ar AllocationRequest) QueryString() string {
	params := []string{}

	params = append(params, fmt.Sprintf("accumulate=%s", ar.Accumulate))
	params = append(params, fmt.Sprintf("aggregate=%s", ar.Aggregate))
	params = append(params, fmt.Sprintf("costUnit=%s", ar.CostUnit))
	params = append(params, fmt.Sprintf("filter=%s", ar.Filter))
	params = append(params, fmt.Sprintf("idle=%s", ar.Idle))
	params = append(params, fmt.Sprintf("idleByNode=%s", ar.IdleByNode))
	params = append(params, fmt.Sprintf("includeSharedCostBreakdown=%s", ar.IncludeSharedCostBreakdown))
	params = append(params, fmt.Sprintf("shareCost=%s", ar.ShareCost))
	params = append(params, fmt.Sprintf("shareIdle=%s", ar.ShareIdle))
	params = append(params, fmt.Sprintf("shareLabels=%s", ar.ShareLabels))
	params = append(params, fmt.Sprintf("shareNamespaces=%s", ar.ShareNamespaces))
	params = append(params, fmt.Sprintf("shareSplit=%s", ar.ShareSplit))
	params = append(params, fmt.Sprintf("shareTenancyCosts=%s", ar.ShareTenancyCosts))
	params = append(params, fmt.Sprintf("window=%s", ar.Window))

	return fmt.Sprintf("?%s", strings.Join(params, "&"))
}

type AllocationResponse struct {
	Code int                                 `json:"code"`
	Data []map[string]AllocationResponseItem `json:"data"`
}

type AllocationResponseItem struct {
	Name                           string                                  `json:"name"`
	Properties                     *AllocationResponseItemProperties       `json:"properties"`
	Window                         Window                                  `json:"window"`
	Start                          time.Time                               `json:"start"`
	End                            time.Time                               `json:"end"`
	CPUCoreHours                   float64                                 `json:"cpuCoreHours"`
	CPUCoreRequestAverage          float64                                 `json:"cpuCoreRequestAverage"`
	CPUCoreUsageAverage            float64                                 `json:"cpuCoreUsageAverage"`
	CPUCost                        float64                                 `json:"cpuCost"`
	CPUCostAdjustment              float64                                 `json:"cpuCostAdjustment"`
	CPUCostIdle                    float64                                 `json:"cpuCostIdle"`
	GPUHours                       float64                                 `json:"gpuHours"`
	GPUCost                        float64                                 `json:"gpuCost"`
	GPUCostAdjustment              float64                                 `json:"gpuCostAdjustment"`
	GPUCostIdle                    float64                                 `json:"gpuCostIdle"`
	NetworkTransferBytes           float64                                 `json:"networkTransferBytes"`
	NetworkReceiveBytes            float64                                 `json:"networkReceiveBytes"`
	NetworkCost                    float64                                 `json:"networkCost"`
	NetworkCrossZoneCost           float64                                 `json:"networkCrossZoneCost"`
	NetworkCrossRegionCost         float64                                 `json:"networkCrossRegionCost"`
	NetworkInternetCost            float64                                 `json:"networkInternetCost"`
	NetworkCostAdjustment          float64                                 `json:"networkCostAdjustment"`
	LoadBalancerCost               float64                                 `json:"loadBalancerCost"`
	LoadBalancerCostAdjustment     float64                                 `json:"loadBalancerCostAdjustment"`
	PersistentVolumes              AllocationResponseItemPersistentVolumes `json:"pvs"`
	PersistentVolumeCostAdjustment float64                                 `json:"pvCostAdjustment"`
	RAMByteHours                   float64                                 `json:"ramByteHours"`
	RAMBytesRequestAverage         float64                                 `json:"ramByteRequestAverage"`
	RAMBytesUsageAverage           float64                                 `json:"ramByteUsageAverage"`
	RAMCost                        float64                                 `json:"ramCost"`
	RAMCostAdjustment              float64                                 `json:"ramCostAdjustment"`
	RAMCostIdle                    float64                                 `json:"ramCostIdle"`
	SharedCost                     float64                                 `json:"sharedCost"`
	TotalCost                      float64                                 `json:"totalCost"`
	TotalEfficiency                float64                                 `json:"totalEfficiency"`
}

func (ari AllocationResponseItem) PersistentVolumeCost() float64 {
	if ari.PersistentVolumes == nil {
		return 0.0
	}

	cost := 0.0

	for _, pv := range ari.PersistentVolumes {
		cost += pv.Cost
	}

	return cost
}

type AllocationResponseItemProperties struct {
	Cluster              string            `json:"cluster"`
	Node                 string            `json:"node"`
	Container            string            `json:"container"`
	Controller           string            `json:"controller"`
	ControllerKind       string            `json:"controllerKind"`
	Namespace            string            `json:"namespace"`
	Pod                  string            `json:"pod"`
	Services             []string          `json:"services"`
	ProviderID           string            `json:"providerID"`
	Labels               map[string]string `json:"labels"`
	Annotations          map[string]string `json:"annotations"`
	NamespaceLabels      map[string]string `json:"namespaceLabels"`
	NamespaceAnnotations map[string]string `json:"namespaceAnnotations"`
}

type AllocationResponseItemPersistentVolumes map[string]AllocationResponseItemPersistentVolume

type AllocationResponseItemPersistentVolume struct {
	ByteHours  float64 `json:"byteHours"`
	Cost       float64 `json:"cost"`
	ProviderID string  `json:"providerID"`
	Adjustment float64 `json:"adjustment"`
}

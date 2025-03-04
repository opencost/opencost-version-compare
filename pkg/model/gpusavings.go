package model

import (
	"fmt"
	"strings"
)

type GPUUtilizationRequest struct {
	Aggregate string
	Window    string
}

type GPUUtilizationData struct {
	Name              string  `json:"name"`
	IsShared          bool    `json:"isShared"`
	AvgGPUUtilization float64 `json:"avgGPUUtilization"`
	MaxGPUUtilization float64 `json:"maxGPUUtilization"`
	Cost              float64 `json:"cost"`
}

type GPUUtilizationSetData struct {
	WorkloadGPUUtilizations []*GPUUtilizationData `json:"workloads"`
}

type GPUUtilizationResponse struct {
	WorkloadGPUUtilizationSets []*GPUUtilizationSetData `json:"sets"`
	Error                      string                   `json:"error"`
}

type GPUUtilizationToplineResponse struct {
	TotalItems int                   `json:"totalItems"`
	Data       []*GPUUtilizationData `json:"data"`
}

func (asr GPUUtilizationRequest) QueryString() string {
	params := []string{}
	params = append(params, fmt.Sprintf("aggregate=%s", asr.Aggregate))
	params = append(params, fmt.Sprintf("window=%s", asr.Window))

	return fmt.Sprintf("?%s", strings.Join(params, "&"))
}

type GPUContainerDetailsRequest struct {
	Aggregate          string
	Window             string
	Filter             string
	GpuContainersCount string
	NodeCount          string
}

type GPUCountDetail struct {
	Containers *int `json:"containers"`
	Nodes      *int `json:"nodes"`
}

type GPUContainerDetailsData struct {
	Name  string         `json:"name"`
	Count GPUCountDetail `json:"count"`
	Cost  float64        `json:"cost"`
}

type GPUContainerDetailsSetData struct {
	GPUContainerDetailsData []*GPUContainerDetailsData `json:"workloads"`
}

type GPUContainerDetailsResponse struct {
	GPUContainerDetailsSets []*GPUContainerDetailsSetData `json:"sets"`
	Error                   string                        `json:"error"`
}

type GPUContainerDetailsToplineResponse struct {
	TotalItems int                        `json:"totalItems"`
	Data       []*GPUContainerDetailsData `json:"data"`
}

func (asr GPUContainerDetailsRequest) QueryString() string {
	params := []string{}
	params = append(params, fmt.Sprintf("aggregate=%s", asr.Aggregate))
	params = append(params, fmt.Sprintf("filter=%s", asr.Filter))
	params = append(params, fmt.Sprintf("window=%s", asr.Window))
	params = append(params, fmt.Sprintf("gpuContainersCount=%s", asr.GpuContainersCount))
	params = append(params, fmt.Sprintf("nodeCount=%s", asr.NodeCount))

	return fmt.Sprintf("?%s", strings.Join(params, "&"))
}

package model

import (
	"fmt"
	"strings"
)

type DiskAssetsRequest struct {
	Window     string
	Accumulate string
}

func (pvar DiskAssetsRequest) QueryString() string {
	params := []string{
		"aggregate=name",
		`filter=assetType:"disk"`,
	}

	params = append(params, fmt.Sprintf("window=%s", pvar.Window))
	params = append(params, fmt.Sprintf("accumulate=%s", pvar.Accumulate))

	return fmt.Sprintf("?%s", strings.Join(params, "&"))
}

type DiskAssetsResponse struct {
	Code int                                 `json:"code"`
	Data []map[string]DiskAssetsResponseItem `json:"data"`
}

type DiskAssetsResponseItem struct {
	AssetsResponseItem
	ByteHours      float64                          `json:"ramByteHours"`
	Bytes          float64                          `json:"bytes"`
	ByteHoursUsed  float64                          `json:"byteHoursUsed"`
	ByteUsageMax   float64                          `json:"byteUsageMax"`
	Breakdown      *DiskAssetsResponseItemBreakdown `json:"breakdown"`
	StorageClass   string                           `json:"storageClass"`
	VolumeName     string                           `json:"volumeName"`
	ClaimName      string                           `json:"claimName"`
	ClaimNamespace string                           `json:"claimNamespace"`
}

type DiskAssetsResponseItemBreakdown struct {
	Idle   float64 `json:"idle"`
	Other  float64 `json:"other"`
	System float64 `json:"system"`
	User   float64 `json:"user"`
}

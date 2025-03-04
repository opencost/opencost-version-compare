package model

import (
	"fmt"
	"strings"
)

type AllocationAutocompleteRequest struct {
	Window string
	Filter string
	Field  string
	Search string
	Limit  string
}

func (aar AllocationAutocompleteRequest) QueryString() string {
	params := []string{}

	params = append(params, fmt.Sprintf("window=%s", aar.Window))
	params = append(params, fmt.Sprintf("filter=%s", aar.Filter))
	params = append(params, fmt.Sprintf("field=%s", aar.Field))
	params = append(params, fmt.Sprintf("search=%s", aar.Search))
	params = append(params, fmt.Sprintf("limit=%s", aar.Limit))

	return fmt.Sprintf("?%s", strings.Join(params, "&"))
}

type AllocationAutocompleteResponse struct {
	Code    int                                 `json:"code"`
	Data    *AllocationAutocompleteResponseData `json:"data"`
	Message string                              `json:"message"`
}

type AllocationAutocompleteResponseData struct {
	Data []string `json:"data"`
}

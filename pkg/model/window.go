package model

import "time"

type Window struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

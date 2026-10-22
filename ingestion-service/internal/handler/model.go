package handler

import "errors"

// Reading is the wire format accepted from both the TCP line protocol and
// the HTTP ingestion endpoint: one newline-delimited JSON object per packet.
type Reading struct {
	SensorID   string  `json:"sensor_id"`
	SensorType string  `json:"sensor_type"`
	Value      float64 `json:"value"`
	Unit       string  `json:"unit"`
	Lat        float64 `json:"lat"`
	Lng        float64 `json:"lng"`
	Timestamp  int64   `json:"ts"`
}

var (
	ErrMissingSensorID = errors.New("sensor_id is required")
	ErrMissingType      = errors.New("sensor_type is required")
	ErrInvalidValue      = errors.New("value is not a finite number")
)

func (r Reading) Validate() error {
	if r.SensorID == "" {
		return ErrMissingSensorID
	}
	if r.SensorType == "" {
		return ErrMissingType
	}
	if r.Value != r.Value { // NaN check without importing math
		return ErrInvalidValue
	}
	return nil
}

package model

// Reading mirrors the wire format produced by the ingestion service.
type Reading struct {
	SensorID   string  `json:"sensor_id"`
	SensorType string  `json:"sensor_type"`
	Value      float64 `json:"value"`
	Unit       string  `json:"unit"`
	Lat        float64 `json:"lat"`
	Lng        float64 `json:"lng"`
	Timestamp  int64   `json:"ts"`
}

// validRanges encodes the physically-plausible bounds per sensor type.
// A reading outside its range is kept in cold/hot storage for audit but
// excluded from window aggregation and alerting — this is the validation
// layer the architecture calls out ("alert on negative moisture" implies
// negative moisture must first be recognized as invalid, not aggregated).
var validRanges = map[string][2]float64{
	"soil_moisture":     {0, 100},
	"soil_temperature":  {-20, 60},
	"fleet_speed":       {0, 220},
	"fleet_fuel_level":  {0, 100},
}

func (r Reading) IsValid() bool {
	bounds, ok := validRanges[r.SensorType]
	if !ok {
		return true
	}
	return r.Value >= bounds[0] && r.Value <= bounds[1]
}

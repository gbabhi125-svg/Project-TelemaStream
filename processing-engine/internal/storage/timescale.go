package storage

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/telemastream/processing-engine/internal/model"
	"github.com/telemastream/processing-engine/internal/windower"
)

type Timescale struct {
	pool *pgxpool.Pool
}

func NewTimescale(ctx context.Context, dsn string) (*Timescale, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &Timescale{pool: pool}, nil
}

func (t *Timescale) Close() {
	t.pool.Close()
}

// InsertReadingsBatch is the hot-path write: a single multi-row INSERT per
// flush rather than one round trip per reading, which is what keeps this
// path from becoming the system's new bottleneck under load.
func (t *Timescale) InsertReadingsBatch(ctx context.Context, readings []model.Reading) error {
	if len(readings) == 0 {
		return nil
	}
	batch := make([][]interface{}, 0, len(readings))
	for _, r := range readings {
		batch = append(batch, []interface{}{
			time.UnixMilli(r.Timestamp), r.SensorID, r.SensorType, r.Value, r.Unit, r.Lat, r.Lng, r.IsValid(),
		})
	}
	_, err := t.pool.CopyFrom(
		ctx,
		[]string{"sensor_readings"},
		[]string{"time", "sensor_id", "sensor_type", "value", "unit", "lat", "lng", "valid"},
		&sliceCopySource{rows: batch},
	)
	return err
}

func (t *Timescale) InsertWindows(ctx context.Context, results []windower.Result) error {
	if len(results) == 0 {
		return nil
	}
	batch := make([][]interface{}, 0, len(results))
	for _, r := range results {
		batch = append(batch, []interface{}{r.WindowEnd, r.SensorID, r.Avg, r.Min, r.Max, r.Count})
	}
	_, err := t.pool.CopyFrom(
		ctx,
		[]string{"sensor_windows"},
		[]string{"window_end", "sensor_id", "avg_value", "min_value", "max_value", "sample_count"},
		&sliceCopySource{rows: batch},
	)
	return err
}

func (t *Timescale) InsertAlert(ctx context.Context, sensorID, alertType, message, severity string) error {
	_, err := t.pool.Exec(ctx,
		`INSERT INTO sensor_alerts (time, sensor_id, alert_type, message, severity) VALUES (now(), $1, $2, $3, $4)`,
		sensorID, alertType, message, severity,
	)
	return err
}

// sliceCopySource adapts an in-memory [][]interface{} to pgx's CopyFromSource.
type sliceCopySource struct {
	rows [][]interface{}
	idx  int
}

func (s *sliceCopySource) Next() bool {
	return s.idx < len(s.rows)
}

func (s *sliceCopySource) Values() ([]interface{}, error) {
	v := s.rows[s.idx]
	s.idx++
	return v, nil
}

func (s *sliceCopySource) Err() error {
	return nil
}

// Package telemetry defines the on-the-wire contract shared by the Streamer
// (producer) and the Collector (consumer).
package telemetry

import (
	"encoding/json"
	"errors"
	"time"
)

// Record is a single GPU telemetry datapoint flowing Streamer → queue → Collector → store.
//
// Timestamp is the processing/publish time (stamped by the Streamer), not the
// original CSV timestamp. BSON tags are a leftover from an earlier MongoDB store;
// they are harmless but not meaningful in the current stack.
type Record struct {
	Timestamp  time.Time         `json:"timestamp" bson:"timestamp"`
	MetricName string            `json:"metric_name" bson:"metric_name"`
	// GPUID is the per-host ordinal index; use UUID for global GPU identity.
	GPUID      string            `json:"gpu_id" bson:"gpu_id"`
	Device     string            `json:"device" bson:"device"`
	// UUID is the globally-unique GPU identifier and the partition key on the queue.
	UUID       string            `json:"uuid" bson:"uuid"`
	ModelName  string            `json:"model_name" bson:"model_name"`
	Hostname   string            `json:"hostname" bson:"hostname"`
	Container  string            `json:"container,omitempty" bson:"container,omitempty"`
	Pod        string            `json:"pod,omitempty" bson:"pod,omitempty"`
	Namespace  string            `json:"namespace,omitempty" bson:"namespace,omitempty"`
	Value      float64           `json:"value" bson:"value"`
	Labels     map[string]string `json:"labels,omitempty" bson:"labels,omitempty"`
}

// ErrInvalidRecord is returned when a Record is missing required fields.
var ErrInvalidRecord = errors.New("invalid telemetry record")

// Validate checks that a decoded Record has the minimum fields required by the pipeline.
func (r *Record) Validate() error {
	switch {
	case r.UUID == "":
		return errors.Join(ErrInvalidRecord, errors.New("missing uuid"))
	case r.MetricName == "":
		return errors.Join(ErrInvalidRecord, errors.New("missing metric_name"))
	case r.Timestamp.IsZero():
		return errors.Join(ErrInvalidRecord, errors.New("missing timestamp"))
	}
	return nil
}

func (r *Record) Marshal() ([]byte, error) { return json.Marshal(r) }

func Unmarshal(data []byte) (Record, error) {
	var r Record
	if err := json.Unmarshal(data, &r); err != nil {
		return Record{}, err
	}
	return r, nil
}

// Package telemetry defines the on-the-wire telemetry contract shared by the
// Streamer (producer) and the Collector (consumer). Keeping the schema in one
// place means both sides serialise/deserialise identically, and the API layer
// can reuse the same type when reading back from storage.
package telemetry

import (
	"encoding/json"
	"errors"
	"time"
)

// Record is a single GPU telemetry datapoint, mirroring one row of the DCGM
// exporter CSV. It is the unit of work that flows Streamer -> queue -> Collector
// -> store.
//
// Per the project brief, the authoritative timestamp of a datapoint is the time
// at which it is *processed* (streamed), not the original CSV timestamp. The
// Streamer stamps Timestamp at publish time; the Collector persists it verbatim.
//
// The struct carries both JSON tags (the queue wire format) and BSON tags (the
// MongoDB document shape) so a Record round-trips through the whole pipeline
// without an intermediate DTO.
type Record struct {
	// Timestamp is the processing/stream time of the datapoint (RFC3339 on the
	// wire). It is the time axis the API orders and filters by.
	Timestamp time.Time `json:"timestamp" bson:"timestamp"`

	// MetricName is the DCGM field, e.g. "DCGM_FI_DEV_GPU_UTIL".
	MetricName string `json:"metric_name" bson:"metric_name"`

	// GPUID is the per-host ordinal GPU index ("0", "1", ...). It is only unique
	// within a host; use UUID to identify a GPU globally.
	GPUID string `json:"gpu_id" bson:"gpu_id"`

	// Device is the Linux device name, e.g. "nvidia0".
	Device string `json:"device" bson:"device"`

	// UUID is the globally-unique GPU identifier (GPU-xxxx...). This is the
	// partition key on the queue and the primary lookup key in the API.
	UUID string `json:"uuid" bson:"uuid"`

	// ModelName is the GPU model, e.g. "NVIDIA H100 80GB HBM3".
	ModelName string `json:"model_name" bson:"model_name"`

	// Hostname is the host that reported the metric.
	Hostname string `json:"hostname" bson:"hostname"`

	// Container/Pod/Namespace are the Kubernetes attribution fields. They are
	// frequently empty in the source data, hence omitempty.
	Container string `json:"container,omitempty" bson:"container,omitempty"`
	Pod       string `json:"pod,omitempty" bson:"pod,omitempty"`
	Namespace string `json:"namespace,omitempty" bson:"namespace,omitempty"`

	// Value is the numeric metric reading.
	Value float64 `json:"value" bson:"value"`

	// Labels holds the parsed key/value pairs from the DCGM "labels_raw" column.
	// Optional; preserved for richer querying without bloating the core schema.
	Labels map[string]string `json:"labels,omitempty" bson:"labels,omitempty"`
}

// ErrInvalidRecord is returned when a Record is missing the fields required to
// store and later query it.
var ErrInvalidRecord = errors.New("invalid telemetry record")

// Validate checks that a decoded Record carries the minimum fields the rest of
// the pipeline relies on. A datapoint with no UUID or metric name cannot be
// attributed to a GPU and is therefore unusable.
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

// Marshal encodes the Record to its queue wire format (JSON).
func (r *Record) Marshal() ([]byte, error) {
	return json.Marshal(r)
}

// Unmarshal decodes a Record from its queue wire format (JSON).
func Unmarshal(data []byte) (Record, error) {
	var r Record
	if err := json.Unmarshal(data, &r); err != nil {
		return Record{}, err
	}
	return r, nil
}

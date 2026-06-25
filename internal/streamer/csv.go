package streamer

import (
	"encoding/csv"
	"fmt"
	"io"
	"regexp"
	"strconv"

	"gpu-telemetry-pipeline/internal/telemetry"
)

// labelPair matches one key="value" entry in the DCGM "labels_raw" column.
// encoding/csv has already collapsed doubled quotes, so values are plain double-quoted.
var labelPair = regexp.MustCompile(`(\w+)="([^"]*)"`)

// parseCSV reads DCGM-exporter rows into Record templates. Timestamp is left
// zero — Run stamps it at publish time. Columns are resolved by header name so
// a reordered export still parses. Rows missing uuid/metric or with an
// unparseable value are skipped.
func parseCSV(r io.Reader) ([]telemetry.Record, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1 // tolerate ragged rows

	header, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("streamer: read csv header: %w", err)
	}
	col := make(map[string]int, len(header))
	for i, name := range header {
		col[name] = i
	}
	for _, required := range []string{"metric_name", "uuid", "value"} {
		if _, ok := col[required]; !ok {
			return nil, fmt.Errorf("streamer: csv missing required column %q", required)
		}
	}

	get := func(row []string, name string) string {
		if i, ok := col[name]; ok && i < len(row) {
			return row[i]
		}
		return ""
	}

	var records []telemetry.Record
	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("streamer: read csv row: %w", err)
		}

		uuid := get(row, "uuid")
		metric := get(row, "metric_name")
		if uuid == "" || metric == "" {
			continue
		}
		value, err := strconv.ParseFloat(get(row, "value"), 64)
		if err != nil {
			continue
		}

		records = append(records, telemetry.Record{
			MetricName: metric,
			GPUID:      get(row, "gpu_id"),
			Device:     get(row, "device"),
			UUID:       uuid,
			ModelName:  get(row, "modelName"),
			Hostname:   get(row, "Hostname"),
			Container:  get(row, "container"),
			Pod:        get(row, "pod"),
			Namespace:  get(row, "namespace"),
			Value:      value,
			Labels:     parseLabels(get(row, "labels_raw")),
		})
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("streamer: csv contained no usable telemetry rows")
	}
	return records, nil
}

// parseLabels parses the DCGM "labels_raw" column into a map; returns nil for empty input.
func parseLabels(raw string) map[string]string {
	if raw == "" {
		return nil
	}
	matches := labelPair.FindAllStringSubmatch(raw, -1)
	if len(matches) == 0 {
		return nil
	}
	labels := make(map[string]string, len(matches))
	for _, m := range matches {
		labels[m[1]] = m[2]
	}
	return labels
}

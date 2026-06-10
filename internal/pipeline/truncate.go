package pipeline

import (
	"encoding/json"
	"fmt"

	"github.com/sanjuthomas/log-forwarder/internal/transform"
)

func marshalPublishPayload(record transform.Record, maxBytes int, primaryField, suffix string) ([]byte, bool, error) {
	payload, err := json.Marshal(record)
	if err != nil {
		return nil, false, fmt.Errorf("marshal record: %w", err)
	}
	if maxBytes <= 0 || len(payload) <= maxBytes {
		return payload, false, nil
	}

	fields := []string{primaryField}
	skip := []string{primaryField}
	if _, ok := record["_raw"]; ok && primaryField != "_raw" {
		fields = append(fields, "_raw")
		skip = append(skip, "_raw")
	}
	if largest := largestStringField(record, skip...); largest != "" {
		fields = append(fields, largest)
	}

	for _, field := range fields {
		if field == "" {
			continue
		}

		payload, ok, err := truncateStringField(record, maxBytes, field, suffix)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return payload, true, nil
		}
	}

	return nil, false, fmt.Errorf("record exceeds max publish size %d bytes after truncation", maxBytes)
}

func truncateStringField(record transform.Record, maxBytes int, field, suffix string) ([]byte, bool, error) {
	val, ok := record[field]
	if !ok {
		return nil, false, nil
	}
	original, ok := val.(string)
	if !ok {
		return nil, false, nil
	}

	originalBytes := len(original)
	runes := []rune(original)

	low, high := 0, len(runes)
	var best []byte

	for low <= high {
		mid := (low + high) / 2
		trial := cloneRecord(record)
		trial[field] = string(runes[:mid]) + suffix
		setTruncationMetadata(trial, field, originalBytes)

		payload, err := json.Marshal(trial)
		if err != nil {
			return nil, false, fmt.Errorf("marshal truncated record: %w", err)
		}
		if len(payload) <= maxBytes {
			best = payload
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	if best != nil {
		return best, true, nil
	}

	trial := cloneRecord(record)
	trial[field] = suffix
	setTruncationMetadata(trial, field, originalBytes)
	payload, err := json.Marshal(trial)
	if err != nil {
		return nil, false, fmt.Errorf("marshal truncated record: %w", err)
	}
	if len(payload) <= maxBytes {
		return payload, true, nil
	}
	return nil, false, nil
}

func setTruncationMetadata(record transform.Record, field string, originalBytes int) {
	record["publish_truncated"] = true
	record[field+"_truncated"] = true
	record[field+"_original_bytes"] = originalBytes
}

func cloneRecord(record transform.Record) transform.Record {
	out := make(transform.Record, len(record)+4)
	for k, v := range record {
		out[k] = v
	}
	return out
}

func largestStringField(record transform.Record, skip ...string) string {
	skipSet := make(map[string]struct{}, len(skip))
	for _, key := range skip {
		skipSet[key] = struct{}{}
	}

	var bestKey string
	var bestLen int
	for key, val := range record {
		if _, skip := skipSet[key]; skip {
			continue
		}
		s, ok := val.(string)
		if !ok {
			continue
		}
		if n := len(s); n > bestLen {
			bestLen = n
			bestKey = key
		}
	}
	return bestKey
}


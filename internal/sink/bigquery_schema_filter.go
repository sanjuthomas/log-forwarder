// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package sink

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sync"

	"cloud.google.com/go/bigquery"
)

type bqFieldSpec struct {
	name     string
	typ      bigquery.FieldType
	required bool
	repeated bool
	schema   bigquery.Schema
}

type schemaFilter struct {
	fields map[string]bqFieldSpec
	warned sync.Map
	logger *slog.Logger
}

func newSchemaFilter(schema bigquery.Schema, logger *slog.Logger) *schemaFilter {
	if logger == nil {
		logger = slog.Default()
	}
	return &schemaFilter{
		fields: buildFieldMap(schema),
		logger: logger,
	}
}

func buildFieldMap(schema bigquery.Schema) map[string]bqFieldSpec {
	fields := make(map[string]bqFieldSpec, len(schema))
	for _, field := range schema {
		fields[field.Name] = bqFieldSpec{
			name:     field.Name,
			typ:      field.Type,
			required: field.Required,
			repeated: field.Repeated,
			schema:   field.Schema,
		}
	}
	return fields
}

func (f *schemaFilter) filterPayload(payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return payload, nil
	}

	var record map[string]any
	if err := json.Unmarshal(payload, &record); err != nil {
		return nil, fmt.Errorf("json decode: %w", err)
	}

	filtered := f.filterObject(f.fields, record, "")
	out, err := json.Marshal(filtered)
	if err != nil {
		return nil, fmt.Errorf("json encode: %w", err)
	}
	return out, nil
}

func (f *schemaFilter) filterObject(fields map[string]bqFieldSpec, obj map[string]any, pathPrefix string) map[string]any {
	out := make(map[string]any, len(obj))
	for key, value := range obj {
		fieldPath := key
		if pathPrefix != "" {
			fieldPath = pathPrefix + "." + key
		}

		spec, ok := fields[key]
		if !ok {
			f.warnOnce(fieldPath, "field not present in BigQuery table schema")
			continue
		}

		filtered, keep := f.filterValue(spec, value, fieldPath)
		if keep {
			out[key] = filtered
		}
	}
	return f.fillRequiredDefaults(fields, out, pathPrefix)
}

func (f *schemaFilter) fillRequiredDefaults(fields map[string]bqFieldSpec, obj map[string]any, pathPrefix string) map[string]any {
	if obj == nil {
		obj = make(map[string]any)
	}

	for name, spec := range fields {
		if !spec.required {
			continue
		}

		fieldPath := name
		if pathPrefix != "" {
			fieldPath = pathPrefix + "." + name
		}

		value, ok := obj[name]
		if ok && value != nil {
			if spec.typ == bigquery.RecordFieldType {
				if nested, nestedOK := value.(map[string]any); nestedOK {
					obj[name] = f.fillRequiredDefaults(buildFieldMap(spec.schema), nested, fieldPath)
				}
			}
			continue
		}

		obj[name] = f.defaultValue(spec, fieldPath)
	}
	return obj
}

func (f *schemaFilter) defaultValue(spec bqFieldSpec, fieldPath string) any {
	if spec.repeated {
		return []any{}
	}

	switch spec.typ {
	case bigquery.StringFieldType, bigquery.BytesFieldType,
		bigquery.TimestampFieldType, bigquery.DateFieldType, bigquery.TimeFieldType, bigquery.DateTimeFieldType,
		bigquery.NumericFieldType, bigquery.BigNumericFieldType, bigquery.GeographyFieldType:
		return ""
	case bigquery.IntegerFieldType:
		return float64(0)
	case bigquery.FloatFieldType:
		return float64(0)
	case bigquery.BooleanFieldType:
		return false
	case bigquery.JSONFieldType:
		return nil
	case bigquery.RecordFieldType:
		return f.fillRequiredDefaults(buildFieldMap(spec.schema), map[string]any{}, fieldPath)
	default:
		return nil
	}
}

func (f *schemaFilter) filterValue(spec bqFieldSpec, value any, fieldPath string) (any, bool) {
	if value == nil {
		return nil, true
	}

	if spec.repeated {
		items, ok := value.([]any)
		if !ok {
			f.warnOnce(fieldPath, fmt.Sprintf("type mismatch: expected array for %s field", spec.typ))
			return nil, false
		}

		elementSpec := spec
		elementSpec.repeated = false
		filteredItems := make([]any, 0, len(items))
		for i, item := range items {
			itemPath := fmt.Sprintf("%s[%d]", fieldPath, i)
			filtered, keep := f.filterValue(elementSpec, item, itemPath)
			if keep {
				filteredItems = append(filteredItems, filtered)
			}
		}
		return filteredItems, true
	}

	switch spec.typ {
	case bigquery.StringFieldType:
		s, ok := value.(string)
		if !ok {
			f.warnOnce(fieldPath, "type mismatch: expected string")
			return nil, false
		}
		return s, true
	case bigquery.IntegerFieldType:
		switch v := value.(type) {
		case float64:
			if math.Trunc(v) != v || v < math.MinInt64 || v > math.MaxInt64 {
				f.warnOnce(fieldPath, "type mismatch: expected integer")
				return nil, false
			}
			return v, true
		default:
			f.warnOnce(fieldPath, "type mismatch: expected integer")
			return nil, false
		}
	case bigquery.FloatFieldType:
		switch value.(type) {
		case float64:
			return value, true
		default:
			f.warnOnce(fieldPath, "type mismatch: expected number")
			return nil, false
		}
	case bigquery.BooleanFieldType:
		_, ok := value.(bool)
		if !ok {
			f.warnOnce(fieldPath, "type mismatch: expected boolean")
			return nil, false
		}
		return value, true
	case bigquery.TimestampFieldType, bigquery.DateFieldType, bigquery.TimeFieldType, bigquery.DateTimeFieldType:
		s, ok := value.(string)
		if !ok {
			f.warnOnce(fieldPath, fmt.Sprintf("type mismatch: expected string for %s field", spec.typ))
			return nil, false
		}
		return s, true
	case bigquery.NumericFieldType, bigquery.BigNumericFieldType, bigquery.GeographyFieldType, bigquery.BytesFieldType:
		s, ok := value.(string)
		if !ok {
			f.warnOnce(fieldPath, fmt.Sprintf("type mismatch: expected string for %s field", spec.typ))
			return nil, false
		}
		return s, true
	case bigquery.JSONFieldType:
		switch value.(type) {
		case map[string]any, []any, string, float64, bool:
			return value, true
		default:
			f.warnOnce(fieldPath, "type mismatch: expected JSON value")
			return nil, false
		}
	case bigquery.RecordFieldType:
		obj, ok := value.(map[string]any)
		if !ok {
			f.warnOnce(fieldPath, "type mismatch: expected object")
			return nil, false
		}
		nestedFields := buildFieldMap(spec.schema)
		return f.filterObject(nestedFields, obj, fieldPath), true
	default:
		f.warnOnce(fieldPath, fmt.Sprintf("unsupported BigQuery field type %q", spec.typ))
		return nil, false
	}
}

func (f *schemaFilter) warnOnce(fieldPath, reason string) {
	if _, loaded := f.warned.LoadOrStore(fieldPath, struct{}{}); loaded {
		return
	}
	f.logger.Warn("bigquery: dropping field from record",
		"field", fieldPath,
		"reason", reason,
	)
}
